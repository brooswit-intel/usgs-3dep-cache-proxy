"""Shared loopback-only byte-range caching proxy core for MINECRAFT-577's four
regional elevation refiners (USGS 3DEP, Canada HRDEM, Antarctica REMA,
Copernicus DEM 30m).

Upstream contract, inventoried at Earth Platform 0.1.0-alpha.73 (tag
combined-73 / commit 88b13c33954550c6ac660dceb743a4288a9aafac):
  - geography/.../UsgsElevation.java  -> https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/historical/{cell}/USGS_13_{cell}_{date}.tif
  - geography/.../HrdemElevation.java -> https://canelevation-dem.s3.ca-central-1.amazonaws.com/hrdem-mosaic-2m/{square}-mosaic-2m-dtm.tif
  - geography/.../RemaSource.java     -> https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/rema/mosaics/v2.0/{tile}_dem.tif
  - geography/.../CopernicusSource.java -> https://copernicus-dem-30m.s3.amazonaws.com/{cell}/{cell}.tif

All four are FloatCogs.java reads: plain HTTPS GET of a fixed, publicly
readable S3 object, *never* a whole-file transfer -- only byte-range reads of
a COG's header, IFD/block-offset directory, and individual tile blocks. Live
checks against each bucket (read-only, no-sign-request, anonymous GET)
confirm Accept-Ranges: bytes, 206 Partial Content with a correct
Content-Range on a byte range, 416 on an out-of-bounds range, and either a
plain (non-multipart) ETag that participates correctly in If-None-Match / 304
(USGS 3DEP, Copernicus) or an opaque multipart-upload ETag that is still a
valid, stable revision token for cache invalidation even though it is not a
content hash (HRDEM, REMA -- both multi-GB mosaics uploaded in parts).

This module is the cache core shared across the four otherwise-independent,
independently-configurable/deployable proxies the ticket calls for: a loopback
HTTP server in front of one fixed, HTTPS, allowlisted upstream origin, with a
persistent bounded SQLite range cache, concurrent-miss deduplication, and
restart persistence. Each source gets its own process/port/db/upstream -- this
module never combines them into one runtime service.
"""
from __future__ import annotations

import http.client
import os
import re
import sqlite3
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Optional


@dataclass(frozen=True)
class SourceConfig:
    """One source's fixed, allowlisted upstream and its native request shape."""
    name: str
    dataset_id: str
    default_upstream: str
    upstream_env_var: str
    # Validates + normalizes a client request path into the exact upstream
    # object path, or returns None to refuse (no arbitrary URL forwarding).
    validate_path: Callable[[str], Optional[str]]
    # A real, always-present object path used only by the dedicated TTL-0
    # health probe (never by ordinary data requests).
    probe_path: str


class UpstreamError(Exception):
    pass


@dataclass
class Stats:
    cold_misses: int = 0
    warm_hits: int = 0
    upstream_requests: int = 0
    upstream_bytes: int = 0
    not_modified: int = 0
    evictions: int = 0
    probe_upstream_ok: int = 0
    probe_upstream_failed: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def as_dict(self) -> dict:
        with self.lock:
            return {
                "cold_misses": self.cold_misses,
                "warm_hits": self.warm_hits,
                "upstream_requests": self.upstream_requests,
                "upstream_bytes": self.upstream_bytes,
                "not_modified": self.not_modified,
                "evictions": self.evictions,
                "probe_upstream_ok": self.probe_upstream_ok,
                "probe_upstream_failed": self.probe_upstream_failed,
            }


class RangeCache:
    """SQLite-backed, restart-persistent, bounded cache of exact byte ranges.

    A row is keyed by (path, start, end) -- the exact slice a COG reader asked
    for (header probe, block-offset directory, one data block, ...). Nothing
    is written unless the upstream fetch for that exact range completed with
    a 200/206 whose body length matches what was declared/implied, so a
    timeout, dropped connection, non-2xx status, or truncated transfer is
    never stored and can never be served back as cached data.
    """

    def __init__(self, db_path: str, max_bytes: int):
        self.max_bytes = max_bytes
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS ranges (
                 path TEXT NOT NULL,
                 start INTEGER NOT NULL,
                 end INTEGER NOT NULL,
                 etag TEXT NOT NULL,
                 body BLOB NOT NULL,
                 size INTEGER NOT NULL,
                 created_at REAL NOT NULL,
                 last_used_at REAL NOT NULL,
                 PRIMARY KEY (path, start, end)
               )"""
        )
        self._conn.commit()
        self._lock = threading.Lock()

    def get(self, path: str, start: int, end: int):
        with self._lock:
            row = self._conn.execute(
                "SELECT etag, body FROM ranges WHERE path = ? AND start = ? AND end = ?",
                (path, start, end),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE ranges SET last_used_at = ? WHERE path = ? AND start = ? AND end = ?",
                (time.time(), path, start, end),
            )
            self._conn.commit()
        return row[0], row[1]

    def put(self, path: str, start: int, end: int, etag: str, body: bytes):
        now = time.time()
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO ranges (path, start, end, etag, body, size, created_at, last_used_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (path, start, end, etag, body, len(body), now, now),
            )
            self._conn.commit()

    def total_bytes(self) -> int:
        with self._lock:
            return self._conn.execute("SELECT COALESCE(SUM(size), 0) FROM ranges").fetchone()[0]

    def evict_to_fit(self, stats: Stats) -> None:
        if self.max_bytes <= 0:
            return
        target = self.max_bytes * 9 // 10
        with self._lock:
            total = self._conn.execute("SELECT COALESCE(SUM(size), 0) FROM ranges").fetchone()[0]
            if total <= self.max_bytes:
                return
            rows = self._conn.execute(
                "SELECT path, start, end, size FROM ranges ORDER BY last_used_at ASC"
            ).fetchall()
            removed = 0
            for path, start, end, size in rows:
                if total - removed <= target:
                    break
                self._conn.execute(
                    "DELETE FROM ranges WHERE path = ? AND start = ? AND end = ?", (path, start, end)
                )
                removed += size
                with stats.lock:
                    stats.evictions += 1
            self._conn.commit()

    def close(self):
        self._conn.close()


class _Flight(threading.Event):
    result = None
    error: Exception | None = None


class _SingleFlight:
    """Deduplicates concurrent misses for the same (path, range): the first
    caller fetches upstream, every other concurrent caller for that exact key
    waits on the same result instead of issuing a second upstream request."""

    def __init__(self):
        self._lock = threading.Lock()
        self._inflight: dict[tuple, _Flight] = {}

    def run(self, key: tuple, loader):
        with self._lock:
            flight = self._inflight.get(key)
            is_leader = flight is None
            if is_leader:
                flight = _Flight()
                self._inflight[key] = flight
        if is_leader:
            try:
                flight.result = loader()
            except Exception as exc:  # noqa: BLE001 - re-raised to every waiter
                flight.error = exc
            finally:
                with self._lock:
                    self._inflight.pop(key, None)
                flight.set()
        else:
            flight.wait()
        if flight.error is not None:
            raise flight.error
        return flight.result


#: Defensive ceiling on a single read when the expected size isn't known
#: up front (an open-ended range, or a 200 answering a range request that
#: should never be trusted/read in full anyway). Matches the order of
#: magnitude of a single COG block (CopernicusSource.MAX_BLOCK_BYTES is
#: 8 MiB) with headroom, not the size of a whole mosaic file.
_UNBOUNDED_READ_SAFETY_CAP = 64 * 1024 * 1024

_CONTENT_RANGE_RE = re.compile(r"^bytes (\d+)-(\d+)/(\d+|\*)$")


def _parse_content_range(content_range: Optional[str]):
    """Returns (start, end, total) from a Content-Range header -- start/end
    inclusive, total an int or None when the upstream sent '*' (unknown
    total) -- or None if the header is absent/unparseable."""
    if content_range is None:
        return None
    m = _CONTENT_RANGE_RE.match(content_range)
    if not m:
        return None
    total = None if m.group(3) == "*" else int(m.group(3))
    return int(m.group(1)), int(m.group(2)), total


def _fetch_range(upstream_base: str, upstream_path: str, start: Optional[int], end: Optional[int],
                  timeout: float, retries: int):
    """GETs exactly one byte range (or the whole object if start is None) from
    the fixed upstream. Returns (status, etag, content_range, body). Raises
    UpstreamError on network failure, a non-2xx status, or a truncated body
    (declared Content-Length not matching the bytes actually received) --
    none of those are ever treated as cacheable success by the caller.

    Status is always checked BEFORE any body is read: a HEAD-like decision
    (404/403/416, or a 200 answering what should have been a 206) happens
    off headers alone, with no read at all. For a genuine 206, the read is
    capped to the exact declared size (defensively ceilinged) rather than
    an unbounded `.read()` -- a misbehaving upstream that ignores Range on
    a multi-GB object (HRDEM mosaics are 1-10 GB) must never be read into
    memory in full just to discover it isn't a 206."""
    parsed = urllib.parse.urlsplit(upstream_base)
    full_path = parsed.path.rstrip("/") + "/" + upstream_path.lstrip("/")
    headers = {"User-Agent": "minecraft-577-regional-elevation-cache/1"}
    if start is not None:
        headers["Range"] = f"bytes={start}-{end}" if end is not None else f"bytes={start}-"
    last_exc: Exception | None = None
    for attempt in range(retries):
        if attempt:
            time.sleep(0.05 * (2 ** attempt))
        conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        conn = conn_cls(parsed.netloc, timeout=timeout)
        try:
            conn.request("GET", full_path, headers=headers)
            resp = conn.getresponse()
            status = resp.status
            if status in (404, 403, 416):
                # Genuine upstream "not found" / "forbidden" / out-of-bounds
                # range -- decided off headers alone, no body read at all;
                # passed through verbatim, never cached, never retried as
                # if it were a transient failure.
                content_range = resp.getheader("Content-Range")
                resp.read()  # drain on this connection before it's closed
                return status, None, content_range, b""
            if status not in (200, 206):
                resp.read()
                last_exc = UpstreamError(f"HTTP {status} for {full_path}")
                continue
            if start is not None and status != 206:
                # Asked for a range but upstream answered 200: never trust or
                # read this as the requested range. Read only a small capped
                # prefix (never the full, possibly huge, object) purely to
                # drain the connection before closing it, then fail the
                # attempt exactly as before.
                resp.read(_UNBOUNDED_READ_SAFETY_CAP)
                last_exc = UpstreamError(f"requested range but upstream returned {status} for {full_path}")
                continue
            declared = resp.getheader("Content-Length")
            # Bound the read to what was declared (a legitimate 206/200 for
            # the exact request made), with a defensive ceiling regardless --
            # never an unbounded .read() driven solely by a remote-controlled
            # Content-Length.
            cap = min(int(declared), _UNBOUNDED_READ_SAFETY_CAP) if declared is not None else _UNBOUNDED_READ_SAFETY_CAP
            body = resp.read(cap)
            if declared is not None and int(declared) != len(body):
                last_exc = UpstreamError(f"truncated transfer for {full_path}: declared {declared}, got {len(body)}")
                continue
            content_range = resp.getheader("Content-Range")
            if status == 206 and start is not None:
                parsed_range = _parse_content_range(content_range)
                if parsed_range is None:
                    last_exc = UpstreamError(f"missing/unparseable Content-Range {content_range!r} for {full_path}")
                    continue
                got_start, got_end, got_total = parsed_range
                # A shorter-than-requested end is only legitimate when a
                # numeric total proves it's the real end of the object (the
                # object is shorter than the requested end) -- never on an
                # unknown total ('*'), which proves nothing. Anything else
                # (a different start entirely, or a short end with no
                # proof) is a 206 that must not be cached/served under the
                # requested key: it is otherwise indistinguishable from a
                # correct reply by length alone.
                end_ok = (
                    end is None
                    or got_end == end
                    or (got_end < end and got_total is not None and got_total == got_end + 1)
                )
                if got_start != start or not end_ok:
                    last_exc = UpstreamError(
                        f"Content-Range {content_range!r} does not match requested bytes {start}-{end} for {full_path}"
                    )
                    continue
                expected_len = got_end - got_start + 1
                if len(body) != expected_len:
                    # Correct offsets, wrong actual byte count -- e.g. a
                    # connection cut short with no (or a matching-but-wrong)
                    # Content-Length header, which the earlier declared-
                    # length check alone would miss whenever Content-Length
                    # is absent. The range interval itself is the ground
                    # truth for how many bytes this reply must contain.
                    last_exc = UpstreamError(
                        f"body length {len(body)} does not match range interval {expected_len} "
                        f"({content_range!r}) for {full_path}"
                    )
                    continue
            etag = resp.getheader("ETag")
            return status, etag, content_range, body
        except OSError as net_err:
            last_exc = net_err
            continue
        finally:
            conn.close()
    raise UpstreamError(f"upstream fetch failed after {retries} attempts: {last_exc}")


class RangeCacheProxy:
    def __init__(self, source: SourceConfig, db_path: str, upstream_base: Optional[str] = None,
                 max_bytes: int = 512 * 1024 * 1024, timeout: float = 10.0, retries: int = 3):
        self.source = source
        self.upstream_base = upstream_base or os.environ.get(source.upstream_env_var, source.default_upstream)
        self.cache = RangeCache(db_path, max_bytes)
        self.stats = Stats()
        self._single_flight = _SingleFlight()
        self._timeout = timeout
        self._retries = retries

    def serve(self, request_path: str, if_none_match: Optional[str], range_header: Optional[str]):
        """Returns (status, headers, body) for one client request."""
        upstream_path = self.source.validate_path(request_path)
        if upstream_path is None:
            return 404, {}, b""

        start = end = None
        if range_header and range_header.startswith("bytes="):
            start_s, _, end_s = range_header[len("bytes="):].partition("-")
            start = int(start_s) if start_s else 0
            end = int(end_s) if end_s else None

        cache_key_end = end if end is not None else -1  # -1 == "to end of object", resolved on miss
        cached = self.cache.get(upstream_path, start if start is not None else 0, cache_key_end) \
            if start is not None else None

        if cached is not None:
            with self.stats.lock:
                self.stats.warm_hits += 1
            etag, body = cached
        else:
            def load():
                status, etag, content_range, body = _fetch_range(
                    self.upstream_base, upstream_path, start, end, self._timeout, self._retries
                )
                if status in (404, 403, 416):
                    return status, None, content_range, None
                with self.stats.lock:
                    self.stats.upstream_requests += 1
                    self.stats.upstream_bytes += len(body)
                if start is not None:
                    resolved_end = cache_key_end if end is not None else len(body) - 1 + start
                    self.cache.put(upstream_path, start, resolved_end, etag or "", body)
                    self.cache.evict_to_fit(self.stats)
                return status, etag, None, body

            with self.stats.lock:
                self.stats.cold_misses += 1
            try:
                flight_key = (upstream_path, start if start is not None else 0, cache_key_end)
                status, etag, content_range, body = self._single_flight.run(flight_key, load)
            except UpstreamError:
                return 502, {}, b""
            if status in (404, 403):
                return status, {}, b""
            if status == 416:
                headers = {"Content-Range": content_range} if content_range else {}
                return 416, headers, b""

        if if_none_match is not None and etag and if_none_match == etag:
            with self.stats.lock:
                self.stats.not_modified += 1
            return 304, {"ETag": etag}, b""

        headers = {"ETag": etag or "", "Accept-Ranges": "bytes"}
        if start is not None:
            # Always the TRUE end of what's actually being returned, never
            # the client's originally requested end: a request for bytes
            # past the object's real EOF is answered with fewer bytes than
            # asked (proven by upstream's own Content-Range total), and the
            # client-facing header must describe what's actually in body,
            # not repeat back a request that was never fully satisfied.
            resolved_end = start + len(body) - 1
            headers["Content-Range"] = f"bytes {start}-{resolved_end}/*"
            return 206, headers, body
        return 200, headers, body

    def probe_upstream(self) -> bool:
        """Dedicated health-check path: TTL 0, never cached, never served
        stale. Issues a fresh Range 0-0 read of the source's canonical probe
        object on every call so proxy-process liveness (this call returning
        at all) is distinguished from upstream reachability (its result)."""
        try:
            status, _etag, _cr, _body = _fetch_range(
                self.upstream_base, self.source.probe_path, 0, 0, self._timeout, self._retries
            )
            ok = status in (200, 206)
        except UpstreamError:
            ok = False
        with self.stats.lock:
            if ok:
                self.stats.probe_upstream_ok += 1
            else:
                self.stats.probe_upstream_failed += 1
        return ok

    def close(self):
        self.cache.close()


def make_handler(proxy: RangeCacheProxy):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass

        def do_HEAD(self):
            # EarthStatus.reach()-style callers send a plain HEAD with no
            # body expected; /__health must answer that the same way GET
            # does (status only, TTL 0, never the ordinary data cache) or a
            # correctly-behaving proxy would fail a HEAD-based health probe
            # with a 501 from BaseHTTPRequestHandler's default dispatch.
            if self.path == "/__health":
                ok = proxy.probe_upstream()
                self.send_response(200 if ok else 503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            import json
            if self.path == "/__stats":
                payload = json.dumps(proxy.stats.as_dict()).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            if self.path == "/__health":
                # Dedicated TTL-0 probe resource: always hits upstream fresh;
                # never answers from the ordinary data cache.
                ok = proxy.probe_upstream()
                self.send_response(200 if ok else 503)
                payload = json.dumps({"upstream_reachable": ok}).encode()
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            status, headers, body = proxy.serve(
                self.path, self.headers.get("If-None-Match"), self.headers.get("Range")
            )
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

    return Handler


def run_server(source: SourceConfig, db_path: str, port: int, upstream_base: Optional[str] = None,
               max_bytes: int = 512 * 1024 * 1024):
    proxy = RangeCacheProxy(source, db_path, upstream_base, max_bytes)
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(proxy))
    return server, proxy
