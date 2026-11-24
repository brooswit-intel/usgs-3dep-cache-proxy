"""Integration proof for the MINECRAFT-577 shared range-cache core, run
against a local mock upstream only (never a live AWS bucket): per the ticket,
no live-provider traffic is authorized before terms verification, and Jira
holds no-merge/no-release/no-live-use until repo access lands. The mock
serves the exact USGS 3DEP byte-range/ETag/conditional contract inventoried
live against prd-tnm.s3.amazonaws.com, so the mechanics under test are the
real ones each of the four proxies will use against their own live default.
"""
import os
import tempfile
import threading
import unittest
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer

from range_cache_proxy import RangeCacheProxy
from source import SOURCE

OBJECT = b"COG-FULL-OBJECT-BYTES-" + b"X" * 2000  # pretend 3DEP tile
ETAG = '"deadbeef1234"'
PATH = "/n06e162/USGS_13_n06e162_20130911.tif"


class MockUpstream(BaseHTTPRequestHandler):
    hit_count = {"n": 0}
    fail_next = {"n": 0}
    serve_whole_on_range = {"on": False}

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        MockUpstream.hit_count["n"] += 1
        if MockUpstream.fail_next["n"] > 0:
            MockUpstream.fail_next["n"] -= 1
            self.send_response(503)
            self.end_headers()
            return
        if self.path != PATH:
            self.send_response(404)
            self.end_headers()
            return
        rng = self.headers.get("Range")
        if rng is None:
            self.send_response(200)
            self.send_header("ETag", ETAG)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(len(OBJECT)))
            self.end_headers()
            self.wfile.write(OBJECT)
            return
        start_s, _, end_s = rng[len("bytes="):].partition("-")
        start = int(start_s)
        end = int(end_s) if end_s else len(OBJECT) - 1
        if start >= len(OBJECT):
            self.send_response(416)
            self.send_header("Content-Range", f"bytes */{len(OBJECT)}")
            self.end_headers()
            return
        end = min(end, len(OBJECT) - 1)
        if MockUpstream.serve_whole_on_range["on"]:
            # Misbehaving upstream: asked for a range, answers 200 with the
            # whole object. The proxy must never trust/slice this as the
            # requested range.
            self.send_response(200)
            self.send_header("ETag", ETAG)
            self.send_header("Content-Length", str(len(OBJECT)))
            self.end_headers()
            self.wfile.write(OBJECT)
            return
        chunk = OBJECT[start:end + 1]
        self.send_response(206)
        self.send_header("ETag", ETAG)
        self.send_header("Content-Range", f"bytes {start}-{end}/{len(OBJECT)}")
        self.send_header("Content-Length", str(len(chunk)))
        self.end_headers()
        self.wfile.write(chunk)


def _fetch(port, path, range_header=None, if_none_match=None):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}")
    if range_header:
        req.add_header("Range", range_header)
    if if_none_match:
        req.add_header("If-None-Match", if_none_match)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


class RangeCacheProxyTest(unittest.TestCase):
    def setUp(self):
        MockUpstream.hit_count["n"] = 0
        MockUpstream.fail_next["n"] = 0
        MockUpstream.serve_whole_on_range["on"] = False
        self.mock = HTTPServer(("127.0.0.1", 0), MockUpstream)
        self.mock_port = self.mock.server_address[1]
        threading.Thread(target=self.mock.serve_forever, daemon=True).start()

        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(self.db_fd)
        self._start_proxy()

    def _start_proxy(self, max_bytes=512 * 1024 * 1024):
        from range_cache_proxy import run_server
        upstream = f"http://127.0.0.1:{self.mock_port}/"
        self.server, self.proxy = run_server(SOURCE, self.db_path, 0, upstream, max_bytes)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.server.shutdown()
        self.proxy.close()
        self.mock.shutdown()
        os.unlink(self.db_path)

    def test_range_passthrough_and_cold_miss(self):
        status, headers, body = _fetch(self.port, PATH, range_header="bytes=0-9")
        self.assertEqual(206, status)
        self.assertEqual(OBJECT[0:10], body)
        self.assertEqual(f"bytes 0-9/*", headers["Content-Range"])
        self.assertEqual(1, MockUpstream.hit_count["n"])
        self.assertEqual(1, self.proxy.stats.cold_misses)
        self.assertEqual(0, self.proxy.stats.warm_hits)

    def test_warm_hit_does_not_touch_upstream(self):
        _fetch(self.port, PATH, range_header="bytes=0-9")
        self.assertEqual(1, MockUpstream.hit_count["n"])
        status, _headers, body = _fetch(self.port, PATH, range_header="bytes=0-9")
        self.assertEqual(206, status)
        self.assertEqual(OBJECT[0:10], body)
        self.assertEqual(1, MockUpstream.hit_count["n"], "warm hit must not re-fetch upstream")
        self.assertEqual(1, self.proxy.stats.warm_hits)

    def test_distinct_ranges_are_cached_separately(self):
        _fetch(self.port, PATH, range_header="bytes=0-9")
        _fetch(self.port, PATH, range_header="bytes=10-19")
        self.assertEqual(2, MockUpstream.hit_count["n"])
        status, _h, body = _fetch(self.port, PATH, range_header="bytes=10-19")
        self.assertEqual(OBJECT[10:20], body)
        self.assertEqual(2, MockUpstream.hit_count["n"], "second range's warm hit must not re-fetch")

    def test_conditional_304_on_matching_etag(self):
        _fetch(self.port, PATH, range_header="bytes=0-9")
        status, headers, body = _fetch(self.port, PATH, range_header="bytes=0-9", if_none_match=ETAG)
        self.assertEqual(304, status)
        self.assertEqual(b"", body)
        self.assertEqual(1, self.proxy.stats.not_modified)

    def test_out_of_bounds_range_is_416_and_not_cached(self):
        status, _headers, _body = _fetch(self.port, PATH, range_header=f"bytes={len(OBJECT) + 100}-")
        self.assertEqual(416, status)
        self.assertEqual(0, self.proxy.cache.total_bytes())

    def test_404_passthrough_never_cached(self):
        status, _headers, _body = _fetch(self.port, "/not/a/real/cell.tif", range_header="bytes=0-9")
        self.assertEqual(404, status)
        self.assertEqual(0, self.proxy.cache.total_bytes())

    def test_unvalidated_path_refused_before_touching_upstream(self):
        status, _headers, _body = _fetch(self.port, "/../../etc/passwd", range_header="bytes=0-9")
        self.assertEqual(404, status)
        self.assertEqual(0, MockUpstream.hit_count["n"], "no arbitrary path forwarding to upstream")

    def test_transient_upstream_failure_is_retried_not_cached_as_success(self):
        MockUpstream.fail_next["n"] = 2
        status, _headers, body = _fetch(self.port, PATH, range_header="bytes=0-9")
        self.assertEqual(206, status)
        self.assertEqual(OBJECT[0:10], body)
        self.assertEqual(3, MockUpstream.hit_count["n"], "2 failed attempts + 1 success")

    def test_persistent_upstream_failure_returns_502_and_caches_nothing(self):
        MockUpstream.fail_next["n"] = 999
        status, _headers, _body = _fetch(self.port, PATH, range_header="bytes=0-9")
        self.assertEqual(502, status)
        self.assertEqual(0, self.proxy.cache.total_bytes())

    def test_misbehaving_upstream_200_on_range_request_is_never_trusted(self):
        MockUpstream.serve_whole_on_range["on"] = True
        status, _headers, _body = _fetch(self.port, PATH, range_header="bytes=0-9")
        self.assertEqual(502, status)
        self.assertEqual(0, self.proxy.cache.total_bytes(), "never cache a range reply upstream didn't actually honor")

    def test_concurrent_miss_deduplication_single_flight(self):
        barrier_hits_before = MockUpstream.hit_count["n"]
        results = []
        threads = [
            threading.Thread(target=lambda: results.append(_fetch(self.port, PATH, range_header="bytes=0-9")))
            for _ in range(8)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)
        self.assertEqual(8, len(results))
        for status, _h, body in results:
            self.assertEqual(206, status)
            self.assertEqual(OBJECT[0:10], body)
        self.assertEqual(
            barrier_hits_before + 1, MockUpstream.hit_count["n"],
            "8 concurrent misses for the same exact range must dedupe to exactly 1 upstream request",
        )

    def test_restart_persistence(self):
        _fetch(self.port, PATH, range_header="bytes=0-9")
        self.assertEqual(1, MockUpstream.hit_count["n"])
        self.server.shutdown()
        self.proxy.close()
        self._start_proxy()
        status, _headers, body = _fetch(self.port, PATH, range_header="bytes=0-9")
        self.assertEqual(206, status)
        self.assertEqual(OBJECT[0:10], body)
        self.assertEqual(1, MockUpstream.hit_count["n"], "restarted proxy must serve the prior range from disk, not re-fetch")

    def test_bounded_cache_evicts_lru(self):
        self.server.shutdown()
        self.proxy.close()
        self._start_proxy(max_bytes=20)  # tiny budget forces eviction
        _fetch(self.port, PATH, range_header="bytes=0-9")   # 10 bytes
        _fetch(self.port, PATH, range_header="bytes=20-29")  # 10 bytes, total 20
        _fetch(self.port, PATH, range_header="bytes=40-49")  # 10 bytes, forces eviction
        self.assertGreaterEqual(self.proxy.stats.evictions, 1)
        # oldest (0-9) range should have been evicted; re-fetching it hits upstream again.
        hits_before = MockUpstream.hit_count["n"]
        _fetch(self.port, PATH, range_header="bytes=0-9")
        self.assertEqual(hits_before + 1, MockUpstream.hit_count["n"])

    def test_health_probe_reports_upstream_ok(self):
        status, _headers, body = _fetch(self.port, "/__health")
        self.assertEqual(200, status)
        self.assertIn(b'"upstream_reachable": true', body)

    def test_health_probe_reflects_upstream_outage_without_direct_bypass(self):
        MockUpstream.fail_next["n"] = 999
        status, _headers, body = _fetch(self.port, "/__health")
        self.assertEqual(503, status)
        self.assertIn(b'"upstream_reachable": false', body)
        MockUpstream.fail_next["n"] = 0
        status, _headers, body = _fetch(self.port, "/__health")
        self.assertEqual(200, status)
        self.assertIn(b'"upstream_reachable": true', body)

    def test_health_probe_never_cached_ttl_zero(self):
        _fetch(self.port, "/__health")
        probes_before = MockUpstream.hit_count["n"]
        _fetch(self.port, "/__health")
        self.assertEqual(probes_before + 1, MockUpstream.hit_count["n"], "every probe call must hit upstream fresh, TTL 0")

    def test_ordinary_data_path_unaffected_by_probe_policy(self):
        _fetch(self.port, "/__health")
        _fetch(self.port, PATH, range_header="bytes=0-9")
        hits_before = MockUpstream.hit_count["n"]
        status, _headers, body = _fetch(self.port, PATH, range_header="bytes=0-9")
        self.assertEqual(206, status)
        self.assertEqual(hits_before, MockUpstream.hit_count["n"], "ordinary data GETs must still cache even though the probe path never does")


if __name__ == "__main__":
    unittest.main()
