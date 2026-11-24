# usgs-3dep-cache-proxy

One of four independently configurable/deployable byte-range caching proxies
for MINECRAFT-577 (Build S3 range-cache proxies for regional elevation
refiners, shared delivery Story MINECRAFT-571). This repo covers USGS 3DEP
only; Canada HRDEM, Antarctica REMA and Copernicus DEM 30m are each their
own separate repo/service, sharing only the `range_cache_proxy.py` cache
core (vendored, not a runtime dependency between them).

Pinned source: Earth Platform `combined-73` (= release 0.1.0-alpha.73,
commit `88b13c33954550c6ac660dceb743a4288a9aafac`),
`geography/src/main/java/io/earthplatform/geography/globe/UsgsElevation.java`.

## Upstream contract

- Fixed, allowlisted upstream:
  `https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/historical/`
  (the exact default from `UsgsElevation.AWS_3DEP`; override via
  `USGS_3DEP_CACHE_UPSTREAM`, default untouched when unset).
- Path: `{cell}/USGS_13_{cell}_{date}.tif`, one cloud-optimised GeoTIFF per
  1-degree cell, pinned to the exact dated release in the mod's own bundled
  `3dep-13-v1.txt` -- the `historical/` path, never the bucket's `current/`,
  which republishes and would silently change a world's terrain.
- Never a whole-file GET: the mod (`FloatCogs`) only reads a COG's header,
  block-offset directory and individual blocks via `Range`. This proxy
  caches at that same granularity -- one cache row per exact byte range
  requested, not per whole object.
- Verified live against the real bucket (anonymous, read-only,
  no-sign-request): `Accept-Ranges: bytes`, 206 with a correct
  `Content-Range`, 416 on an out-of-bounds range, 304 on a matching
  `If-None-Match`, and a plain (non-multipart) ETag.
- Attribution: public domain (`UsgsElevation.ATTRIBUTION`); no caching or
  republishing restriction found.

## Contract this proxy keeps

- Loopback-only bind (`127.0.0.1`).
- Request path validated against the exact `{cell}/USGS_13_{cell}_{date}.tif`
  shape -- no arbitrary URL forwarding.
- SQLite cache keyed by `(path, byte-range)`, restart-persistent, bounded
  with LRU eviction.
- Concurrent misses for the same exact range single-flight to one upstream
  request.
- A transient upstream failure (5xx, connection error) is retried 3x with
  backoff and never cached; a truncated transfer (declared Content-Length
  not matching the bytes received) is never cached; an upstream that
  answers 200 instead of 206 to a range request is never trusted or sliced
  as if it had honored the range.
- Genuine upstream 404/403/416 are passed through verbatim and never
  cached as success.
- Dedicated `/__health` probe: TTL 0, a fresh `Range: bytes=0-0` read of a
  real always-present object on every call, never served from the ordinary
  data cache -- so proxy-process liveness is distinguished from upstream
  reachability, and a fixture upstream outage is reflected without a direct
  bypass.
- `/__stats` reports cold/warm/upstream request and byte counters.

## Run

```
python3 -m unittest test_proxy -v   # 17 tests, local mock upstream only
python3 proxy.py                     # serves on 127.0.0.1:8101 by default
```

## Status

No live-provider traffic beyond the read-only verification probes recorded
on MINECRAFT-577; no merge/release/Zippy action. Tested against a local mock
upstream only. Mod-side wiring (an env-overridable `terrariumBase()`-style
helper for `UsgsElevation`'s base URI) is tracked on MINECRAFT-580, the
coordinated alpha.73 integration lane -- not duplicated here.
