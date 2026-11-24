"""This proxy's fixed, allowlisted upstream and native request shape.

Inventoried from Earth Platform 0.1.0-alpha.73 (tag combined-73 / commit
88b13c33954550c6ac660dceb743a4288a9aafac),
geography/src/main/java/io/earthplatform/geography/globe/UsgsElevation.java:
the mod reads one cloud-optimised GeoTIFF per 1-degree cell from the USGS
National Map's public S3 bucket, pinned to the exact dated release recorded
in its own bundled 3dep-13-v1.txt -- not the bucket's "current" path, which
republishes cells over time and would silently change a world's terrain.
Live checks against the historical/ path below confirm Accept-Ranges: bytes,
206 Partial Content with a correct Content-Range, 416 on an out-of-bounds
range, 304 on a matching If-None-Match, and a plain (non-multipart) ETag.
"""
import re

from range_cache_proxy import SourceConfig

_PATH_RE = re.compile(r"^/([ns]\d{2}[ew]\d{3})/USGS_13_\1_(\d{8})\.tif$")


def _validate_path(path: str):
    return path.lstrip("/") if _PATH_RE.match(path) else None


SOURCE = SourceConfig(
    name="usgs-3dep",
    dataset_id="3dep-13-v1",
    default_upstream="https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/historical/",
    upstream_env_var="USGS_3DEP_CACHE_UPSTREAM",
    validate_path=_validate_path,
    probe_path="/n06e162/USGS_13_n06e162_20130911.tif",
)
