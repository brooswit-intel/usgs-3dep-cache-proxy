"""Entrypoint: loopback-only byte-range cache proxy for USGS 3DEP.

Run: python3 proxy.py
Env overrides: USGS_3DEP_CACHE_UPSTREAM, USGS_3DEP_CACHE_PORT, USGS_3DEP_CACHE_DB
"""
import os

from range_cache_proxy import run_server
from source import SOURCE

if __name__ == "__main__":
    port = int(os.environ.get("USGS_3DEP_CACHE_PORT", "8101"))
    db = os.environ.get("USGS_3DEP_CACHE_DB", "usgs-3dep-cache.sqlite3")
    server, proxy = run_server(SOURCE, db, port)
    print(f"usgs-3dep cache proxy on http://127.0.0.1:{port} -> upstream {proxy.upstream_base}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        proxy.close()
