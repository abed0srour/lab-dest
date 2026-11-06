"""Fill the cluster with sample data for the web UI (uploads go through the gateway API).

  docs     (replication)      2,000 small files from datasets/small, tagged type=<folder>,project=lab
  archive  (erasure coding)   disk-00.img and disk-05.img (a new version of it), tagged type=disk-image

Enough data that a node failure keeps the repair busy for a while, so it can be watched in the UI.

Usage: python scripts/seed_ui_data.py [--small 2000]
"""

import argparse
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
TYPES = {"logs": "log", "notes": "note", "users": "user", "orders": "order", "configs": "config"}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--gateway", default="http://127.0.0.1:8080")
    p.add_argument("--api-key", default="bench-key-2026")
    p.add_argument("--small", type=int, default=2000, help="small files to upload (spread over the 5 folders)")
    a = p.parse_args()
    local = threading.local()

    def client() -> httpx.Client:
        if not hasattr(local, "c"):
            local.c = httpx.Client(base_url=a.gateway, headers={"X-API-Key": a.api_key}, timeout=600)
        return local.c

    def put(bucket: str, key: str, path: Path, tags: str, content_type: str) -> None:
        with open(path, "rb") as f:
            client().put(f"/buckets/{bucket}/objects/{key}", content=f,
                         headers={"X-Tags": tags, "Content-Type": content_type}).raise_for_status()

    client().put("/buckets/docs").raise_for_status()
    client().put("/buckets/archive", params={"redundancy": "ec"}).raise_for_status()

    small = ROOT / "datasets" / "small"
    per_folder = max(1, a.small // len(TYPES))
    jobs = []
    for folder, kind in TYPES.items():
        for f in sorted((small / folder).iterdir())[:per_folder]:
            ctype = "application/json" if f.suffix == ".json" else "text/plain"
            jobs.append(("docs", f"{folder}/{f.name}", f, f"type={kind},project=lab", ctype))
    large = ROOT / "datasets" / "large"
    for name in ("disk-00.img", "disk-05.img"):
        jobs.append(("archive", f"images/{name}", large / name, "type=disk-image,project=lab",
                     "application/octet-stream"))

    print(f"uploading {len(jobs)} files through the gateway ...", flush=True)
    with ThreadPoolExecutor(8) as pool:
        list(pool.map(lambda j: put(*j), jobs))
    print("done: bucket docs (replication) and bucket archive (erasure coding) are ready")


if __name__ == "__main__":
    sys.exit(main())
