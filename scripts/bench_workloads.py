"""Throughput and latency of the two workloads through the gateway API (HTTP + API key).

For each workload (5,000 small files / the first --large-files disk images) and each
redundancy scheme (replication / erasure coding), with --workers parallel clients:
  upload    every file with PUT, streamed from disk
  download  every file with GET, checking its SHA-256 against the ETag
Reports files/s, MB/s and request latency (median and 95th percentile), then deletes
what it uploaded (--keep leaves it).

Run it on an empty cluster (docker compose down -v ; docker compose up -d): chunks
already stored by earlier tests would be deduplicated and make uploads look faster.

Usage: python scripts/bench_workloads.py [--large-files 5] [--workers 8] [--keep]
"""

import argparse
import hashlib
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bench_chunking import print_table, write_csv  # noqa: E402

SCHEMES = {"rep": "replication", "ec": "ec"}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--gateway", default="http://127.0.0.1:8080")
    p.add_argument("--api-key", default="bench-key-2026", help="a key with a high rate limit")
    p.add_argument("--datasets", default=str(ROOT / "datasets"))
    p.add_argument("--large-files", type=int, default=5)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--keep", action="store_true", help="do not delete the uploaded objects")
    p.add_argument("--out", default=str(ROOT / "benchmarks" / "results"))
    a = p.parse_args()

    local = threading.local()

    def client() -> httpx.Client:
        if not hasattr(local, "c"):
            local.c = httpx.Client(base_url=a.gateway, headers={"X-API-Key": a.api_key}, timeout=600)
        return local.c

    def put(job):
        bucket, key, path = job
        with open(path, "rb") as f:
            t = time.perf_counter()
            r = client().put(f"/buckets/{bucket}/objects/{key}", content=f)
            r.raise_for_status()
        return time.perf_counter() - t, r.json()["size"], r.json()["deduplicated_chunks"]

    def get(job):
        bucket, key, _ = job
        digest, size = hashlib.sha256(), 0
        t = time.perf_counter()
        with client().stream("GET", f"/buckets/{bucket}/objects/{key}") as r:
            r.raise_for_status()
            etag = r.headers["etag"].strip('"')
            for block in r.iter_bytes(1 << 20):
                digest.update(block)
                size += len(block)
        return time.perf_counter() - t, size, digest.hexdigest() == etag

    def measure(fn, jobs) -> tuple[dict, list]:
        t = time.perf_counter()
        with ThreadPoolExecutor(a.workers) as pool:
            results = list(pool.map(fn, jobs))
        wall = time.perf_counter() - t
        latencies = sorted(r[0] for r in results)
        total = sum(r[1] for r in results)
        return {"files": len(jobs), "mb": round(total / 2**20, 1), "seconds": round(wall, 1),
                "files_per_s": round(len(jobs) / wall, 1), "mb_per_s": round(total / wall / 1e6, 1),
                "p50_ms": round(statistics.median(latencies) * 1000, 1),
                "p95_ms": round(latencies[int(0.95 * (len(latencies) - 1))] * 1000, 1)}, results

    datasets = Path(a.datasets)
    workloads = {
        "small": (datasets / "small", sorted(f for f in (datasets / "small").rglob("*") if f.is_file())),
        "large": (datasets / "large", sorted((datasets / "large").glob("*.img"))[:a.large_files]),
    }
    rows, uploaded = [], []
    admin = client()
    for workload, (root, files) in workloads.items():
        for tag, redundancy in SCHEMES.items():
            bucket = f"wl-{workload}-{tag}"
            admin.put(f"/buckets/{bucket}", params={"redundancy": redundancy}).raise_for_status()
            jobs = [(bucket, f.relative_to(root).as_posix(), f) for f in files]
            print(f"{workload:5} {redundancy:11} upload   {len(jobs)} files ...", flush=True)
            up, results = measure(put, jobs)
            up["dedup_chunks"] = sum(r[2] for r in results)
            print(f"{workload:5} {redundancy:11} download {len(jobs)} files ...", flush=True)
            down, results = measure(get, jobs)
            down["sha256_ok"] = f"{sum(r[2] for r in results)}/{len(jobs)}"
            rows += [{"workload": workload, "redundancy": redundancy, "operation": "upload", **up},
                     {"workload": workload, "redundancy": redundancy, "operation": "download", **down}]
            uploaded += jobs

    print("\nWorkload results through the gateway "
          f"({a.workers} parallel clients, {len(workloads['large'][1])} large files)")
    print_table(rows, [("workload", "WORKLOAD"), ("redundancy", "REDUNDANCY"), ("operation", "OPERATION"),
                       ("files", "FILES"), ("mb", "MB"), ("seconds", "SECONDS"), ("files_per_s", "FILES/S"),
                       ("mb_per_s", "MB/S"), ("p50_ms", "MEDIAN MS"), ("p95_ms", "P95 MS")])
    checks = [r["sha256_ok"] for r in rows if r["operation"] == "download"]
    print(f"\ndownloads matching their SHA-256: {', '.join(checks)}")
    dedup = sum(r.get("dedup_chunks", 0) for r in rows)
    print(f"chunks deduplicated during the uploads: {dedup} "
          "(the small workload contains 539 duplicate files; more means data was already stored)")
    write_csv(Path(a.out) / "workloads.csv", rows)
    print(f"results written to {Path(a.out) / 'workloads.csv'}")

    if not a.keep:
        print(f"deleting the {len(uploaded)} uploaded objects ...", flush=True)
        with ThreadPoolExecutor(a.workers) as pool:
            list(pool.map(lambda j: client().delete(f"/buckets/{j[0]}/objects/{j[1]}"), uploaded))


if __name__ == "__main__":
    main()
