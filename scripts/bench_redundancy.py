"""Compare replication (3 copies) with erasure coding (4 data + 2 parity) on both workloads:
storage overhead, and repair traffic after a storage node fails.

  1. Upload the small workload (5,000 files) and the first --large-files disk images
     into four buckets: small-rep, small-ec, large-rep, large-ec.
  2. Storage overhead per bucket = bytes stored on the nodes / unique bytes.
  3. Kill the storage node that holds the most data, wait until the metadata service
     marks it FAILED and the repair worker has rebuilt everything it held.
  4. Repair traffic per bucket = bytes the repair jobs read and wrote.
  5. Start the node again.

Run it on a fresh cluster so the numbers only reflect this run:
  docker compose down -v ; docker compose up -d
Takes about 5-10 minutes.

Usage: python scripts/bench_redundancy.py [--large-files 4] [--node storage-node-7]
"""

import argparse
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.chunking import MiB  # noqa: E402
from common.client import StorageClient  # noqa: E402
from bench_chunking import print_table, write_csv  # noqa: E402

SCHEMES = {"rep": "replication", "ec": "ec"}


def upload(bucket: str, root: Path, files: list[Path], workers: int = 8) -> float:
    local = threading.local()

    def put(p: Path) -> None:
        if not hasattr(local, "client"):
            local.client = StorageClient()
        local.client.put_file(bucket, p.relative_to(root).as_posix(), str(p))

    t = time.perf_counter()
    with ThreadPoolExecutor(workers) as pool:
        list(pool.map(put, files))
    return time.perf_counter() - t


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, capture_output=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--datasets", default=str(ROOT / "datasets"))
    p.add_argument("--large-files", type=int, default=4)
    p.add_argument("--node", help="node to fail (default: the one holding the most blobs)")
    p.add_argument("--out", default=str(ROOT / "benchmarks" / "results"))
    a = p.parse_args()
    datasets = Path(a.datasets)
    workloads = {
        "small": (datasets / "small", sorted(f for f in (datasets / "small").rglob("*") if f.is_file())),
        "large": (datasets / "large", sorted((datasets / "large").glob("*.img"))[:a.large_files]),
    }

    with StorageClient() as c:
        alive = c.cluster()["nodes"]["alive"]
        if alive < 7:
            print(f"warning: {alive} storage nodes alive; 7 are needed for EC 4+2 to repair without doubling up")

        # 1-2. upload and measure storage overhead
        rows = {}
        for workload, (root, files) in workloads.items():
            for tag, redundancy in SCHEMES.items():
                bucket = f"{workload}-{tag}"
                c.create_bucket(bucket, redundancy)
                print(f"uploading {len(files)} {workload} files to {bucket} ({redundancy}) ...", flush=True)
                seconds = upload(bucket, root, files)
                s = c.stats(bucket)
                rows[bucket] = {"workload": workload, "redundancy": redundancy, "files": len(files),
                                "unique_mb": round(s["unique_bytes"] / MiB, 1),
                                "stored_mb": round(s["stored_bytes"] / MiB, 1),
                                "overhead": s["storage_overhead"], "blobs": s["stored_blobs"],
                                "upload_s": round(seconds, 1)}

        # 3. fail the busiest node
        node = a.node or max(c.nodes(), key=lambda n: n["assigned_blobs"])["node_id"]
        since = c.repair_stats()["last_job_id"]
        print(f"\nkilling {node} (holds {next(n for n in c.nodes() if n['node_id'] == node)['assigned_blobs']} blobs)")
        compose("kill", node)
        t0 = time.perf_counter()
        detected = None
        try:
            while time.perf_counter() - t0 < 1800:
                h = c.cluster()
                if detected is None and node in h["nodes"]["failed"]:
                    detected = time.perf_counter() - t0
                    print(f"  {node} marked FAILED after {detected:.1f} s")
                q = h["repair_jobs"]
                if detected is not None:
                    print(f"  t={time.perf_counter() - t0:6.1f}s  repair jobs: {q['pending']} pending, "
                          f"{q['running']} running   degraded objects: {h['objects']['degraded']}", flush=True)
                    if q["pending"] + q["running"] == 0 and h["objects"]["degraded"] == 0:
                        break
                time.sleep(3)
            repaired = time.perf_counter() - t0

            # 4. repair traffic per bucket
            for bucket, row in rows.items():
                stats = c.repair_stats(bucket, since)["by_redundancy"]
                r = stats[0] if stats else {"rebuilt": 0, "bytes_read": 0, "bytes_written": 0, "seconds": 0}
                row.update({"repairs": r["rebuilt"], "repair_read_mb": round(r["bytes_read"] / MiB, 1),
                            "repair_written_mb": round(r["bytes_written"] / MiB, 1),
                            "read_per_written": round(r["bytes_read"] / r["bytes_written"], 2)
                            if r["bytes_written"] else None})
        finally:
            # 5. bring the node back
            compose("start", node)
    print(f"\n{node} detected as failed after {detected:.1f} s; cluster fully repaired {repaired:.0f} s after the kill")

    table = list(rows.values())
    print("\nStorage overhead and repair traffic: replication (3 copies) vs erasure coding (4 + 2)")
    print_table(table, [("workload", "WORKLOAD"), ("redundancy", "REDUNDANCY"), ("files", "FILES"),
                        ("unique_mb", "UNIQUE MB"), ("stored_mb", "STORED MB"), ("overhead", "OVERHEAD x"),
                        ("blobs", "BLOBS"), ("repairs", "REBUILT"), ("repair_read_mb", "REPAIR READ MB"),
                        ("repair_written_mb", "REPAIR WRITTEN MB"), ("read_per_written", "READ / WRITTEN")])
    write_csv(Path(a.out) / "redundancy.csv", table)
    print(f"\nresults written to {Path(a.out) / 'redundancy.csv'}")


if __name__ == "__main__":
    main()
