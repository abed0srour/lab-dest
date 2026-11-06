"""Compare fixed-size chunking with content-defined chunking (FastCDC) on both workloads.

Part 1, for each workload and method: number of chunks, average chunk size,
unique chunks, deduplication ratio, and speed (read + chunk + SHA-256).
Part 2, the edit test: insert a few bytes in the middle of a large file and
count how many chunks change, i.e. how much would have to be stored again.

Usage: python scripts/bench_chunking.py [--datasets datasets] [--out benchmarks/results]
"""

import argparse
import csv
import io
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.chunking import MiB, chunk_stream, sha256  # noqa: E402

METHODS = {"fixed": "Fixed 4 MB", "cdc": "FastCDC (avg 4 MB)"}


def scan(files: list[Path], method: str) -> dict:
    seen: dict[str, int] = {}
    logical = count = 0
    t = time.perf_counter()
    for path in files:
        with open(path, "rb") as f:
            for chunk in chunk_stream(f, method):
                count += 1
                logical += len(chunk)
                seen.setdefault(sha256(chunk), len(chunk))
    dt = time.perf_counter() - t
    unique = sum(seen.values())
    return {"chunks": count, "avg_chunk_kb": round(logical / count / 1024, 1), "unique_chunks": len(seen),
            "logical_mb": round(logical / MiB, 1), "unique_mb": round(unique / MiB, 1),
            "dedup_ratio": round(logical / unique, 3), "saving_pct": round((1 - unique / logical) * 100, 1),
            "speed_mb_s": round(logical / dt / 1e6)}


def edit_test(path: Path, method: str, insert: bytes = b"0123456789") -> dict:
    base = path.read_bytes()
    mid = len(base) // 2
    modified = base[:mid] + insert + base[mid:]
    before = {sha256(c) for c in chunk_stream(io.BytesIO(base), method)}
    after = [(sha256(c), len(c)) for c in chunk_stream(io.BytesIO(modified), method)]
    changed = [(h, n) for h, n in after if h not in before]
    return {"chunks": len(after), "changed_chunks": len(changed),
            "changed_pct": round(len(changed) / len(after) * 100, 1),
            "restore_mb": round(sum(n for _, n in changed) / MiB, 1)}


def print_table(rows: list[dict], cols: list[tuple[str, str]]) -> None:
    widths = [max(len(title), *(len(str(r[key])) for r in rows)) for key, title in cols]
    print("  ".join(t.ljust(w) for (_, t), w in zip(cols, widths)))
    print("  ".join("-" * w for w in widths))
    for r in rows:
        print("  ".join(str(r[k]).ljust(w) for (k, _), w in zip(cols, widths)))


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(dict.fromkeys(k for r in rows for k in r))  # rows may have different columns
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, restval="")
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--datasets", default="datasets")
    p.add_argument("--out", default="benchmarks/results")
    a = p.parse_args()
    root, out = Path(a.datasets), Path(a.out)

    rows = []
    for workload in ("small", "large"):
        files = sorted(f for f in (root / workload).rglob("*") if f.is_file())
        for method, label in METHODS.items():
            print(f"chunking {len(files)} {workload} files with {label} ...", flush=True)
            rows.append({"workload": workload, "files": len(files), "method": label, **scan(files, method)})
    print()
    print_table(rows, [("workload", "WORKLOAD"), ("files", "FILES"), ("method", "METHOD"), ("chunks", "CHUNKS"),
                       ("avg_chunk_kb", "AVG CHUNK KB"), ("unique_chunks", "UNIQUE"), ("logical_mb", "LOGICAL MB"),
                       ("unique_mb", "UNIQUE MB"), ("dedup_ratio", "DEDUP RATIO"), ("saving_pct", "SAVED %"),
                       ("speed_mb_s", "MB/s")])
    write_csv(out / "chunking.csv", rows)

    target = root / "large" / "disk-00.img"
    edits = [{"file": target.name, "method": label, **edit_test(target, method)} for method, label in METHODS.items()]
    print(f"\nEdit test: 10 bytes inserted in the middle of {target.name} ({target.stat().st_size / MiB:.0f} MB)")
    print_table(edits, [("method", "METHOD"), ("chunks", "CHUNKS"), ("changed_chunks", "CHANGED CHUNKS"),
                        ("changed_pct", "CHANGED %"), ("restore_mb", "MB TO STORE AGAIN")])
    write_csv(out / "chunking_edit.csv", edits)
    print(f"\nresults written to {out / 'chunking.csv'} and {out / 'chunking_edit.csv'}")


if __name__ == "__main__":
    main()
