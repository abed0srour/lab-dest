"""Measure how much data moves when a storage node is added or removed, and how evenly
the hash ring spreads data.

Every chunk of the real datasets (fixed 4 MB chunks) is placed on N = 3 nodes,
first with the consistent hashing ring (128 virtual nodes per node), then with
simple modulo placement (hash mod number_of_nodes) for comparison.

  Add a node:    4 nodes -> 5 nodes  (storage-node-5 joins)
  Remove a node: 5 nodes -> 4 nodes  (storage-node-3 leaves)

The best possible result is that only the new node's fair share (1/5 of all copies)
moves when adding, and only the removed node's copies (also 1/5) move when removing.
The same 1/5 is the ideal for "primary moved", the share of chunks whose first
(primary) node changes.

Usage: python scripts/bench_placement.py [--datasets datasets] [--out benchmarks/results]
"""

import argparse
import statistics
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.chunking import chunk_stream, sha256  # noqa: E402
from common.ring import HashRing  # noqa: E402
from bench_chunking import print_table, write_csv  # noqa: E402

N = 3
FOUR = [f"storage-node-{i}" for i in range(1, 5)]
FIVE = FOUR + ["storage-node-5"]
WITHOUT_3 = [n for n in FIVE if n != "storage-node-3"]


def dataset_hashes(root: Path) -> list[str]:
    hashes = set()
    for path in sorted(f for f in root.rglob("*") if f.is_file()):
        with open(path, "rb") as f:
            hashes.update(sha256(c) for c in chunk_stream(f, "fixed"))
    return sorted(hashes)


def ring_placer(nodes: list[str], vnodes: int = 128):
    ring = HashRing(nodes, vnodes)
    return lambda h: ring.nodes_for(h, N)


def modulo_placer(nodes: list[str]):
    return lambda h: [nodes[(int(h[:16], 16) + i) % len(nodes)] for i in range(N)]


def movement(hashes: list[str], before, after) -> tuple[float, float, float]:
    """% of all stored copies that must be copied to a new node, % of chunks with at least
    one moved copy, and % of chunks whose primary (first) node changed."""
    moved = touched = primary = 0
    for h in hashes:
        a, b = before(h), after(h)
        new_copies = len(set(b) - set(a))
        moved += new_copies
        touched += new_copies > 0
        primary += a[0] != b[0]
    n = len(hashes)
    return round(moved / (N * n) * 100, 1), round(touched / n * 100, 1), round(primary / n * 100, 1)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--datasets", default="datasets")
    p.add_argument("--out", default="benchmarks/results")
    a = p.parse_args()
    out = Path(a.out)

    print("hashing the dataset chunks ...", flush=True)
    hashes = dataset_hashes(Path(a.datasets))
    print(f"{len(hashes)} unique chunks, {N} copies each\n")

    rows = []
    for scenario, before, after in (("add node (4 -> 5)", FOUR, FIVE), ("remove node (5 -> 4)", FIVE, WITHOUT_3)):
        for method, make in (("consistent hashing ring", ring_placer), ("modulo (hash mod n)", modulo_placer)):
            copies, chunks, primary = movement(hashes, make(before), make(after))
            rows.append({"scenario": scenario, "method": method, "copies_moved_pct": copies,
                         "chunks_affected_pct": chunks, "primary_moved_pct": primary, "ideal_pct": 20.0})
    print("Data movement when the cluster changes")
    print_table(rows, [("scenario", "SCENARIO"), ("method", "PLACEMENT"), ("copies_moved_pct", "COPIES MOVED %"),
                       ("chunks_affected_pct", "CHUNKS AFFECTED %"), ("primary_moved_pct", "PRIMARY MOVED %"),
                       ("ideal_pct", "IDEAL %")])
    write_csv(out / "placement_movement.csv", rows)

    balance = []
    for vnodes in (1, 8, 32, 128, 512):
        place = ring_placer(FOUR, vnodes)
        load = Counter(node for h in hashes for node in place(h))
        counts = [load[n] for n in FOUR]
        mean = statistics.mean(counts)
        balance.append({"vnodes": vnodes, **{n.replace("storage-", ""): load[n] for n in FOUR},
                        "max_over_avg_pct": round((max(counts) / mean - 1) * 100, 1),
                        "std_dev_pct": round(statistics.pstdev(counts) / mean * 100, 1)})
    print("\nChunk copies per node (4 nodes) for different numbers of virtual nodes")
    print_table(balance, [("vnodes", "VNODES")] + [(n.replace("storage-", ""), n) for n in FOUR]
                + [("max_over_avg_pct", "BUSIEST NODE vs AVG %"), ("std_dev_pct", "STD DEV %")])
    write_csv(out / "placement_balance.csv", balance)
    print(f"\nresults written to {out / 'placement_movement.csv'} and {out / 'placement_balance.csv'}")


if __name__ == "__main__":
    main()
