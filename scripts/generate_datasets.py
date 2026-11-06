"""Generate the two test workloads (deterministic: the same seed gives the same files).

  small/  5,000 text and JSON files of 1-50 KB in five folders (logs, notes, users,
          orders, configs). About 10% are exact copies of earlier files, so
          deduplication has something to find.
  large/  10 "disk images" of 100-300 MB. disk-00..04 are random; disk-05..09 are
          new versions of disk-00..04 with a few bytes inserted and overwritten,
          like an updated VM image. Inserted bytes shift everything after them,
          which is what separates fixed-size from content-defined chunking.

Usage: python scripts/generate_datasets.py [--out datasets] [--only small|large]
"""

import argparse
import json
import random
import time
from pathlib import Path

KB, MB = 1024, 1024 * 1024
TEXT_DIRS = ("logs", "notes")
JSON_DIRS = ("users", "orders", "configs")


def vocabulary(rng: random.Random, size: int = 800) -> list[str]:
    syllables = ["ka", "lo", "mi", "ra", "te", "nu", "si", "po", "de", "ba", "ve", "zo", "ul", "an", "er", "is"]
    return ["".join(rng.choices(syllables, k=rng.randint(1, 4))) for _ in range(size)]


def text_file(rng: random.Random, words: list[str], target: int) -> bytes:
    lines, size = [], 0
    while size < target:
        line = f"2026-11-{rng.randint(1, 30):02d} {rng.randint(0, 23):02d}:{rng.randint(0, 59):02d} " \
               f"{rng.choice(['INFO', 'WARN', 'ERROR', 'DEBUG'])} " + " ".join(rng.choices(words, k=rng.randint(5, 20)))
        lines.append(line)
        size += len(line) + 1
    return ("\n".join(lines) + "\n").encode()[:target]


def json_file(rng: random.Random, words: list[str], target: int) -> bytes:
    records = []
    while True:
        records.append({
            "id": rng.randint(1, 10**9),
            "name": " ".join(rng.choices(words, k=2)).title(),
            "email": f"{rng.choice(words)}{rng.randint(1, 999)}@example.com",
            "score": round(rng.random() * 100, 2),
            "active": rng.random() < 0.8,
            "tags": rng.sample(words, k=3),
            "note": " ".join(rng.choices(words, k=rng.randint(5, 25))),
        })
        data = json.dumps(records, indent=2).encode()
        if len(data) >= target or len(data) > 49 * KB:
            return data


def generate_small(out: Path, count: int, seed: int) -> None:
    rng = random.Random(seed)
    words = vocabulary(rng)
    written: list[bytes] = []
    duplicates = total = 0
    for i in range(count):
        folder = rng.choice(TEXT_DIRS + JSON_DIRS)
        ext = "txt" if folder in TEXT_DIRS else "json"
        if written and rng.random() < 0.10:
            data = rng.choice(written)
            duplicates += 1
        else:
            target = rng.randint(1 * KB, 49 * KB)
            data = text_file(rng, words, target) if ext == "txt" else json_file(rng, words, target)
            written.append(data)
        path = out / "small" / folder / f"{folder[:-1]}-{i:05d}.{ext}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        total += len(data)
    print(f"small: {count} files, {total / MB:.1f} MB total, {duplicates} exact duplicates")


def edited(rng: random.Random, base: bytes) -> bytes:
    """A new version of base: 5 small insertions and 5 overwritten 4 KB blocks."""
    data = bytearray(base)
    for _ in range(5):
        pos = rng.randrange(len(data))
        data[pos:pos + 4 * KB] = rng.randbytes(4 * KB)
    for _ in range(5):
        pos = rng.randrange(len(data))
        data[pos:pos] = rng.randbytes(rng.randint(1, 100))
    return bytes(data)


def generate_large(out: Path, count: int, min_mb: int, max_mb: int, seed: int) -> None:
    rng = random.Random(seed + 1)
    folder = out / "large"
    folder.mkdir(parents=True, exist_ok=True)
    bases = count // 2
    total = 0
    for i in range(count):
        if i < bases:
            data = rng.randbytes(rng.randint(min_mb, max_mb) * MB)
            what = "random"
        else:
            data = edited(rng, (folder / f"disk-{i - bases:02d}.img").read_bytes())
            what = f"new version of disk-{i - bases:02d}.img"
        (folder / f"disk-{i:02d}.img").write_bytes(data)
        total += len(data)
        print(f"  disk-{i:02d}.img  {len(data) / MB:7.1f} MB  ({what})")
    print(f"large: {count} files, {total / MB / 1024:.2f} GB total")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default="datasets")
    p.add_argument("--only", choices=["small", "large"])
    p.add_argument("--small-count", type=int, default=5000)
    p.add_argument("--large-count", type=int, default=10)
    p.add_argument("--large-min-mb", type=int, default=100)
    p.add_argument("--large-max-mb", type=int, default=300)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    out = Path(a.out)
    t = time.perf_counter()
    if a.only != "large":
        generate_small(out, a.small_count, a.seed)
    if a.only != "small":
        generate_large(out, a.large_count, a.large_min_mb, a.large_max_mb, a.seed)
    print(f"done in {time.perf_counter() - t:.1f} s -> {out.resolve()}")


if __name__ == "__main__":
    main()
