"""Command-line tool for the storage cluster (used until the gateway exists in Phase 5).

Examples:
  python scripts/cli.py nodes
  python scripts/cli.py mb photos                      (replicated bucket, 3 copies)
  python scripts/cli.py mb archive --redundancy ec     (erasure-coded bucket, 4 + 2)
  python scripts/cli.py put photos cat.jpg C:/pictures/cat.jpg --chunking cdc --tags "type=image,year=2026"
  python scripts/cli.py get photos cat.jpg downloaded.jpg
  python scripts/cli.py ls photos --prefix ca
  python scripts/cli.py stat photos cat.jpg
  python scripts/cli.py rm photos cat.jpg
  python scripts/cli.py put-dir small datasets/small
  python scripts/cli.py stats --bucket photos
  python scripts/cli.py health                         (nodes, degraded objects, repair queue)
  python scripts/cli.py watch --until repaired         (follow failure detection and repair live)
  python scripts/cli.py repairs                        (repair traffic per redundancy scheme)
  python scripts/cli.py scrub                          (re-check every stored chunk now)
"""

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.client import DEFAULT_METADATA_URL, StorageClient, StorageError  # noqa: E402


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def table(rows: list[list], headers: list[str]) -> None:
    rows = [[str(c) for c in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    print("  ".join(h.ljust(w) for h, w in zip(headers, widths)))
    print("  ".join("-" * w for w in widths))
    for r in rows:
        print("  ".join(c.ljust(w) for c, w in zip(r, widths)))


def parse_tags(text: str | None) -> dict:
    return dict(pair.split("=", 1) for pair in text.split(",")) if text else {}


def cmd_nodes(c: StorageClient, a) -> None:
    table([[n["node_id"], n["status"].upper() if n["status"] == "failed" else n["status"],
            f"{n['seconds_since_heartbeat']:.1f}s ago", human(n["free_bytes"]), n["chunk_count"], n["assigned_blobs"]]
           for n in c.nodes()],
          ["NODE", "STATUS", "LAST HEARTBEAT", "FREE SPACE", "BLOBS ON DISK", "BLOBS IN METADATA"])


def cmd_mb(c: StorageClient, a) -> None:
    b = c.create_bucket(a.bucket, a.redundancy)
    print(f"Bucket {b['name']} ({b['redundancy']}) {'created' if b['created'] else 'already exists'}")


def cmd_buckets(c: StorageClient, a) -> None:
    table([[b["name"], b["redundancy"], b["objects"], b["created_at"][:19]] for b in c.list_buckets()],
          ["BUCKET", "REDUNDANCY", "OBJECTS", "CREATED"])


def cmd_put(c: StorageClient, a) -> None:
    t = time.perf_counter()
    r = c.put_file(a.bucket, a.key, a.file, a.chunking, parse_tags(a.tags))
    dt = time.perf_counter() - t
    print(f"Uploaded {a.bucket}/{a.key} version {r['version']} ({r['redundancy']}): {human(r['size'])} in "
          f"{r['chunks']} chunks ({r['new_chunks']} new, {r['deduplicated_chunks']} deduplicated), "
          f"{dt:.2f} s, {r['size'] / dt / 1e6:.1f} MB/s")
    if r["under_replicated_chunks"]:
        print(f"  {r['under_replicated_chunks']} chunk(s) reached the write quorum but not full redundancy "
              f"(a node did not answer); repair queued")
    print(f"SHA-256 of uploaded file:   {r['sha256']}")


def cmd_get(c: StorageClient, a) -> None:
    t = time.perf_counter()
    r = c.get_file(a.bucket, a.key, a.out, a.version)
    dt = time.perf_counter() - t
    print(f"Downloaded {a.bucket}/{a.key} version {r['version']} ({r['redundancy']}) to {a.out}: "
          f"{human(r['size'])} from {len(r['chunks'])} chunks, {dt:.2f} s, {r['size'] / dt / 1e6:.1f} MB/s")
    print(f"SHA-256 of downloaded file: {r['sha256']}")


def cmd_ls(c: StorageClient, a) -> None:
    objs = c.list_objects(a.bucket, a.prefix)
    table([[o["key"], o["version"], human(o["size"]), o["chunking"], o["created_at"][:19],
            ",".join(f"{k}={v}" for k, v in o["tags"].items())] for o in objs],
          ["KEY", "VERSION", "SIZE", "CHUNKING", "CREATED", "TAGS"])
    print(f"{len(objs)} object(s)")


def cmd_stat(c: StorageClient, a) -> None:
    o = c.get_object(a.bucket, a.key, a.version)
    if a.json:
        print(json.dumps(o, default=str))
        return
    print(f"Object:     {o['bucket']}/{o['key']}  (version {o['version']})")
    print(f"Size:       {human(o['size'])} ({o['size']} bytes)")
    print(f"Chunking:   {o['chunking']}   Created: {o['created_at'][:19]}   Tags: {o['tags']}")
    ec = o["redundancy"] == "ec"
    if ec and o["chunks"]:
        print(f"Redundancy: erasure coding {o['chunks'][0]['ec_k']} data + {o['chunks'][0]['ec_m']} parity fragments")
    else:
        print(f"Redundancy: replication, {o['chunks'][0]['replicas'] if o['chunks'] else '-'} copies")
    print(f"Chunks:     {len(o['chunks'])}\n")
    rows = []
    for ch in o["chunks"][:a.limit]:
        def mark(l):
            return "" if l["alive"] and not l["bad"] else " (BAD)" if l["bad"] else " (DOWN)"
        if ec:
            where = "  ".join(f"{l['fragment']}:{l['node_id'].replace('storage-', '')}{mark(l)}" for l in ch["locations"])
        else:
            where = ", ".join(l["node_id"] + mark(l) for l in ch["locations"])
        rows.append([ch["seq"], ch["hash"][:16] + "...", human(ch["size"]), ch["refcount"], where])
    table(rows, ["#", "CHUNK SHA-256", "SIZE", "REFCOUNT", "FRAGMENT:NODE" if ec else "STORED ON"])
    if len(o["chunks"]) > a.limit:
        print(f"... {len(o['chunks']) - a.limit} more chunks (use --limit)")


def cmd_rm(c: StorageClient, a) -> None:
    r = c.delete_object(a.bucket, a.key, a.version)
    print(f"Deleted {a.bucket}/{a.key} ({r['deleted_versions']} version(s)). "
          f"Freed {r['freed_chunks']} chunk(s) no longer used by any object; "
          f"removed {r['removed_copies']} copies from the storage nodes.")


def cmd_stats(c: StorageClient, a) -> None:
    s = c.stats(a.bucket)
    print(f"Scope:                     {'bucket ' + a.bucket if a.bucket else 'whole cluster'}")
    print(f"Objects (versions):        {s['objects']} ({s['versions']})")
    print(f"Logical size (uploaded):   {human(s['logical_bytes'])}")
    print(f"Unique chunks:             {s['unique_chunks']}")
    print(f"Unique size (after dedup): {human(s['unique_bytes'])}")
    print(f"Stored size (all copies):  {human(s['stored_bytes'])} in {s['stored_blobs']} blobs on the nodes")
    if s["dedup_ratio"]:
        print(f"Deduplication ratio:       {s['dedup_ratio']:.2f}x  ({s['dedup_saving']:.1%} space saved)")
        print(f"Storage overhead:          {s['storage_overhead']:.2f}x the unique data")


def health_line(h: dict, t0: float) -> str:
    n, o, r = h["nodes"], h["objects"], h["repair_jobs"]
    failed = ",".join(x.replace("storage-", "") for x in n["failed"]) or "none"
    return (f"t={time.perf_counter() - t0:5.1f}s  nodes alive {n['alive']}/{n['total']}  failed: {failed:<8}  "
            f"degraded objects {o['degraded']:>4}  lost {o['lost']:>3}  |  repair jobs: {r['pending']:>5} pending "
            f"{r['running']:>3} running {r['done']:>6} done {r['failed']:>3} failed")


def cmd_health(c: StorageClient, a) -> None:
    h = c.cluster()
    s = h["settings"]
    print(f"Settings:  replication N={s['N']} W={s['W']} R={s['R']}   erasure coding {s['ec']}   "
          f"node marked failed after {s['node_timeout_s']:.0f} s without heartbeat\n")
    table([[n["node_id"], n["status"].upper() if n["status"] == "failed" else n["status"],
            f"{n['seconds_since_heartbeat']:.1f}s ago", n["assigned_blobs"]] for n in h["nodes"]["list"]],
          ["NODE", "STATUS", "LAST HEARTBEAT", "BLOBS"])
    ch, o = h["chunks"], h["objects"]
    print(f"\nChunks:   {ch['healthy']} healthy, {ch['degraded']} degraded, {ch['lost']} lost")
    for red, st in sorted(ch["by_redundancy"].items()):
        print(f"          {red:<12} {st['healthy']} healthy, {st['degraded']} degraded, {st['lost']} lost")
    print(f"Objects:  {o['total']} total, {o['degraded']} degraded, {o['lost']} unreadable")
    r, sc = h["repair_jobs"], h["scrub_jobs"]
    print(f"Repairs:  {r['pending']} pending, {r['running']} running, {r['done']} done, {r['failed']} failed")
    print(f"Scrubs:   {sc['pending']} pending, {sc['running']} running, {sc['done']} done")


def cmd_watch(c: StorageClient, a) -> None:
    """Print the cluster state every 2 s until the condition holds (or the timeout)."""
    t0 = time.perf_counter()
    seen_failure = False
    while time.perf_counter() - t0 < a.timeout:
        h = c.cluster()
        print(health_line(h, t0), flush=True)
        failed = bool(h["nodes"]["failed"])
        seen_failure |= failed
        idle = h["repair_jobs"]["pending"] + h["repair_jobs"]["running"] == 0
        if a.until == "failed" and failed:
            return
        if a.until == "repaired" and seen_failure and idle and h["objects"]["degraded"] == 0:
            print("Repair finished: every object is back to full redundancy.")
            return
        if a.until == "healthy" and not failed and idle and h["objects"]["degraded"] == 0:
            return
        time.sleep(2)
    print(f"stopped after {a.timeout:.0f} s")


def cmd_repairs(c: StorageClient, a) -> None:
    r = c.repair_stats(a.bucket, a.since)
    table([[x["redundancy"], x["jobs"], x["rebuilt"], x["pending"] + x["running"], x["failed"],
            human(x["bytes_read"]), human(x["bytes_written"]),
            f"{x['bytes_read'] / x['bytes_written']:.1f}x" if x["bytes_written"] else "-",
            f"{x['seconds']:.1f} s" if x["seconds"] else "-"] for x in r["by_redundancy"]],
          ["REDUNDANCY", "JOBS", "REBUILT", "WAITING", "FAILED", "READ", "WRITTEN", "READ / WRITTEN", "TIME"])


def cmd_scrub(c: StorageClient, a) -> None:
    r = c.scrub()
    print(f"Scrub queued for {len(r['scrub_queued_for'])} nodes: {', '.join(r['scrub_queued_for'])}")
    print("The repair worker re-hashes every blob on each node; follow it with: docker compose logs -f repair-worker")


def cmd_put_dir(c: StorageClient, a) -> None:
    root = Path(a.dir)
    files = sorted(p for p in root.rglob("*") if p.is_file())
    local = threading.local()

    def upload(p: Path) -> dict:
        if not hasattr(local, "client"):
            local.client = StorageClient(a.metadata)
        return local.client.put_file(a.bucket, p.relative_to(root).as_posix(), str(p), a.chunking)

    total = new = dedup = done = 0
    t = time.perf_counter()
    with ThreadPoolExecutor(a.workers) as pool:
        for fut in as_completed([pool.submit(upload, p) for p in files]):
            r = fut.result()
            total += r["size"]
            new += r["new_chunks"]
            dedup += r["deduplicated_chunks"]
            done += 1
            if done % 500 == 0 or done == len(files):
                print(f"  {done}/{len(files)} files")
    dt = time.perf_counter() - t
    print(f"Uploaded {len(files)} files ({human(total)}) to bucket {a.bucket} in {dt:.1f} s: "
          f"{len(files) / dt:.0f} files/s, {total / dt / 1e6:.1f} MB/s. "
          f"Chunks: {new} new, {dedup} deduplicated.")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--metadata", default=os.environ.get("METADATA_URL", DEFAULT_METADATA_URL))
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("nodes", help="storage nodes and their heartbeats").set_defaults(fn=cmd_nodes)
    s = sub.add_parser("mb", help="make a bucket")
    s.add_argument("bucket")
    s.add_argument("--redundancy", default="replication", choices=["replication", "ec"])
    s.set_defaults(fn=cmd_mb)
    sub.add_parser("buckets", help="list buckets").set_defaults(fn=cmd_buckets)
    for name, fn, help_ in (("put", cmd_put, "upload a file"), ("put-dir", cmd_put_dir, "upload a folder")):
        s = sub.add_parser(name, help=help_)
        s.add_argument("bucket")
        if name == "put":
            s.add_argument("key")
            s.add_argument("file")
            s.add_argument("--tags", help='e.g. "type=video,project=lab" (quote it in PowerShell)')
        else:
            s.add_argument("dir")
            s.add_argument("--workers", type=int, default=8)
        s.add_argument("--chunking", default="fixed", choices=["fixed", "cdc"])
        s.set_defaults(fn=fn)
    for name, fn, help_ in (("get", cmd_get, "download an object"), ("stat", cmd_stat, "object metadata and chunks"),
                            ("rm", cmd_rm, "delete an object")):
        s = sub.add_parser(name, help=help_)
        s.add_argument("bucket")
        s.add_argument("key")
        if name == "get":
            s.add_argument("out")
        if name == "stat":
            s.add_argument("--limit", type=int, default=20, help="chunks to show")
            s.add_argument("--json", action="store_true", help="print the raw metadata")
        s.add_argument("--version", type=int)
        s.set_defaults(fn=fn)
    s = sub.add_parser("ls", help="list objects")
    s.add_argument("bucket")
    s.add_argument("--prefix", default="")
    s.set_defaults(fn=cmd_ls)
    s = sub.add_parser("stats", help="storage, deduplication and overhead statistics")
    s.add_argument("--bucket")
    s.set_defaults(fn=cmd_stats)
    sub.add_parser("health", help="cluster health and repair queue").set_defaults(fn=cmd_health)
    s = sub.add_parser("watch", help="follow failure detection and repair live")
    s.add_argument("--until", choices=["failed", "repaired", "healthy"], default="repaired")
    s.add_argument("--timeout", type=float, default=300)
    s.set_defaults(fn=cmd_watch)
    s = sub.add_parser("repairs", help="repair jobs and repair traffic per redundancy scheme")
    s.add_argument("--bucket")
    s.add_argument("--since", type=int, default=0, help="only jobs with an id above this")
    s.set_defaults(fn=cmd_repairs)
    sub.add_parser("scrub", help="scrub every node now").set_defaults(fn=cmd_scrub)

    a = p.parse_args()
    try:
        with StorageClient(a.metadata) as c:
            a.fn(c, a)
    except StorageError as e:
        sys.exit(f"error: {e}")


if __name__ == "__main__":
    main()
