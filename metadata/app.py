"""Metadata service: buckets, objects, chunk reference counts, node registry, placement,
failure detection and cluster health.

The service never touches chunk data on the write path. Clients ask it where to
put new chunks (/plan), write the replicas or erasure-coded fragments to the
storage nodes themselves, and then commit the object, which records the chunk
list, the locations and the reference counts.
"""

import logging
import os
import threading
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated

import httpx
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi import Path as PathParam
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from common.db import CHUNK_STATES, SCHEMA, alive_nodes, enqueue_repair, report_bad_copy
from common.ring import HashRing

log = logging.getLogger("metadata")

BucketName = Annotated[str, PathParam(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")]
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Redundancy = Annotated[str, Field(pattern="^(replication|ec)$")]


class Settings:
    def __init__(self):
        env = os.environ.get
        self.replicas = int(env("REPLICAS", "3"))             # N
        self.write_quorum = int(env("WRITE_QUORUM", "2"))     # W
        self.read_quorum = int(env("READ_QUORUM", "2"))       # R
        self.ec_k = int(env("EC_K", "4"))
        self.ec_m = int(env("EC_M", "2"))
        self.ec_write_quorum = int(env("EC_WRITE_QUORUM", str(self.ec_k + 1)))
        self.vnodes = int(env("RING_VNODES", "128"))
        self.node_timeout = float(env("NODE_TIMEOUT", "15"))  # 3 missed heartbeats of 5 s
        self.check_interval = float(env("FAILURE_CHECK_INTERVAL", "2"))
        if self.write_quorum + self.read_quorum <= self.replicas:
            raise RuntimeError(f"quorum rule violated: W + R = {self.write_quorum + self.read_quorum} "
                               f"must be greater than N = {self.replicas}")
        if not 1 <= self.write_quorum <= self.replicas or not 1 <= self.read_quorum <= self.replicas:
            raise RuntimeError("W and R must be between 1 and N")
        if not self.ec_k <= self.ec_write_quorum <= self.ec_k + self.ec_m:
            raise RuntimeError("EC_WRITE_QUORUM must be between k and k + m")


class Heartbeat(BaseModel):
    node_id: str
    url: str
    public_url: str | None = None
    free_bytes: int
    chunk_count: int


class BucketIn(BaseModel):
    redundancy: Redundancy = "replication"


class PlanIn(BaseModel):
    bucket: str
    hashes: list[Sha256]


class Location(BaseModel):
    node_id: str
    fragment: int = -1   # -1 = full replica
    blob_hash: Sha256
    blob_size: int


class ChunkRef(BaseModel):
    hash: Sha256
    size: int
    locations: list[Location] | None = None  # where the client just wrote it; None = already stored (dedup)


class ObjectIn(BaseModel):
    size: int
    chunking: str = "fixed"
    tags: dict[str, str] = {}
    sha256: Sha256 | None = None
    content_type: str | None = None
    chunks: list[ChunkRef]


class BadCopy(BaseModel):
    chunk_hash: Sha256
    redundancy: Redundancy
    fragment: int = -1
    node_id: str
    reason: str = "read error"


def like_prefix(prefix: str) -> str:
    return prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


def create_app() -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = Settings()
    pool = ConnectionPool(
        os.environ.get("DATABASE_URL", "postgresql://dos:dos@localhost:5432/dos"),
        min_size=2, max_size=20, open=False, kwargs={"row_factory": dict_row},
    )
    stop = threading.Event()

    def failure_detector():
        """Mark nodes that stopped sending heartbeats as failed and queue repairs for their chunks."""
        while not stop.wait(cfg.check_interval):
            try:
                with pool.connection() as conn:
                    failed = conn.execute(
                        """UPDATE nodes SET status = 'failed', failed_at = now()
                           WHERE status = 'alive' AND last_heartbeat < now() - make_interval(secs => %s)
                           RETURNING node_id, EXTRACT(EPOCH FROM now() - last_heartbeat)::float AS silent""",
                        (cfg.node_timeout,)).fetchall()
                    for f in failed:
                        queued = conn.execute(
                            """INSERT INTO repair_jobs (kind, chunk_hash, redundancy, node_id, reason)
                               SELECT DISTINCT 'repair', chunk_hash, redundancy, node_id, 'node failed'
                               FROM chunk_locations WHERE node_id = %s
                               ON CONFLICT (chunk_hash, redundancy) WHERE kind = 'repair' AND status = 'pending'
                               DO NOTHING""", (f["node_id"],)).rowcount
                        log.warning("node %s marked FAILED: no heartbeat for %.1f s; %d repair jobs queued",
                                    f["node_id"], f["silent"], queued)
            except Exception:
                log.exception("failure detector error")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool.open(wait=True, timeout=60)
        with pool.connection() as conn:
            conn.execute(SCHEMA)
        log.info("quorum N=%d W=%d R=%d, erasure coding %d+%d (write quorum %d), node timeout %.0f s",
                 cfg.replicas, cfg.write_quorum, cfg.read_quorum, cfg.ec_k, cfg.ec_m, cfg.ec_write_quorum,
                 cfg.node_timeout)
        detector = threading.Thread(target=failure_detector, daemon=True)
        detector.start()
        yield
        stop.set()
        pool.close()

    app = FastAPI(title="Metadata service", lifespan=lifespan)

    def bucket_redundancy(conn, bucket: str) -> str:
        row = conn.execute("SELECT redundancy FROM buckets WHERE name = %s", (bucket,)).fetchone()
        if row is None:
            raise HTTPException(404, f"bucket {bucket!r} does not exist")
        return row["redundancy"]

    @app.get("/health")
    def health():
        return {"status": "ok"}

    # ---- nodes -------------------------------------------------------------

    @app.post("/heartbeat")
    def heartbeat(hb: Heartbeat):
        with pool.connection() as conn:
            prev = conn.execute("SELECT status FROM nodes WHERE node_id = %s", (hb.node_id,)).fetchone()
            conn.execute(
                """INSERT INTO nodes (node_id, url, public_url, free_bytes, chunk_count, last_heartbeat)
                   VALUES (%s, %s, %s, %s, %s, now())
                   ON CONFLICT (node_id) DO UPDATE SET
                     url = EXCLUDED.url, public_url = EXCLUDED.public_url, free_bytes = EXCLUDED.free_bytes,
                     chunk_count = EXCLUDED.chunk_count, last_heartbeat = now(), status = 'alive', failed_at = NULL""",
                (hb.node_id, hb.url, hb.public_url, hb.free_bytes, hb.chunk_count))
            if prev is None:
                log.info("node %s joined the cluster", hb.node_id)
            elif prev["status"] == "failed":
                # Its disk may hold copies that missed updates or decayed while it was away.
                conn.execute("""INSERT INTO repair_jobs (kind, node_id, reason) VALUES ('scrub', %s, 'node rejoined')
                                ON CONFLICT (node_id) WHERE kind = 'scrub' AND status IN ('pending', 'running')
                                DO NOTHING""", (hb.node_id,))
                log.info("node %s is back after a failure; it rejoins the ring and will be scrubbed", hb.node_id)
        return {"ok": True}

    @app.get("/nodes")
    def list_nodes():
        with pool.connection() as conn:
            return conn.execute(
                """SELECT n.node_id, n.url, n.public_url, n.status, n.free_bytes, n.chunk_count, n.last_heartbeat,
                          n.failed_at, EXTRACT(EPOCH FROM now() - n.last_heartbeat)::float AS seconds_since_heartbeat,
                          (SELECT count(*) FROM chunk_locations l WHERE l.node_id = n.node_id) AS assigned_blobs
                   FROM nodes n ORDER BY n.node_id""").fetchall()

    # ---- buckets -----------------------------------------------------------

    @app.put("/buckets/{bucket}", status_code=201)
    def create_bucket(bucket: BucketName, response: Response, body: BucketIn | None = None):
        body = body or BucketIn()
        with pool.connection() as conn:
            created = conn.execute(
                "INSERT INTO buckets (name, redundancy) VALUES (%s, %s) ON CONFLICT DO NOTHING RETURNING name",
                (bucket, body.redundancy)).fetchone() is not None
            row = conn.execute("SELECT * FROM buckets WHERE name = %s", (bucket,)).fetchone()
        if not created:
            response.status_code = 200
        return {**row, "created": created}

    @app.get("/buckets/{bucket}")
    def get_bucket(bucket: str):
        with pool.connection() as conn:
            row = conn.execute("SELECT * FROM buckets WHERE name = %s", (bucket,)).fetchone()
        if row is None:
            raise HTTPException(404, f"bucket {bucket!r} does not exist")
        return row

    @app.get("/buckets")
    def list_buckets():
        with pool.connection() as conn:
            return conn.execute(
                """SELECT b.name, b.redundancy, b.created_at, count(DISTINCT o.key) AS objects
                   FROM buckets b LEFT JOIN objects o ON o.bucket = b.name
                   GROUP BY b.name ORDER BY b.name""").fetchall()

    # ---- placement and dedup -----------------------------------------------

    @app.post("/plan")
    def plan(body: PlanIn):
        """For each chunk hash: is it already stored (dedup)? If not, which nodes get its copies or fragments."""
        hashes = list(dict.fromkeys(body.hashes))
        with pool.connection() as conn:
            redundancy = bucket_redundancy(conn, body.bucket)
            existing = {r["hash"] for r in conn.execute(
                "SELECT hash FROM chunks WHERE redundancy = %s AND hash = ANY(%s)", (redundancy, hashes))}
            nodes = alive_nodes(conn, cfg.node_timeout)
        if redundancy == "ec":
            width, quorum = cfg.ec_k + cfg.ec_m, cfg.ec_write_quorum
            if len(nodes) < width:
                raise HTTPException(503, f"erasure coding {cfg.ec_k}+{cfg.ec_m} needs {width} alive storage nodes, "
                                         f"only {len(nodes)} are alive")
        else:
            width, quorum = cfg.replicas, cfg.write_quorum
            if len(nodes) < quorum:
                raise HTTPException(503, f"write quorum W={quorum} needs {quorum} alive storage nodes, "
                                         f"only {len(nodes)} are alive")
        ring = HashRing(nodes, cfg.vnodes)
        chunks = {h: {"exists": True, "nodes": []} if h in existing
                  else {"exists": False, "nodes": [nodes[n] for n in ring.nodes_for(h, width)]} for h in hashes}
        return {"redundancy": redundancy, "width": width, "write_quorum": quorum,
                "ec": {"k": cfg.ec_k, "m": cfg.ec_m} if redundancy == "ec" else None, "chunks": chunks}

    # ---- objects -----------------------------------------------------------

    @app.post("/buckets/{bucket}/objects/{key:path}", status_code=201)
    def commit_object(bucket: str, key: str, body: ObjectIn):
        if sum(c.size for c in body.chunks) != body.size:
            raise HTTPException(400, "object size does not equal the sum of its chunk sizes")
        occurrences = Counter(c.hash for c in body.chunks)
        sizes = {c.hash: c.size for c in body.chunks}
        written: dict[str, dict[tuple, Location]] = {}
        for c in body.chunks:
            for loc in c.locations or []:
                written.setdefault(c.hash, {})[(loc.fragment, loc.node_id)] = loc

        with pool.connection() as conn, conn.transaction():
            redundancy = bucket_redundancy(conn, bucket)
            ec = redundancy == "ec"
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{bucket}/{key}",))
            new = degraded = 0
            missing = []
            for h, n in occurrences.items():
                if h in written:
                    locs = list(written[h].values())
                    acks = len({l.fragment for l in locs}) if ec else len({l.node_id for l in locs})
                    quorum = cfg.ec_write_quorum if ec else cfg.write_quorum
                    if acks < quorum:
                        raise HTTPException(400, f"chunk {h[:12]} has {acks} copies; the write quorum is {quorum}")
                    row = conn.execute(
                        """INSERT INTO chunks (hash, redundancy, size, refcount, replicas, ec_k, ec_m)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (hash, redundancy) DO UPDATE SET refcount = chunks.refcount + EXCLUDED.refcount
                           RETURNING (xmax = 0) AS inserted""",
                        (h, redundancy, sizes[h], n, None if ec else cfg.replicas,
                         cfg.ec_k if ec else None, cfg.ec_m if ec else None)).fetchone()
                    new += row["inserted"]
                    with conn.cursor() as cur:
                        cur.executemany(
                            """INSERT INTO chunk_locations (chunk_hash, redundancy, fragment, node_id, blob_hash, blob_size)
                               VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                            [(h, redundancy, l.fragment, l.node_id, l.blob_hash, l.blob_size) for l in locs])
                    if row["inserted"] and acks < (cfg.ec_k + cfg.ec_m if ec else cfg.replicas):
                        enqueue_repair(conn, h, redundancy, "written below full redundancy")
                        degraded += 1
                elif conn.execute("UPDATE chunks SET refcount = refcount + %s WHERE hash = %s AND redundancy = %s",
                                  (n, h, redundancy)).rowcount == 0:
                    missing.append(h)  # client skipped it as a duplicate, but it was deleted meanwhile
            if missing:
                raise HTTPException(409, {"message": "chunks are not stored; upload them and retry", "missing": missing})

            version = conn.execute(
                "SELECT coalesce(max(version), 0) + 1 AS v FROM objects WHERE bucket = %s AND key = %s",
                (bucket, key)).fetchone()["v"]
            obj = conn.execute(
                """INSERT INTO objects (bucket, key, version, size, chunking, tags, sha256, content_type)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id, created_at""",
                (bucket, key, version, body.size, body.chunking, Jsonb(body.tags), body.sha256,
                 body.content_type)).fetchone()
            rows, offset = [], 0
            for seq, c in enumerate(body.chunks):
                rows.append((obj["id"], seq, c.hash, redundancy, offset))
                offset += c.size
            with conn.cursor() as cur:
                cur.executemany(
                    """INSERT INTO object_chunks (object_id, seq, chunk_hash, redundancy, offset_bytes)
                       VALUES (%s, %s, %s, %s, %s)""", rows)

        return {"bucket": bucket, "key": key, "version": version, "size": body.size, "redundancy": redundancy,
                "sha256": body.sha256, "content_type": body.content_type, "tags": body.tags,
                "created_at": obj["created_at"], "chunks": len(body.chunks), "new_chunks": new,
                "deduplicated_chunks": len(body.chunks) - new, "under_replicated_chunks": degraded}

    def find_object(conn, bucket: str, key: str, version: int | None) -> dict:
        if version is None:
            row = conn.execute("SELECT * FROM objects WHERE bucket = %s AND key = %s ORDER BY version DESC LIMIT 1",
                               (bucket, key)).fetchone()
        else:
            row = conn.execute("SELECT * FROM objects WHERE bucket = %s AND key = %s AND version = %s",
                               (bucket, key, version)).fetchone()
        if row is None:
            raise HTTPException(404, f"object {bucket}/{key} not found")
        return row

    @app.get("/buckets/{bucket}/objects")
    def list_objects(bucket: str, prefix: str = "", limit: int = Query(1000, ge=1, le=10000)):
        with pool.connection() as conn:
            bucket_redundancy(conn, bucket)
            return conn.execute(
                """SELECT DISTINCT ON (key) key, version, size, chunking, tags, sha256, content_type, created_at
                   FROM objects WHERE bucket = %s AND key LIKE %s ORDER BY key, version DESC LIMIT %s""",
                (bucket, like_prefix(prefix), limit)).fetchall()

    @app.get("/search")
    def search(bucket: str | None = None, prefix: str = "", min_size: int | None = None,
               max_size: int | None = None, after: datetime | None = None, before: datetime | None = None,
               tag: Annotated[list[str], Query()] = [], limit: int = Query(100, ge=1, le=10000)):
        """Latest version of every object matching all the given filters. tag=key=value, repeatable."""
        try:
            tags = dict(t.split("=", 1) for t in tag)
        except ValueError:
            raise HTTPException(400, "tag filters look like tag=type=video")
        with pool.connection() as conn:
            rows = conn.execute(
                """SELECT * FROM (
                       SELECT DISTINCT ON (bucket, key) bucket, key, version, size, chunking, tags, sha256,
                              content_type, created_at
                       FROM objects ORDER BY bucket, key, version DESC) o
                   WHERE (%(bucket)s::text IS NULL OR bucket = %(bucket)s)
                     AND key LIKE %(prefix)s
                     AND (%(min)s::bigint IS NULL OR size >= %(min)s)
                     AND (%(max)s::bigint IS NULL OR size <= %(max)s)
                     AND (%(after)s::timestamptz IS NULL OR created_at >= %(after)s)
                     AND (%(before)s::timestamptz IS NULL OR created_at < %(before)s)
                     AND tags @> %(tags)s
                   ORDER BY bucket, key LIMIT %(limit)s""",
                {"bucket": bucket, "prefix": like_prefix(prefix), "min": min_size, "max": max_size,
                 "after": after, "before": before, "tags": Jsonb(tags), "limit": limit}).fetchall()
        return {"count": len(rows), "results": rows}

    @app.get("/buckets/{bucket}/objects/{key:path}")
    def get_object(bucket: str, key: str, version: int | None = None):
        """Object metadata with its ordered chunk list and every copy/fragment of each chunk."""
        with pool.connection() as conn:
            redundancy = bucket_redundancy(conn, bucket)
            obj = find_object(conn, bucket, key, version)
            chunks = conn.execute(
                """SELECT oc.seq, oc.chunk_hash AS hash, c.size, oc.offset_bytes AS offset, c.refcount,
                          c.redundancy, c.replicas, c.ec_k, c.ec_m,
                          coalesce(json_agg(json_build_object(
                              'node_id', l.node_id, 'fragment', l.fragment, 'blob_hash', l.blob_hash,
                              'url', n.url, 'public_url', n.public_url,
                              'alive', n.status = 'alive' AND n.last_heartbeat > now() - make_interval(secs => %s),
                              'bad', EXISTS (SELECT 1 FROM bad_copies b
                                             WHERE b.chunk_hash = l.chunk_hash AND b.redundancy = l.redundancy
                                               AND b.fragment = l.fragment AND b.node_id = l.node_id))
                            ORDER BY l.fragment, l.node_id) FILTER (WHERE l.node_id IS NOT NULL), '[]') AS locations
                   FROM object_chunks oc
                   JOIN chunks c ON c.hash = oc.chunk_hash AND c.redundancy = oc.redundancy
                   LEFT JOIN chunk_locations l ON l.chunk_hash = oc.chunk_hash AND l.redundancy = oc.redundancy
                   LEFT JOIN nodes n ON n.node_id = l.node_id
                   WHERE oc.object_id = %s
                   GROUP BY oc.seq, oc.chunk_hash, c.size, oc.offset_bytes, c.refcount,
                            c.redundancy, c.replicas, c.ec_k, c.ec_m
                   ORDER BY oc.seq""", (cfg.node_timeout, obj["id"])).fetchall()
        obj.pop("id")
        return {**obj, "redundancy": redundancy, "read_quorum": cfg.read_quorum, "chunks": chunks}

    @app.delete("/buckets/{bucket}/objects/{key:path}")
    def delete_object(bucket: str, key: str, version: int | None = None):
        """Delete an object (all versions, or one). Chunks no other object uses are garbage-collected."""
        with pool.connection() as conn:
            with conn.transaction():
                conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{bucket}/{key}",))
                if version is None:
                    ids = [r["id"] for r in conn.execute(
                        "SELECT id FROM objects WHERE bucket = %s AND key = %s", (bucket, key))]
                else:
                    ids = [r["id"] for r in conn.execute(
                        "SELECT id FROM objects WHERE bucket = %s AND key = %s AND version = %s",
                        (bucket, key, version))]
                if not ids:
                    raise HTTPException(404, f"object {bucket}/{key} not found")
                refs = conn.execute(
                    """SELECT chunk_hash, redundancy, count(*) AS n FROM object_chunks
                       WHERE object_id = ANY(%s) GROUP BY chunk_hash, redundancy""", (ids,)).fetchall()
                conn.execute("DELETE FROM objects WHERE id = ANY(%s)", (ids,))
                freed_h, freed_r = [], []
                for r in refs:
                    left = conn.execute(
                        "UPDATE chunks SET refcount = refcount - %s WHERE hash = %s AND redundancy = %s RETURNING refcount",
                        (r["n"], r["chunk_hash"], r["redundancy"])).fetchone()["refcount"]
                    if left == 0:
                        freed_h.append(r["chunk_hash"])
                        freed_r.append(r["redundancy"])
                freed = "(SELECT * FROM unnest(%s::text[], %s::text[]))"
                garbage = conn.execute(
                    f"""SELECT l.blob_hash, l.node_id, n.url, n.status FROM chunk_locations l
                        JOIN nodes n ON n.node_id = l.node_id
                        WHERE (l.chunk_hash, l.redundancy) IN {freed}""", (freed_h, freed_r)).fetchall()
                conn.execute(f"DELETE FROM chunks WHERE (hash, redundancy) IN {freed} AND refcount = 0",
                             (freed_h, freed_r))
                conn.execute(f"DELETE FROM bad_copies WHERE (chunk_hash, redundancy) IN {freed}", (freed_h, freed_r))
                # A blob can be shared, e.g. identical fragments of different chunks: keep those still referenced.
                still_used = {(r["blob_hash"], r["node_id"]) for r in conn.execute(
                    "SELECT blob_hash, node_id FROM chunk_locations WHERE blob_hash = ANY(%s)",
                    ([g["blob_hash"] for g in garbage],))}

        # Remove the unreferenced blobs from the nodes after the commit (best effort: blobs on failed
        # nodes stay as orphans until those nodes are cleaned).
        removed = 0
        with httpx.Client(timeout=10) as client:
            for g in garbage:
                if g["status"] != "alive" or (g["blob_hash"], g["node_id"]) in still_used:
                    continue
                try:
                    removed += client.delete(f"{g['url']}/chunks/{g['blob_hash']}").status_code in (204, 404)
                except httpx.HTTPError as e:
                    log.warning("could not delete blob %s from %s: %r", g["blob_hash"], g["node_id"], e)
        return {"bucket": bucket, "key": key, "deleted_versions": len(ids),
                "freed_chunks": len(freed_h), "removed_copies": removed}

    # ---- integrity: reports from readers, scrub requests ------------------

    @app.post("/report")
    def report(bad: BadCopy):
        """A reader found a copy that is missing or does not match its hash."""
        with pool.connection() as conn:
            report_bad_copy(conn, bad.chunk_hash, bad.redundancy, bad.fragment, bad.node_id, bad.reason)
        log.warning("bad copy reported: chunk %s fragment %d on %s (%s)",
                    bad.chunk_hash[:12], bad.fragment, bad.node_id, bad.reason)
        return {"queued": True}

    @app.post("/scrub")
    def scrub():
        """Ask the repair worker to scrub every alive node now."""
        with pool.connection() as conn:
            nodes = sorted(alive_nodes(conn, cfg.node_timeout))
            for node in nodes:
                conn.execute("""INSERT INTO repair_jobs (kind, node_id, reason) VALUES ('scrub', %s, 'requested')
                                ON CONFLICT (node_id) WHERE kind = 'scrub' AND status IN ('pending', 'running')
                                DO NOTHING""", (node,))
        return {"scrub_queued_for": nodes}

    # ---- statistics and health ---------------------------------------------

    @app.get("/stats")
    def stats(bucket: str | None = None):
        """Logical size (uploaded) vs unique size (after dedup) vs stored size (all copies or fragments)."""
        with pool.connection() as conn:
            row = conn.execute(
                """WITH o AS (SELECT * FROM objects WHERE %(b)s::text IS NULL OR bucket = %(b)s),
                        u AS (SELECT DISTINCT oc.chunk_hash, oc.redundancy
                              FROM object_chunks oc JOIN o ON o.id = oc.object_id)
                   SELECT (SELECT count(*) FROM (SELECT DISTINCT bucket, key FROM o) k) AS objects,
                          (SELECT count(*) FROM o) AS versions,
                          (SELECT coalesce(sum(size), 0)::bigint FROM o) AS logical_bytes,
                          (SELECT count(*) FROM u) AS unique_chunks,
                          (SELECT coalesce(sum(c.size), 0)::bigint FROM u
                              JOIN chunks c ON c.hash = u.chunk_hash AND c.redundancy = u.redundancy) AS unique_bytes,
                          (SELECT coalesce(sum(l.blob_size), 0)::bigint FROM u
                              JOIN chunk_locations l ON l.chunk_hash = u.chunk_hash AND l.redundancy = u.redundancy)
                              AS stored_bytes,
                          (SELECT count(*) FROM u
                              JOIN chunk_locations l ON l.chunk_hash = u.chunk_hash AND l.redundancy = u.redundancy)
                              AS stored_blobs""",
                {"b": bucket}).fetchone()
        logical, unique, stored = row["logical_bytes"], row["unique_bytes"], row["stored_bytes"]
        row["dedup_ratio"] = round(logical / unique, 3) if unique else None
        row["dedup_saving"] = round(1 - unique / logical, 4) if logical else 0.0
        row["storage_overhead"] = round(stored / unique, 3) if unique else None
        return {"bucket": bucket, **row}

    @app.get("/cluster")
    def cluster():
        """Cluster health: nodes, chunk and object states, repair queue."""
        with pool.connection() as conn:
            nodes = list_nodes()
            chunk_rows = conn.execute(
                f"SELECT redundancy, state, count(*) AS n FROM ({CHUNK_STATES}) s GROUP BY redundancy, state").fetchall()
            objects = conn.execute(
                f"""WITH s AS ({CHUNK_STATES})
                    SELECT (SELECT count(*) FROM objects) AS total,
                           count(*) FILTER (WHERE worst = 1) AS degraded,
                           count(*) FILTER (WHERE worst = 2) AS lost
                    FROM (SELECT oc.object_id,
                                 max(CASE s.state WHEN 'lost' THEN 2 WHEN 'degraded' THEN 1 ELSE 0 END) AS worst
                          FROM object_chunks oc JOIN s ON s.hash = oc.chunk_hash AND s.redundancy = oc.redundancy
                          GROUP BY oc.object_id) x""").fetchone()
            jobs = conn.execute("SELECT kind, status, count(*) AS n FROM repair_jobs GROUP BY kind, status").fetchall()
        chunks = {"healthy": 0, "degraded": 0, "lost": 0}
        by_redundancy: dict[str, dict] = {}
        for r in chunk_rows:
            chunks[r["state"]] += r["n"]
            by_redundancy.setdefault(r["redundancy"], {"healthy": 0, "degraded": 0, "lost": 0})[r["state"]] = r["n"]
        queue = {kind: {s: 0 for s in ("pending", "running", "done", "failed")} for kind in ("repair", "scrub")}
        for j in jobs:
            queue[j["kind"]][j["status"]] = j["n"]
        return {
            "nodes": {"total": len(nodes), "alive": sum(n["status"] == "alive" for n in nodes),
                      "failed": [n["node_id"] for n in nodes if n["status"] == "failed"], "list": nodes},
            "chunks": {**chunks, "by_redundancy": by_redundancy},
            "objects": objects, "repair_jobs": queue["repair"], "scrub_jobs": queue["scrub"],
            "settings": {"N": cfg.replicas, "W": cfg.write_quorum, "R": cfg.read_quorum,
                         "ec": f"{cfg.ec_k}+{cfg.ec_m}", "node_timeout_s": cfg.node_timeout},
        }

    @app.get("/repair/stats")
    def repair_stats(bucket: str | None = None, since_id: int = 0):
        """Repair work and traffic per redundancy scheme (optionally for one bucket, or jobs after since_id)."""
        with pool.connection() as conn:
            rows = conn.execute(
                """SELECT j.redundancy, count(*) AS jobs,
                          count(*) FILTER (WHERE status = 'pending') AS pending,
                          count(*) FILTER (WHERE status = 'running') AS running,
                          count(*) FILTER (WHERE status = 'done') AS done,
                          count(*) FILTER (WHERE status = 'failed') AS failed,
                          count(*) FILTER (WHERE bytes_written > 0) AS rebuilt,
                          coalesce(sum(bytes_read), 0)::bigint AS bytes_read,
                          coalesce(sum(bytes_written), 0)::bigint AS bytes_written,
                          EXTRACT(EPOCH FROM max(finished_at) FILTER (WHERE bytes_written > 0)
                                           - min(started_at) FILTER (WHERE bytes_written > 0))::float AS seconds
                   FROM repair_jobs j
                   WHERE kind = 'repair' AND id > %(since)s
                     AND (%(b)s::text IS NULL OR EXISTS (
                         SELECT 1 FROM object_chunks oc JOIN objects o ON o.id = oc.object_id
                         WHERE oc.chunk_hash = j.chunk_hash AND oc.redundancy = j.redundancy AND o.bucket = %(b)s))
                   GROUP BY j.redundancy ORDER BY j.redundancy""", {"b": bucket, "since": since_id}).fetchall()
            last = conn.execute("SELECT coalesce(max(id), 0) AS id FROM repair_jobs").fetchone()["id"]
        return {"bucket": bucket, "since_id": since_id, "last_job_id": last, "by_redundancy": rows}

    return app
