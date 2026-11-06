"""Metadata service: buckets, objects, chunk reference counts, node registry and placement.

The service never touches chunk data on the write path. Clients ask it where to
put new chunks (/plan), write the chunks to the storage nodes themselves, and
then commit the object, which records the chunk list and updates reference counts.
"""

import logging
import os
from collections import Counter
from contextlib import asynccontextmanager
from typing import Annotated

import httpx
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi import Path as PathParam
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from common.ring import HashRing

log = logging.getLogger("metadata")

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    node_id        TEXT PRIMARY KEY,
    url            TEXT NOT NULL,
    public_url     TEXT,
    free_bytes     BIGINT NOT NULL DEFAULT 0,
    chunk_count    BIGINT NOT NULL DEFAULT 0,
    first_seen     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS buckets (
    name       TEXT PRIMARY KEY,
    redundancy TEXT NOT NULL DEFAULT 'replication' CHECK (redundancy IN ('replication', 'ec')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS objects (
    id         BIGSERIAL PRIMARY KEY,
    bucket     TEXT NOT NULL REFERENCES buckets(name),
    key        TEXT NOT NULL,
    version    INT NOT NULL,
    size       BIGINT NOT NULL,
    chunking   TEXT NOT NULL,
    tags       JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (bucket, key, version)
);
CREATE TABLE IF NOT EXISTS chunks (
    hash       TEXT PRIMARY KEY,
    size       INT NOT NULL,
    refcount   INT NOT NULL CHECK (refcount >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS object_chunks (
    object_id    BIGINT NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    seq          INT NOT NULL,
    chunk_hash   TEXT NOT NULL REFERENCES chunks(hash),
    offset_bytes BIGINT NOT NULL,
    PRIMARY KEY (object_id, seq)
);
CREATE INDEX IF NOT EXISTS object_chunks_hash ON object_chunks (chunk_hash);
CREATE TABLE IF NOT EXISTS chunk_locations (
    chunk_hash TEXT NOT NULL REFERENCES chunks(hash) ON DELETE CASCADE,
    node_id    TEXT NOT NULL,
    PRIMARY KEY (chunk_hash, node_id)
);
CREATE INDEX IF NOT EXISTS chunk_locations_node ON chunk_locations (node_id);
"""

BucketName = Annotated[str, PathParam(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")]
ChunkHash = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class Heartbeat(BaseModel):
    node_id: str
    url: str
    public_url: str | None = None
    free_bytes: int
    chunk_count: int


class BucketIn(BaseModel):
    redundancy: str = Field("replication", pattern="^(replication|ec)$")


class PlanIn(BaseModel):
    hashes: list[ChunkHash]


class ChunkRef(BaseModel):
    hash: ChunkHash
    size: int
    nodes: list[str] | None = None  # nodes the client just wrote it to; None = already stored (deduplicated)


class ObjectIn(BaseModel):
    size: int
    chunking: str = "fixed"
    tags: dict[str, str] = {}
    chunks: list[ChunkRef]


def like_prefix(prefix: str) -> str:
    return prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


def create_app() -> FastAPI:
    logging.basicConfig(level=logging.INFO)
    replicas = int(os.environ.get("REPLICAS", "3"))
    vnodes = int(os.environ.get("RING_VNODES", "128"))
    node_timeout = float(os.environ.get("NODE_TIMEOUT", "15"))
    pool = ConnectionPool(
        os.environ.get("DATABASE_URL", "postgresql://dos:dos@localhost:5432/dos"),
        min_size=2, max_size=20, open=False, kwargs={"row_factory": dict_row},
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool.open(wait=True, timeout=60)
        with pool.connection() as conn:
            conn.execute(SCHEMA)
        yield
        pool.close()

    app = FastAPI(title="Metadata service", lifespan=lifespan)

    def alive_nodes(conn) -> dict[str, dict]:
        rows = conn.execute(
            "SELECT node_id, url, public_url FROM nodes"
            " WHERE last_heartbeat > now() - make_interval(secs => %s)", (node_timeout,))
        return {r["node_id"]: r for r in rows}

    def bucket_exists(conn, bucket: str) -> None:
        if not conn.execute("SELECT 1 FROM buckets WHERE name = %s", (bucket,)).fetchone():
            raise HTTPException(404, f"bucket {bucket!r} does not exist")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    # ---- nodes -------------------------------------------------------------

    @app.post("/heartbeat")
    def heartbeat(hb: Heartbeat):
        with pool.connection() as conn:
            conn.execute(
                """INSERT INTO nodes (node_id, url, public_url, free_bytes, chunk_count, last_heartbeat)
                   VALUES (%s, %s, %s, %s, %s, now())
                   ON CONFLICT (node_id) DO UPDATE SET
                     url = EXCLUDED.url, public_url = EXCLUDED.public_url, free_bytes = EXCLUDED.free_bytes,
                     chunk_count = EXCLUDED.chunk_count, last_heartbeat = now()""",
                (hb.node_id, hb.url, hb.public_url, hb.free_bytes, hb.chunk_count))
        return {"ok": True}

    @app.get("/nodes")
    def list_nodes():
        with pool.connection() as conn:
            rows = conn.execute(
                """SELECT n.node_id, n.url, n.public_url, n.free_bytes, n.chunk_count, n.last_heartbeat,
                          EXTRACT(EPOCH FROM now() - n.last_heartbeat)::float AS seconds_since_heartbeat,
                          (SELECT count(*) FROM chunk_locations l WHERE l.node_id = n.node_id) AS assigned_chunks
                   FROM nodes n ORDER BY n.node_id""").fetchall()
        for r in rows:
            r["status"] = "alive" if r["seconds_since_heartbeat"] <= node_timeout else "down"
        return rows

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
        """For each chunk hash: is it already stored (dedup), and if not, which nodes should hold it."""
        hashes = list(dict.fromkeys(body.hashes))
        with pool.connection() as conn:
            existing = {r["hash"] for r in conn.execute("SELECT hash FROM chunks WHERE hash = ANY(%s)", (hashes,))}
            nodes = alive_nodes(conn)
        if not nodes:
            raise HTTPException(503, "no storage nodes are alive")
        ring = HashRing(nodes, vnodes)
        chunks = {}
        for h in hashes:
            if h in existing:
                chunks[h] = {"exists": True, "nodes": []}
            else:
                chunks[h] = {"exists": False, "nodes": [nodes[n] for n in ring.nodes_for(h, replicas)]}
        return {"replicas": min(replicas, len(nodes)), "chunks": chunks}

    # ---- objects -----------------------------------------------------------

    @app.post("/buckets/{bucket}/objects/{key:path}", status_code=201)
    def commit_object(bucket: str, key: str, body: ObjectIn):
        if sum(c.size for c in body.chunks) != body.size:
            raise HTTPException(400, "object size does not equal the sum of its chunk sizes")
        occurrences = Counter(c.hash for c in body.chunks)
        sizes = {c.hash: c.size for c in body.chunks}
        locations: dict[str, set[str]] = {}
        for c in body.chunks:
            if c.nodes:
                locations.setdefault(c.hash, set()).update(c.nodes)

        with pool.connection() as conn, conn.transaction():
            bucket_exists(conn, bucket)
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{bucket}/{key}",))
            new, missing = 0, []
            for h, n in occurrences.items():
                if h in locations:
                    row = conn.execute(
                        """INSERT INTO chunks (hash, size, refcount) VALUES (%s, %s, %s)
                           ON CONFLICT (hash) DO UPDATE SET refcount = chunks.refcount + EXCLUDED.refcount
                           RETURNING (xmax = 0) AS inserted""", (h, sizes[h], n)).fetchone()
                    new += row["inserted"]
                    with conn.cursor() as cur:
                        cur.executemany(
                            "INSERT INTO chunk_locations (chunk_hash, node_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            [(h, node) for node in locations[h]])
                elif conn.execute("UPDATE chunks SET refcount = refcount + %s WHERE hash = %s", (n, h)).rowcount == 0:
                    missing.append(h)  # client skipped it as a duplicate, but it was deleted meanwhile
            if missing:
                raise HTTPException(409, {"message": "chunks are not stored; upload them and retry", "missing": missing})

            version = conn.execute(
                "SELECT coalesce(max(version), 0) + 1 AS v FROM objects WHERE bucket = %s AND key = %s",
                (bucket, key)).fetchone()["v"]
            obj = conn.execute(
                """INSERT INTO objects (bucket, key, version, size, chunking, tags)
                   VALUES (%s, %s, %s, %s, %s, %s) RETURNING id, created_at""",
                (bucket, key, version, body.size, body.chunking, Jsonb(body.tags))).fetchone()
            rows, offset = [], 0
            for seq, c in enumerate(body.chunks):
                rows.append((obj["id"], seq, c.hash, offset))
                offset += c.size
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO object_chunks (object_id, seq, chunk_hash, offset_bytes) VALUES (%s, %s, %s, %s)", rows)

        return {"bucket": bucket, "key": key, "version": version, "size": body.size,
                "created_at": obj["created_at"], "chunks": len(body.chunks),
                "new_chunks": new, "deduplicated_chunks": len(body.chunks) - new}

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
            bucket_exists(conn, bucket)
            return conn.execute(
                """SELECT DISTINCT ON (key) key, version, size, chunking, tags, created_at
                   FROM objects WHERE bucket = %s AND key LIKE %s ORDER BY key, version DESC LIMIT %s""",
                (bucket, like_prefix(prefix), limit)).fetchall()

    @app.get("/buckets/{bucket}/objects/{key:path}")
    def get_object(bucket: str, key: str, version: int | None = None):
        """Object metadata, with its ordered chunk list and where each chunk is stored."""
        with pool.connection() as conn:
            obj = find_object(conn, bucket, key, version)
            chunks = conn.execute(
                """SELECT oc.seq, oc.chunk_hash AS hash, c.size, oc.offset_bytes AS offset, c.refcount,
                          coalesce(json_agg(json_build_object(
                              'node_id', l.node_id, 'url', n.url, 'public_url', n.public_url,
                              'alive', n.last_heartbeat > now() - make_interval(secs => %s))
                            ORDER BY l.node_id) FILTER (WHERE l.node_id IS NOT NULL), '[]') AS locations
                   FROM object_chunks oc
                   JOIN chunks c ON c.hash = oc.chunk_hash
                   LEFT JOIN chunk_locations l ON l.chunk_hash = oc.chunk_hash
                   LEFT JOIN nodes n ON n.node_id = l.node_id
                   WHERE oc.object_id = %s
                   GROUP BY oc.seq, oc.chunk_hash, c.size, oc.offset_bytes, c.refcount
                   ORDER BY oc.seq""", (node_timeout, obj["id"])).fetchall()
        obj.pop("id")
        return {**obj, "chunks": chunks}

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
                    "SELECT chunk_hash, count(*) AS n FROM object_chunks WHERE object_id = ANY(%s) GROUP BY chunk_hash",
                    (ids,)).fetchall()
                conn.execute("DELETE FROM objects WHERE id = ANY(%s)", (ids,))
                freed = []
                for r in refs:
                    left = conn.execute("UPDATE chunks SET refcount = refcount - %s WHERE hash = %s RETURNING refcount",
                                        (r["n"], r["chunk_hash"])).fetchone()["refcount"]
                    if left == 0:
                        freed.append(r["chunk_hash"])
                garbage = conn.execute(
                    """SELECT l.chunk_hash, n.url FROM chunk_locations l JOIN nodes n ON n.node_id = l.node_id
                       WHERE l.chunk_hash = ANY(%s)""", (freed,)).fetchall()
                conn.execute("DELETE FROM chunks WHERE hash = ANY(%s) AND refcount = 0", (freed,))

        # Remove the unreferenced copies from the nodes after the commit. Best effort for now;
        # Phase 4 moves garbage collection into the repair worker.
        removed = 0
        with httpx.Client(timeout=10) as client:
            for g in garbage:
                try:
                    removed += client.delete(f"{g['url']}/chunks/{g['chunk_hash']}").status_code in (204, 404)
                except httpx.HTTPError as e:
                    log.warning("could not delete chunk %s from %s: %r", g["chunk_hash"], g["url"], e)
        return {"bucket": bucket, "key": key, "deleted_versions": len(ids),
                "freed_chunks": len(freed), "removed_copies": removed}

    # ---- statistics --------------------------------------------------------

    @app.get("/stats")
    def stats(bucket: str | None = None):
        """Logical size (what users uploaded) vs unique size (after dedup) vs stored size (with replicas)."""
        with pool.connection() as conn:
            row = conn.execute(
                """WITH o AS (SELECT * FROM objects WHERE %(b)s::text IS NULL OR bucket = %(b)s),
                        u AS (SELECT DISTINCT oc.chunk_hash FROM object_chunks oc JOIN o ON o.id = oc.object_id)
                   SELECT (SELECT count(*) FROM (SELECT DISTINCT bucket, key FROM o) k) AS objects,
                          (SELECT count(*) FROM o) AS versions,
                          (SELECT coalesce(sum(size), 0)::bigint FROM o) AS logical_bytes,
                          (SELECT count(*) FROM u) AS unique_chunks,
                          (SELECT coalesce(sum(c.size), 0)::bigint FROM u JOIN chunks c ON c.hash = u.chunk_hash)
                              AS unique_bytes,
                          (SELECT coalesce(sum(c.size), 0)::bigint FROM u JOIN chunks c ON c.hash = u.chunk_hash
                              JOIN chunk_locations l ON l.chunk_hash = u.chunk_hash) AS stored_bytes""",
                {"b": bucket}).fetchone()
        logical, unique = row["logical_bytes"], row["unique_bytes"]
        row["dedup_ratio"] = round(logical / unique, 3) if unique else None
        row["dedup_saving"] = round(1 - unique / logical, 4) if logical else 0.0
        return {"bucket": bucket, **row}

    return app
