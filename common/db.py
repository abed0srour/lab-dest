"""Database schema and queries shared by the metadata service and the repair worker."""

from psycopg.types.json import Jsonb
from psycopg_pool.base import AttemptWithBackoff

# Reconnect to the database every second instead of after growing gaps (1, 2, 4, 8, 16 s ...): after a
# PostgreSQL failover the services then reach the new primary about a second after its promotion.
AttemptWithBackoff.DELAY_BACKOFF = 1.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    node_id        TEXT PRIMARY KEY,
    url            TEXT NOT NULL,
    public_url     TEXT,
    free_bytes     BIGINT NOT NULL DEFAULT 0,
    chunk_count    BIGINT NOT NULL DEFAULT 0,
    status         TEXT NOT NULL DEFAULT 'alive' CHECK (status IN ('alive', 'failed')),
    first_seen     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
    failed_at      TIMESTAMPTZ
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
-- Phase 5: whole-object SHA-256 (served as the ETag) and the content type given at upload.
ALTER TABLE objects ADD COLUMN IF NOT EXISTS sha256 TEXT;
ALTER TABLE objects ADD COLUMN IF NOT EXISTS content_type TEXT;
CREATE INDEX IF NOT EXISTS objects_tags ON objects USING gin (tags);
CREATE INDEX IF NOT EXISTS objects_created ON objects (created_at);
-- A chunk is identified by its hash *and* the redundancy scheme of the bucket that stores it,
-- so a replicated bucket and an erasure-coded bucket never share (and weaken) each other's copies.
CREATE TABLE IF NOT EXISTS chunks (
    hash       TEXT NOT NULL,
    redundancy TEXT NOT NULL CHECK (redundancy IN ('replication', 'ec')),
    size       INT NOT NULL,
    refcount   INT NOT NULL CHECK (refcount >= 0),
    replicas   INT,            -- replication: copies wanted
    ec_k       INT,            -- erasure coding: data fragments
    ec_m       INT,            -- erasure coding: parity fragments
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (hash, redundancy)
);
CREATE TABLE IF NOT EXISTS object_chunks (
    object_id    BIGINT NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    seq          INT NOT NULL,
    chunk_hash   TEXT NOT NULL,
    redundancy   TEXT NOT NULL,
    offset_bytes BIGINT NOT NULL,
    PRIMARY KEY (object_id, seq),
    FOREIGN KEY (chunk_hash, redundancy) REFERENCES chunks(hash, redundancy)
);
CREATE INDEX IF NOT EXISTS object_chunks_chunk ON object_chunks (chunk_hash, redundancy);
CREATE TABLE IF NOT EXISTS chunk_locations (
    chunk_hash TEXT NOT NULL,
    redundancy TEXT NOT NULL,
    fragment   INT NOT NULL,   -- -1 = full replica, 0..k+m-1 = erasure-coded fragment
    node_id    TEXT NOT NULL,
    blob_hash  TEXT NOT NULL,  -- SHA-256 of the bytes the node stores (= chunk hash for a replica)
    blob_size  INT NOT NULL,
    PRIMARY KEY (chunk_hash, redundancy, fragment, node_id),
    FOREIGN KEY (chunk_hash, redundancy) REFERENCES chunks(hash, redundancy) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS chunk_locations_node ON chunk_locations (node_id);
CREATE INDEX IF NOT EXISTS chunk_locations_blob ON chunk_locations (blob_hash);
-- Copies found corrupted or missing (by the scrubber or a read) and not repaired yet.
CREATE TABLE IF NOT EXISTS bad_copies (
    chunk_hash  TEXT NOT NULL,
    redundancy  TEXT NOT NULL,
    fragment    INT NOT NULL,
    node_id     TEXT NOT NULL,
    reason      TEXT NOT NULL,
    reported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chunk_hash, redundancy, fragment, node_id)
);
-- Work queue for the repair worker(s). Workers claim jobs with FOR UPDATE SKIP LOCKED.
CREATE TABLE IF NOT EXISTS repair_jobs (
    id            BIGSERIAL PRIMARY KEY,
    kind          TEXT NOT NULL CHECK (kind IN ('repair', 'scrub')),
    chunk_hash    TEXT,          -- repair: the chunk to bring back to full redundancy
    redundancy    TEXT,
    node_id       TEXT,          -- scrub: the node to scrub; repair: the node that triggered it
    reason        TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'done', 'failed')),
    attempts      INT NOT NULL DEFAULT 0,
    run_after     TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    bytes_read    BIGINT NOT NULL DEFAULT 0,
    bytes_written BIGINT NOT NULL DEFAULT 0,
    result        TEXT
);
CREATE INDEX IF NOT EXISTS repair_jobs_pending ON repair_jobs (id) WHERE status = 'pending';
-- Phase 7: jobs are handed to RabbitMQ ('queued'), there are rebalance and garbage-collection jobs,
-- and a job can carry a payload (e.g. the blobs a gc job must delete).
ALTER TABLE repair_jobs ADD COLUMN IF NOT EXISTS queued_at TIMESTAMPTZ;
ALTER TABLE repair_jobs ADD COLUMN IF NOT EXISTS detail JSONB;
ALTER TABLE repair_jobs DROP CONSTRAINT IF EXISTS repair_jobs_status_check;
ALTER TABLE repair_jobs ADD CONSTRAINT repair_jobs_status_check
    CHECK (status IN ('pending', 'queued', 'running', 'done', 'failed'));
ALTER TABLE repair_jobs DROP CONSTRAINT IF EXISTS repair_jobs_kind_check;
ALTER TABLE repair_jobs ADD CONSTRAINT repair_jobs_kind_check CHECK (kind IN ('repair', 'scrub', 'rebalance', 'gc'));
DROP INDEX IF EXISTS one_pending_repair_per_chunk;
DROP INDEX IF EXISTS one_scrub_per_node;
CREATE UNIQUE INDEX IF NOT EXISTS one_waiting_job_per_chunk ON repair_jobs (kind, chunk_hash, redundancy)
    WHERE chunk_hash IS NOT NULL AND status IN ('pending', 'queued');
CREATE UNIQUE INDEX IF NOT EXISTS one_job_per_node ON repair_jobs (kind, node_id)
    WHERE chunk_hash IS NULL AND node_id IS NOT NULL AND status IN ('pending', 'queued', 'running');
CREATE UNIQUE INDEX IF NOT EXISTS one_waiting_rebalance_plan ON repair_jobs (kind)
    WHERE kind = 'rebalance' AND chunk_hash IS NULL AND status IN ('pending', 'queued');
"""

# ON CONFLICT targets matching the partial unique indexes above (duplicate jobs are skipped).
CHUNK_JOB_CONFLICT = ("ON CONFLICT (kind, chunk_hash, redundancy) "
                      "WHERE chunk_hash IS NOT NULL AND status IN ('pending', 'queued') DO NOTHING")
NODE_JOB_CONFLICT = ("ON CONFLICT (kind, node_id) WHERE chunk_hash IS NULL AND node_id IS NOT NULL "
                     "AND status IN ('pending', 'queued', 'running') DO NOTHING")
PLAN_JOB_CONFLICT = ("ON CONFLICT (kind) WHERE kind = 'rebalance' AND chunk_hash IS NULL "
                     "AND status IN ('pending', 'queued') DO NOTHING")
WAITING = "('pending', 'queued')"  # job statuses that mean "not started yet"

# One row per chunk: how many healthy copies/fragments it has (on alive nodes, not reported bad),
# how many it should have, how many it needs to be readable, and the resulting state.
CHUNK_STATES = """
SELECT c.hash, c.redundancy, p.have, p.want, p.need,
       CASE WHEN p.have < p.need THEN 'lost' WHEN p.have < p.want THEN 'degraded' ELSE 'healthy' END AS state
FROM chunks c
CROSS JOIN LATERAL (
    SELECT CASE WHEN c.redundancy = 'replication' THEN count(DISTINCT l.node_id)
                ELSE count(DISTINCT l.fragment) END AS have,
           CASE WHEN c.redundancy = 'replication' THEN c.replicas ELSE c.ec_k + c.ec_m END AS want,
           CASE WHEN c.redundancy = 'replication' THEN 1 ELSE c.ec_k END AS need
    FROM chunk_locations l
    JOIN nodes n ON n.node_id = l.node_id AND n.status = 'alive'
    WHERE l.chunk_hash = c.hash AND l.redundancy = c.redundancy
      AND NOT EXISTS (SELECT 1 FROM bad_copies b
                      WHERE b.chunk_hash = l.chunk_hash AND b.redundancy = l.redundancy
                        AND b.fragment = l.fragment AND b.node_id = l.node_id)
) p
"""


def enqueue_repair(conn, chunk_hash: str, redundancy: str, reason: str, node_id: str | None = None,
                   kind: str = "repair") -> bool:
    """Queue a repair (or rebalance) job for one chunk unless one is already waiting. True if queued."""
    return conn.execute(
        f"""INSERT INTO repair_jobs (kind, chunk_hash, redundancy, node_id, reason) VALUES (%s, %s, %s, %s, %s)
            {CHUNK_JOB_CONFLICT}""", (kind, chunk_hash, redundancy, node_id, reason)).rowcount == 1


def enqueue_node_job(conn, kind: str, node_id: str, reason: str, delay: float = 0) -> bool:
    """A scrub or orphan-collection job for one node (run in `delay` seconds), unless one is
    already waiting or running."""
    return conn.execute(
        f"""INSERT INTO repair_jobs (kind, node_id, reason, run_after)
            VALUES (%s, %s, %s, now() + make_interval(secs => %s)) {NODE_JOB_CONFLICT}""",
        (kind, node_id, reason, delay)).rowcount == 1


def enqueue_rebalance_plan(conn, reason: str) -> bool:
    """A job that compares every chunk's placement with the hash ring and queues the moves needed."""
    return conn.execute(f"""INSERT INTO repair_jobs (kind, reason) VALUES ('rebalance', %s) {PLAN_JOB_CONFLICT}""",
                        (reason,)).rowcount == 1


def enqueue_blob_deletes(conn, blobs: list[tuple[str, str]], reason: str) -> None:
    """A garbage-collection job that deletes these (blob_hash, node_id) copies, if still unreferenced."""
    if blobs:
        conn.execute("INSERT INTO repair_jobs (kind, reason, detail) VALUES ('gc', %s, %s)",
                     (reason, Jsonb({"blobs": [list(b) for b in blobs]})))


def report_bad_copy(conn, chunk_hash: str, redundancy: str, fragment: int, node_id: str, reason: str) -> None:
    """Record a corrupted or missing copy and queue its chunk for repair."""
    conn.execute(
        """INSERT INTO bad_copies (chunk_hash, redundancy, fragment, node_id, reason) VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT DO NOTHING""", (chunk_hash, redundancy, fragment, node_id, reason))
    enqueue_repair(conn, chunk_hash, redundancy, reason, node_id)


def alive_nodes(conn, timeout: float) -> dict[str, dict]:
    rows = conn.execute(
        """SELECT node_id, url, public_url FROM nodes
           WHERE status = 'alive' AND last_heartbeat > now() - make_interval(secs => %s)""", (timeout,))
    return {r["node_id"]: r for r in rows}
