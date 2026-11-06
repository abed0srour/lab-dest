"""Database schema and queries shared by the metadata service and the repair worker."""

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
CREATE UNIQUE INDEX IF NOT EXISTS one_pending_repair_per_chunk ON repair_jobs (chunk_hash, redundancy)
    WHERE kind = 'repair' AND status = 'pending';
CREATE UNIQUE INDEX IF NOT EXISTS one_scrub_per_node ON repair_jobs (node_id)
    WHERE kind = 'scrub' AND status IN ('pending', 'running');
CREATE INDEX IF NOT EXISTS repair_jobs_pending ON repair_jobs (id) WHERE status = 'pending';
"""

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


def enqueue_repair(conn, chunk_hash: str, redundancy: str, reason: str, node_id: str | None = None) -> bool:
    """Queue a repair job unless one is already pending for this chunk. True if queued."""
    return conn.execute(
        """INSERT INTO repair_jobs (kind, chunk_hash, redundancy, node_id, reason) VALUES ('repair', %s, %s, %s, %s)
           ON CONFLICT (chunk_hash, redundancy) WHERE kind = 'repair' AND status = 'pending' DO NOTHING""",
        (chunk_hash, redundancy, node_id, reason)).rowcount == 1


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
