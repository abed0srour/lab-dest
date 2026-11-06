"""Repair worker: repairs, rebalances, garbage-collects and scrubs. Several workers share the work.

Every job is a row in the `repair_jobs` table, so its state, its bytes read and written and
the progress shown in the UI live in PostgreSQL. With RABBITMQ_URL set, a dispatcher moves
waiting jobs to RabbitMQ queues (one per kind) and the worker threads of every worker take
them from there: RabbitMQ spreads the jobs over the workers and redelivers a job if a worker
dies before acknowledging it. Without RABBITMQ_URL the threads claim jobs straight from
PostgreSQL (FOR UPDATE SKIP LOCKED).

  repair     a chunk lost a copy/fragment: rebuild it (copy a replica, or decode + re-encode)
  rebalance  without a chunk: compare every chunk with the hash ring and queue a move job for each
             chunk that is not where the ring places it; with a chunk: move its misplaced copies
  gc         with blobs: delete copies that no object uses any more; with a node: delete that
             node's orphan blobs (e.g. copies rebuilt elsewhere while it was down)
  scrub      ask a node to re-hash everything it stores; bad copies are queued for repair
"""

import json
import logging
import os
import socket
import threading
import time
from collections import defaultdict

import httpx
import pika
from prometheus_client import Counter, Histogram, start_http_server
from psycopg import errors
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from common import erasure
from common.chunking import sha256
from common.db import (CHUNK_JOB_CONFLICT, CHUNK_STATES, enqueue_blob_deletes, enqueue_node_job,
                       report_bad_copy)
from common.ring import HashRing

log = logging.getLogger("worker")

KINDS = ("repair", "rebalance", "gc", "scrub")  # RabbitMQ queues are checked in this order
RECENT = 30.0  # seconds: a blob rewritten this recently is never deleted by a gc job
JOBS = Counter("dos_worker_jobs_total", "Jobs finished by this worker", ["kind", "result"])
JOB_BYTES = Counter("dos_worker_bytes_total", "Bytes read from and written to nodes by jobs",
                    ["kind", "redundancy", "direction"])
JOB_SECONDS = Histogram("dos_worker_job_seconds", "Time to run one job", ["kind"],
                        buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300))


class Retry(Exception):
    """Temporary problem: try the job again later."""


class Unrecoverable(Exception):
    """Not enough healthy data left to rebuild the chunk."""


class Worker:
    def __init__(self):
        env = os.environ.get
        self.name = env("WORKER_ID", socket.gethostname())
        self.threads = int(env("WORKER_THREADS", "8"))
        self.vnodes = int(env("RING_VNODES", "128"))
        self.scrub_interval = float(env("SCRUB_INTERVAL", "600"))  # 0 = only when requested
        self.sweep_interval = float(env("SWEEP_INTERVAL", "30"))
        self.gc_interval = float(env("GC_INTERVAL", "1800"))       # orphan collection on every node
        self.gc_min_age = float(env("GC_MIN_AGE", "600"))          # never collect a blob younger than this
        self.max_attempts = int(env("MAX_ATTEMPTS", "5"))
        self.amqp_url = env("RABBITMQ_URL")                         # unset = take jobs from PostgreSQL
        self.metrics_port = int(env("METRICS_PORT", "9100"))
        self.pool = ConnectionPool(env("DATABASE_URL", "postgresql://dos:dos@localhost:5432/dos"),
                                   min_size=2, max_size=self.threads + 4, open=False,
                                   kwargs={"row_factory": dict_row, "autocommit": True},
                                   check=ConnectionPool.check_connection)
        self.http = httpx.Client(timeout=httpx.Timeout(60, connect=2.0))

    # ---- job state in PostgreSQL --------------------------------------------------

    def claim_next(self) -> dict | None:
        """PostgreSQL-only mode: take the oldest waiting job (repairs first)."""
        with self.pool.connection() as conn:
            return conn.execute(
                """UPDATE repair_jobs SET status = 'running', attempts = attempts + 1,
                                          started_at = coalesce(started_at, now())
                   WHERE id = (SELECT id FROM repair_jobs WHERE status = 'pending' AND run_after <= now()
                               ORDER BY kind <> 'repair', id LIMIT 1 FOR UPDATE SKIP LOCKED)
                   RETURNING *""").fetchone()

    def claim(self, job_id: int, redelivered: bool) -> dict | None:
        """RabbitMQ mode: take the job a message points to. A redelivered message means the worker
        that had it died, so its 'running' job is taken over."""
        with self.pool.connection() as conn:
            return conn.execute(
                """UPDATE repair_jobs SET status = 'running', attempts = attempts + 1,
                                          started_at = coalesce(started_at, now())
                   WHERE id = %s AND (status IN ('pending', 'queued') OR (status = 'running' AND %s))
                   RETURNING *""", (job_id, redelivered)).fetchone()

    def finish(self, job: dict, status: str, result: str, read: int = 0, written: int = 0) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                """UPDATE repair_jobs SET status = %s, finished_at = now(), result = %s,
                          bytes_read = bytes_read + %s, bytes_written = bytes_written + %s WHERE id = %s""",
                (status, result, read, written, job["id"]))
        red = job["redundancy"] or "-"
        JOB_BYTES.labels(job["kind"], red, "read").inc(read)
        JOB_BYTES.labels(job["kind"], red, "written").inc(written)

    def retry(self, job: dict, reason: str) -> None:
        if job["attempts"] >= self.max_attempts:
            return self.finish(job, "failed", f"gave up after {job['attempts']} attempts: {reason}")
        try:
            with self.pool.connection() as conn:
                conn.execute("""UPDATE repair_jobs SET status = 'pending', result = %s,
                                run_after = now() + make_interval(secs => %s) WHERE id = %s""",
                             (reason, 5 * job["attempts"], job["id"]))
        except errors.UniqueViolation:  # a newer waiting job for the same chunk already exists
            self.finish(job, "done", f"superseded by a newer job ({reason})")

    def run_job(self, job: dict) -> None:
        start, outcome = time.perf_counter(), "done"
        try:
            if job["kind"] == "repair":
                result, read, written = self.repair(job)
            elif job["kind"] == "rebalance":
                result, read, written = self.rebalance(job) if job["chunk_hash"] else self.rebalance_plan(job)
            elif job["kind"] == "gc":
                result, read, written = self.gc(job), 0, 0
            else:
                result, read, written = self.scrub(job), 0, 0
            self.finish(job, "done", result, read, written)
        except Retry as e:
            outcome = "retry"
            self.retry(job, str(e))
        except Unrecoverable as e:
            outcome = "failed"
            log.error("chunk %s (%s) cannot be repaired: %s", (job["chunk_hash"] or "")[:12], job["redundancy"], e)
            self.finish(job, "failed", f"unrecoverable: {e}")
        except Exception as e:
            outcome = "retry"
            log.exception("job %d failed", job["id"])
            self.retry(job, repr(e))
        JOBS.labels(job["kind"], outcome).inc()
        JOB_SECONDS.labels(job["kind"]).observe(time.perf_counter() - start)

    # ---- getting jobs -------------------------------------------------------------

    def work_from_database(self) -> None:
        while True:
            job = self.claim_next()
            if job is None:
                time.sleep(1)
            else:
                self.run_job(job)

    def rabbit(self) -> pika.BlockingConnection:
        conn = pika.BlockingConnection(pika.URLParameters(self.amqp_url))
        ch = conn.channel()
        for kind in KINDS:
            ch.queue_declare(f"jobs.{kind}", durable=True)
        return conn

    def work_from_rabbitmq(self) -> None:
        """One consumer per thread. A message is acknowledged only after its job has finished, so a
        job whose worker dies is delivered again to another worker."""
        while True:
            try:
                conn = self.rabbit()
                ch = conn.channel()
                while True:
                    for kind in KINDS:
                        method, _, body = ch.basic_get(f"jobs.{kind}")
                        if method is not None:
                            job = self.claim(json.loads(body)["id"], method.redelivered)
                            if job is not None:
                                self.run_job(job)
                            ch.basic_ack(method.delivery_tag)
                            break  # back to the most urgent queue
                    else:
                        conn.sleep(0.5)  # nothing to do; keeps the connection alive
            except pika.exceptions.AMQPError as e:
                log.warning("RabbitMQ connection lost (%r); reconnecting in 3 s", e)
                time.sleep(3)

    def dispatch(self, publisher) -> int:
        """Hand waiting jobs to RabbitMQ. SKIP LOCKED lets several workers dispatch side by side."""
        with self.pool.connection() as conn:
            jobs = conn.execute(
                """UPDATE repair_jobs SET status = 'queued', queued_at = now()
                   WHERE id IN (SELECT id FROM repair_jobs WHERE status = 'pending' AND run_after <= now()
                                ORDER BY id LIMIT 1000 FOR UPDATE SKIP LOCKED)
                   RETURNING id, kind""").fetchall()
        sent = 0
        try:
            for j in jobs:
                publisher.basic_publish("", f"jobs.{j['kind']}", json.dumps({"id": j["id"]}),
                                        pika.BasicProperties(delivery_mode=2))  # persistent
                sent += 1
        finally:
            if sent < len(jobs):  # publishing failed: give the rest back to the next dispatch
                with self.pool.connection() as conn:
                    conn.execute("UPDATE repair_jobs SET status = 'pending' WHERE id = ANY(%s) AND status = 'queued'",
                                 ([j["id"] for j in jobs[sent:]],))
        return sent

    # ---- node I/O -----------------------------------------------------------------

    def send(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Retry once if a kept-alive connection was closed by the node just as it was reused."""
        try:
            return self.http.request(method, url, **kwargs)
        except (httpx.RemoteProtocolError, httpx.ReadError):
            return self.http.request(method, url, **kwargs)

    def get_blob(self, url: str, blob_hash: str) -> tuple[bytes | None, bool]:
        """(data or None, whether the node was reachable)."""
        try:
            r = self.send("GET", f"{url}/chunks/{blob_hash}")
        except httpx.TransportError:
            return None, False
        return (r.content if r.status_code == 200 and sha256(r.content) == blob_hash else None), True

    def put_blob(self, url: str, blob_hash: str, data: bytes) -> bool:
        try:
            return self.send("PUT", f"{url}/chunks/{blob_hash}", content=data).status_code in (200, 201)
        except httpx.HTTPError:
            return False

    def delete_blob(self, url: str, blob_hash: str, older_than: float) -> bool:
        """Delete a blob unless it was written in the last `older_than` seconds: a blob that was just
        (re)written belongs to an upload, repair or move that is about to record it."""
        try:
            r = self.send("DELETE", f"{url}/chunks/{blob_hash}", params={"older_than": older_than})
            return r.status_code in (204, 404)
        except httpx.HTTPError:
            return False

    # ---- per-chunk work (repair and rebalance share the chunk lock) -----------------

    def with_chunk_lock(self, job: dict, fn):
        h, red = job["chunk_hash"], job["redundancy"]
        lock = f"{red}:{h}"
        with self.pool.connection() as conn:
            if not conn.execute("SELECT pg_try_advisory_lock(hashtext(%s)) AS ok", (lock,)).fetchone()["ok"]:
                raise Retry("another job is working on this chunk")
            try:
                return fn(conn, h, red)
            finally:
                conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock,))

    def chunk_state(self, conn, h: str, red: str) -> dict | None:
        chunk = conn.execute("SELECT * FROM chunks WHERE hash = %s AND redundancy = %s", (h, red)).fetchone()
        if chunk is None:
            return None
        nodes = {r["node_id"]: r for r in conn.execute("SELECT node_id, url, status FROM nodes")}
        alive = {n for n, r in nodes.items() if r["status"] == "alive"}
        locs = conn.execute("""SELECT fragment, node_id, blob_hash, blob_size FROM chunk_locations
                               WHERE chunk_hash = %s AND redundancy = %s""", (h, red)).fetchall()
        bad = {(r["fragment"], r["node_id"]) for r in conn.execute(
            "SELECT fragment, node_id FROM bad_copies WHERE chunk_hash = %s AND redundancy = %s", (h, red))}
        healthy = [l for l in locs if l["node_id"] in alive and (l["fragment"], l["node_id"]) not in bad]
        width = chunk["replicas"] if red == "replication" else chunk["ec_k"] + chunk["ec_m"]
        ring_order = HashRing(alive, self.vnodes).nodes_for(h, len(alive)) if alive else []
        return {"chunk": chunk, "nodes": nodes, "alive": alive, "locs": locs, "bad": bad,
                "healthy": healthy, "width": width, "ring_order": ring_order}

    def repair(self, job: dict) -> tuple[str, int, int]:
        return self.with_chunk_lock(job, self.repair_locked)

    def repair_locked(self, conn, h: str, red: str) -> tuple[str, int, int]:
        s = self.chunk_state(conn, h, red)
        if s is None:
            return "chunk no longer exists", 0, 0
        if red == "replication":
            keep, new, read, written, want = self.rebuild_replicas(h, s)
            have = len({l["node_id"] for l in keep + new})
        else:
            keep, new, read, written, want = self.rebuild_fragments(h, s)
            have = len({l["fragment"] for l in keep + new})
        complete = have >= want

        with conn.transaction():
            if not conn.execute("SELECT 1 FROM chunks WHERE hash = %s AND redundancy = %s FOR UPDATE",
                                (h, red)).fetchone():
                return "chunk was deleted during repair", read, written
            if complete:  # the chunk is whole again: forget copies on failed nodes and bad copies
                conn.execute("DELETE FROM chunk_locations WHERE chunk_hash = %s AND redundancy = %s", (h, red))
                conn.execute("DELETE FROM bad_copies WHERE chunk_hash = %s AND redundancy = %s", (h, red))
            rows = keep + new if complete else new
            with conn.cursor() as cur:
                cur.executemany(
                    """INSERT INTO chunk_locations (chunk_hash, redundancy, fragment, node_id, blob_hash, blob_size)
                       VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                    [(h, red, l["fragment"], l["node_id"], l["blob_hash"], l["blob_size"]) for l in rows])
                if not complete and new:
                    cur.executemany("DELETE FROM bad_copies WHERE chunk_hash = %s AND redundancy = %s "
                                    "AND fragment = %s AND node_id = %s",
                                    [(h, red, l["fragment"], l["node_id"]) for l in new])

        if not new:
            return ("already healthy" if complete else f"only {have} of {want} possible, no node available"), read, 0
        where = ", ".join(f"{l['node_id']}" + (f" (fragment {l['fragment']})" if l["fragment"] >= 0 else "")
                          for l in new)
        log.info("repaired chunk %s (%s): rebuilt on %s; read %d KB, wrote %d KB%s", h[:12], red, where,
                 read // 1024, written // 1024, "" if complete else f"; still {have}/{want}")
        return f"rebuilt {len(new)} on {where}", read, written

    def rebuild_replicas(self, h, s):
        want, healthy, nodes = s["width"], s["healthy"], s["nodes"]
        if len({l["node_id"] for l in healthy}) >= want:
            return healthy, [], 0, 0, want
        keep, data, read, unreachable = [], None, 0, False
        for l in healthy:  # find one copy that really is readable; the others are kept as they are
            if data is None:
                data, reachable = self.get_blob(nodes[l["node_id"]]["url"], h)
                unreachable |= not reachable
                if data is None:
                    continue  # unreadable: not counted, it will be replaced
                read = len(data)
            keep.append(l)
        if data is None:
            if unreachable:  # a node that is down but not yet marked failed: try again later
                raise Retry("no replica reachable right now")
            raise Unrecoverable("no readable replica left")
        have = {l["node_id"] for l in keep}
        rewrite_in_place = [n for f, n in s["bad"] if n in s["alive"]]
        targets = [n for n in dict.fromkeys(rewrite_in_place + s["ring_order"]) if n not in have][:want - len(have)]
        new, written = [], 0
        for n in targets:
            if self.put_blob(nodes[n]["url"], h, data):
                new.append({"fragment": -1, "node_id": n, "blob_hash": h, "blob_size": len(data)})
                written += len(data)
        if targets and not new:
            raise Retry("could not write any new replica")
        return keep, new, read, written, want

    def rebuild_fragments(self, h, s):
        chunk, nodes, alive = s["chunk"], s["nodes"], s["alive"]
        k, m = chunk["ec_k"], chunk["ec_m"]
        want = k + m
        by_frag: dict[int, dict] = {}
        for l in s["healthy"]:
            by_frag.setdefault(l["fragment"], l)
        if len(by_frag) >= want:
            return list(by_frag.values()), [], 0, 0, want
        if len(by_frag) < k:
            raise Unrecoverable(f"only {len(by_frag)} of the {k} fragments needed are left")
        got, read, unreachable = {}, 0, False
        for i in sorted(by_frag):  # data fragments first; stop as soon as k are in hand
            if len(got) == k:
                break
            blob, reachable = self.get_blob(nodes[by_frag[i]["node_id"]]["url"], by_frag[i]["blob_hash"])
            unreachable |= not reachable
            if blob is None:
                del by_frag[i]  # unreadable: treat it as missing too
                continue
            got[i] = blob
            read += len(blob)
        if len(got) < k:
            if unreachable:
                raise Retry(f"only {len(got)} of the {k} fragments needed are reachable right now")
            raise Unrecoverable(f"only {len(got)} of the {k} fragments needed are readable")
        data = erasure.decode(got, k, m, chunk["size"])
        if sha256(data) != h:
            raise Unrecoverable("rebuilt chunk does not match its SHA-256")
        fragments = erasure.encode(data, k, m)

        used = {l["node_id"] for l in by_frag.values()}
        load = {n: sum(l["node_id"] == n for l in by_frag.values()) for n in alive}
        new, written = [], 0
        for i in (i for i in range(want) if i not in by_frag):
            in_place = [n for f, n in s["bad"] if f == i and n in alive]
            candidates = in_place + [n for n in s["ring_order"] if n not in used]
            # Fewer free nodes than missing fragments: put a second fragment on the least loaded node.
            candidates += sorted(alive, key=lambda n: load[n])
            blob = fragments[i]
            for n in dict.fromkeys(candidates):
                if self.put_blob(nodes[n]["url"], sha256(blob), blob):
                    new.append({"fragment": i, "node_id": n, "blob_hash": sha256(blob), "blob_size": len(blob)})
                    used.add(n)
                    load[n] += 1
                    written += len(blob)
                    break
        if not new:
            raise Retry("could not write any rebuilt fragment")
        return list(by_frag.values()), new, read, written, want

    # ---- rebalance ----------------------------------------------------------------

    def rebalance_plan(self, job: dict) -> tuple[str, int, int]:
        """Find every chunk that does not sit on the nodes the ring assigns it now, and queue a move."""
        with self.pool.connection() as conn:
            alive = [r["node_id"] for r in conn.execute("SELECT node_id FROM nodes WHERE status = 'alive'")]
            chunks = conn.execute("SELECT hash, redundancy, replicas, ec_k, ec_m FROM chunks").fetchall()
            current = defaultdict(set)
            for l in conn.execute("SELECT chunk_hash, redundancy, node_id FROM chunk_locations"):
                current[(l["chunk_hash"], l["redundancy"])].add(l["node_id"])
        ring = HashRing(alive, self.vnodes)
        moves = []
        for c in chunks:
            width = c["replicas"] if c["redundancy"] == "replication" else c["ec_k"] + c["ec_m"]
            if len(alive) >= width and current[(c["hash"], c["redundancy"])] != set(ring.nodes_for(c["hash"], width)):
                moves.append((c["hash"], c["redundancy"]))
        with self.pool.connection() as conn:
            queued = conn.execute(
                f"""INSERT INTO repair_jobs (kind, chunk_hash, redundancy, reason)
                    SELECT 'rebalance', h, r, %s FROM unnest(%s::text[], %s::text[]) AS m(h, r)
                    {CHUNK_JOB_CONFLICT}""",
                (f"rebalance ({job['reason']})", [h for h, _ in moves], [r for _, r in moves])).rowcount
            if queued:
                # Copies moved away that were themselves only seconds old (e.g. written by a repair just
                # before) are kept by their gc job; collect them as orphans once they are old enough.
                for node in alive:
                    enqueue_node_job(conn, "gc", node, "after rebalancing", delay=self.gc_min_age + 15)
        log.info("rebalance plan (%s): %d of %d chunks are not where the hash ring places them; "
                 "%d move jobs queued", job["reason"], len(moves), len(chunks), queued)
        return f"{queued} of {len(chunks)} chunks to move", 0, 0

    def rebalance(self, job: dict) -> tuple[str, int, int]:
        return self.with_chunk_lock(job, self.rebalance_locked)

    def rebalance_locked(self, conn, h: str, red: str) -> tuple[str, int, int]:
        """Move this chunk's misplaced copies (or fragments) onto the nodes the ring assigns it now.
        Old copies are deleted later by a gc job, so a reader that still uses them is not broken."""
        s = self.chunk_state(conn, h, red)
        if s is None:
            return "chunk no longer exists", 0, 0
        width, nodes = s["width"], s["nodes"]
        if len(s["alive"]) < width:
            return "not enough alive nodes", 0, 0
        desired = set(s["ring_order"][:width])  # ring_order is the ring walk over the alive nodes
        moved, read, written = [], 0, 0  # (new location, old location)

        if red == "replication":
            have = {l["node_id"] for l in s["healthy"]}
            if len(have) < width:
                return "chunk is degraded: left to the repair", 0, 0
            to_add = [n for n in s["ring_order"][:width] if n not in have]
            to_drop = [l for l in s["healthy"] if l["node_id"] not in desired]
            if not to_add:
                return "already in place", 0, 0
            data = None
            for l in s["healthy"]:
                data, _ = self.get_blob(nodes[l["node_id"]]["url"], h)
                if data is not None:
                    read = len(data)
                    break
            if data is None:
                raise Retry("no readable replica right now")
            for n, old in zip(to_add, to_drop):
                if self.put_blob(nodes[n]["url"], h, data):
                    moved.append(({"fragment": -1, "node_id": n, "blob_hash": h, "blob_size": len(data)}, old))
                    written += len(data)
        else:
            by_frag: dict[int, dict] = {}
            for l in s["healthy"]:
                by_frag.setdefault(l["fragment"], l)
            if len(by_frag) < width:
                return "chunk is degraded: left to the repair", 0, 0
            seen: set[str] = set()
            misplaced = []  # fragments on a node outside the ring's set, or a second fragment on one node
            for i in sorted(by_frag):
                l = by_frag[i]
                if l["node_id"] not in desired or l["node_id"] in seen:
                    misplaced.append(l)
                seen.add(l["node_id"])
            free = [n for n in s["ring_order"][:width] if n not in seen]
            if not misplaced or not free:
                return "already in place", 0, 0
            for old, n in zip(misplaced, free):
                blob, _ = self.get_blob(nodes[old["node_id"]]["url"], old["blob_hash"])
                if blob is not None and self.put_blob(nodes[n]["url"], old["blob_hash"], blob):
                    moved.append(({**old, "node_id": n}, old))
                    read += len(blob)
                    written += len(blob)

        if not moved:
            raise Retry("could not move any copy")
        with conn.transaction():
            if not conn.execute("SELECT 1 FROM chunks WHERE hash = %s AND redundancy = %s FOR UPDATE",
                                (h, red)).fetchone():
                return "chunk was deleted during rebalancing", read, written
            with conn.cursor() as cur:
                cur.executemany(
                    """INSERT INTO chunk_locations (chunk_hash, redundancy, fragment, node_id, blob_hash, blob_size)
                       VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                    [(h, red, new["fragment"], new["node_id"], new["blob_hash"], new["blob_size"]) for new, _ in moved])
                cur.executemany(
                    """DELETE FROM chunk_locations WHERE chunk_hash = %s AND redundancy = %s
                       AND fragment = %s AND node_id = %s""",
                    [(h, red, old["fragment"], old["node_id"]) for _, old in moved])
            enqueue_blob_deletes(conn, [(old["blob_hash"], old["node_id"]) for _, old in moved],
                                 f"rebalanced chunk {h[:12]}")
        log.info("rebalanced chunk %s (%s): %s", h[:12], red,
                 ", ".join(f"{old['node_id']} -> {new['node_id']}" for new, old in moved))
        return f"moved {len(moved)}", read, written

    # ---- garbage collection -------------------------------------------------------

    def gc(self, job: dict) -> str:
        with self.pool.connection() as conn:
            nodes = {r["node_id"]: r for r in conn.execute("SELECT node_id, url, status FROM nodes")}
        if job["node_id"]:  # orphans on one node
            node = nodes.get(job["node_id"])
            if node is None or node["status"] != "alive":
                return "node is not alive"
            try:
                # Only blobs older than GC_MIN_AGE: a blob that was just written may belong to an
                # upload or repair that has not committed its location yet.
                listed = self.send("GET", f"{node['url']}/chunks",
                                   params={"older_than": self.gc_min_age}).raise_for_status().json()["chunks"]
            except httpx.HTTPError as e:
                raise Retry(f"node {job['node_id']} unreachable: {e!r}")
            with self.pool.connection() as conn:  # read the references after listing the node
                used = {r["blob_hash"] for r in conn.execute(
                    "SELECT blob_hash FROM chunk_locations WHERE node_id = %s", (job["node_id"],))}
            orphans = [b for b in listed if b not in used]
            deleted = sum(self.delete_blob(node["url"], b, self.gc_min_age) for b in orphans)
            log.info("gc %s: %d blobs checked, %d orphans deleted", job["node_id"], len(listed), deleted)
            return f"{deleted} orphan blobs deleted"

        blobs = [tuple(b) for b in (job["detail"] or {}).get("blobs", [])]
        with self.pool.connection() as conn:  # a blob can be shared: keep those still referenced
            used = {(r["blob_hash"], r["node_id"]) for r in conn.execute(
                "SELECT blob_hash, node_id FROM chunk_locations WHERE blob_hash = ANY(%s)", ([b for b, _ in blobs],))}
        deleted = skipped = 0
        for blob_hash, node_id in blobs:
            node = nodes.get(node_id)
            if (blob_hash, node_id) in used or node is None or node["status"] != "alive":
                skipped += 1  # still used, or on a failed node (collected as an orphan when it returns)
            elif self.delete_blob(node["url"], blob_hash, RECENT):
                deleted += 1
            else:
                skipped += 1  # rewritten moments ago (e.g. the same file uploaded again): left to orphan gc
        if deleted:
            log.info("gc (%s): %d blobs deleted, %d kept", job["reason"], deleted, skipped)
        return f"{deleted} blobs deleted, {skipped} kept"

    # ---- scrub --------------------------------------------------------------------

    def scrub(self, job: dict) -> str:
        node_id = job["node_id"]
        with self.pool.connection() as conn:
            node = conn.execute("SELECT url, status FROM nodes WHERE node_id = %s", (node_id,)).fetchone()
            if node is None or node["status"] != "alive":
                return "node is not alive"
            # Snapshot the expected blobs *before* listing the node, so a blob written in between
            # can never look missing.
            expected = conn.execute("""SELECT chunk_hash, redundancy, fragment, blob_hash FROM chunk_locations
                                       WHERE node_id = %s""", (node_id,)).fetchall()
        try:
            report = self.http.post(f"{node['url']}/scrub", timeout=900).raise_for_status().json()
            present = set(self.send("GET", f"{node['url']}/chunks").raise_for_status().json()["chunks"])
        except httpx.HTTPError as e:
            raise Retry(f"node {node_id} unreachable: {e!r}")
        corrupted = set(report["corrupted"])
        found = 0
        with self.pool.connection() as conn:
            for e in expected:
                if e["blob_hash"] in corrupted:
                    reason = "corrupted (scrub)"
                elif e["blob_hash"] not in present:
                    reason = "missing (scrub)"
                else:
                    continue
                report_bad_copy(conn, e["chunk_hash"], e["redundancy"], e["fragment"], node_id, reason)
                found += 1
                log.warning("scrub %s: %s copy of chunk %s (%s, fragment %d) queued for repair",
                            node_id, reason.split()[0], e["chunk_hash"][:12], e["redundancy"], e["fragment"])
        log.info("scrubbed %s: %d blobs (%d MB) re-hashed, %d corrupted, %d bad copies queued for repair",
                 node_id, report["checked"], report["bytes"] // 2**20, len(corrupted), found)
        return f"checked {report['checked']} blobs, {found} bad copies"

    # ---- background scheduling ----------------------------------------------------

    def sweep(self) -> int:
        """Anti-entropy: queue every degraded chunk that has no repair job yet."""
        with self.pool.connection() as conn:
            return conn.execute(
                f"""INSERT INTO repair_jobs (kind, chunk_hash, redundancy, reason)
                    SELECT 'repair', s.hash, s.redundancy, 'degraded (sweep)' FROM ({CHUNK_STATES}) s
                    WHERE s.state = 'degraded' AND NOT EXISTS (
                        SELECT 1 FROM repair_jobs j WHERE j.kind = 'repair'
                          AND j.status IN ('pending', 'queued', 'running')
                          AND j.chunk_hash = s.hash AND j.redundancy = s.redundancy)
                    {CHUNK_JOB_CONFLICT}""").rowcount

    def queue_for_all_nodes(self, kind: str, reason: str) -> None:
        with self.pool.connection() as conn:
            for r in conn.execute("SELECT node_id FROM nodes WHERE status = 'alive'").fetchall():
                enqueue_node_job(conn, kind, r["node_id"], reason)

    def queue_counts(self) -> dict:
        with self.pool.connection() as conn:
            rows = conn.execute("SELECT status, count(*) AS n FROM repair_jobs GROUP BY status")
            return {r["status"]: r["n"] for r in rows}

    def scheduler(self, publisher) -> None:
        last = {"sweep": time.monotonic(), "scrub": time.monotonic(), "gc": time.monotonic(),
                "stale": 0.0, "log": 0.0}
        last_counts = None
        while True:
            now = time.monotonic()
            if publisher is not None:
                self.dispatch(publisher.channel_for_publish)
                if now - last["stale"] >= 60:  # a dispatcher that died between marking and publishing
                    with self.pool.connection() as conn:
                        conn.execute("""UPDATE repair_jobs SET status = 'pending' WHERE status = 'queued'
                                        AND queued_at < now() - interval '15 minutes'""")
                    last["stale"] = now
            if now - last["sweep"] >= self.sweep_interval:
                if queued := self.sweep():
                    log.info("sweep: %d degraded chunks queued for repair", queued)
                last["sweep"] = now
            if self.scrub_interval > 0 and now - last["scrub"] >= self.scrub_interval:
                self.queue_for_all_nodes("scrub", "scheduled")
                last["scrub"] = now
            if self.gc_interval > 0 and now - last["gc"] >= self.gc_interval:
                self.queue_for_all_nodes("gc", "scheduled")
                last["gc"] = now
            if now - last["log"] >= 5:
                counts = self.queue_counts()
                waiting = counts.get("pending", 0) + counts.get("queued", 0)
                if counts != last_counts and (waiting or counts.get("running") or
                                              (last_counts and last_counts.get("queued"))):
                    log.info("job queue: %d waiting (%d in RabbitMQ), %d running, %d done, %d failed", waiting,
                             counts.get("queued", 0), counts.get("running", 0), counts.get("done", 0),
                             counts.get("failed", 0))
                last_counts, last["log"] = counts, now
            if publisher is not None:
                publisher.connection.sleep(1)  # services the RabbitMQ connection while waiting
            else:
                time.sleep(1)

    def run(self) -> None:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("pika").setLevel(logging.WARNING)
        start_http_server(self.metrics_port)
        self.pool.open(wait=True, timeout=120)
        mode = "RabbitMQ" if self.amqp_url else "PostgreSQL"
        log.info("worker %s started: %d threads taking jobs from %s; scrub every %s, orphan gc every %.0f s",
                 self.name, self.threads, mode,
                 f"{self.scrub_interval:.0f} s" if self.scrub_interval > 0 else "never", self.gc_interval)
        target = self.work_from_rabbitmq if self.amqp_url else self.work_from_database
        for _ in range(self.threads):
            threading.Thread(target=target, daemon=True).start()
        while True:
            try:
                publisher = Publisher(self.rabbit()) if self.amqp_url else None
                self.scheduler(publisher)
            except pika.exceptions.AMQPError as e:
                log.warning("dispatcher lost RabbitMQ (%r); reconnecting in 3 s", e)
                time.sleep(3)
            except Exception:
                log.exception("scheduler error; restarting it in 3 s")
                time.sleep(3)


class Publisher:
    def __init__(self, connection: pika.BlockingConnection):
        self.connection = connection
        self.channel_for_publish = connection.channel()
        self.channel_for_publish.confirm_delivery()  # basic_publish waits until RabbitMQ has the message


if __name__ == "__main__":
    Worker().run()
