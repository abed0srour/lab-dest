"""Repair worker: brings chunks back to full redundancy and scrubs the storage nodes.

Jobs come from the `repair_jobs` table (claimed with FOR UPDATE SKIP LOCKED, so
several workers can share the queue):

  repair  a chunk lost a copy/fragment (node failed, corruption found, write below
          full redundancy). Replication: copy a healthy replica to a new node.
          Erasure coding: read any k fragments, decode, re-encode the missing ones.
  scrub   ask a node to re-hash everything it stores; corrupted or missing blobs
          are recorded as bad copies and their chunks queued for repair.

Every repair records the bytes it read and wrote, which is the repair traffic
compared between replication and erasure coding.
"""

import logging
import os
import threading
import time

import httpx
from psycopg import errors
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from common import erasure
from common.chunking import sha256
from common.db import CHUNK_STATES, report_bad_copy
from common.ring import HashRing

log = logging.getLogger("repair")


class Retry(Exception):
    """Temporary problem: try the job again later."""


class Unrecoverable(Exception):
    """Not enough healthy data left to rebuild the chunk."""


class Worker:
    def __init__(self):
        env = os.environ.get
        self.threads = int(env("WORKER_THREADS", "8"))
        self.vnodes = int(env("RING_VNODES", "128"))
        self.scrub_interval = float(env("SCRUB_INTERVAL", "600"))  # 0 = only when requested
        self.sweep_interval = float(env("SWEEP_INTERVAL", "30"))
        self.max_attempts = int(env("MAX_ATTEMPTS", "5"))
        self.pool = ConnectionPool(env("DATABASE_URL", "postgresql://dos:dos@localhost:5432/dos"),
                                   min_size=2, max_size=self.threads + 4, open=False,
                                   kwargs={"row_factory": dict_row, "autocommit": True})
        self.http = httpx.Client(timeout=httpx.Timeout(60, connect=2.0))

    # ---- queue ---------------------------------------------------------------

    def claim(self) -> dict | None:
        with self.pool.connection() as conn:
            return conn.execute(
                """UPDATE repair_jobs SET status = 'running', attempts = attempts + 1,
                                          started_at = coalesce(started_at, now())
                   WHERE id = (SELECT id FROM repair_jobs WHERE status = 'pending' AND run_after <= now()
                               ORDER BY kind = 'scrub', id LIMIT 1 FOR UPDATE SKIP LOCKED)
                   RETURNING *""").fetchone()

    def finish(self, job: dict, status: str, result: str, read: int = 0, written: int = 0) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                """UPDATE repair_jobs SET status = %s, finished_at = now(), result = %s,
                          bytes_read = bytes_read + %s, bytes_written = bytes_written + %s WHERE id = %s""",
                (status, result, read, written, job["id"]))

    def retry(self, job: dict, reason: str) -> None:
        if job["attempts"] >= self.max_attempts:
            return self.finish(job, "failed", f"gave up after {job['attempts']} attempts: {reason}")
        try:
            with self.pool.connection() as conn:
                conn.execute("""UPDATE repair_jobs SET status = 'pending', result = %s,
                                run_after = now() + make_interval(secs => %s) WHERE id = %s""",
                             (reason, 5 * job["attempts"], job["id"]))
        except errors.UniqueViolation:  # a newer pending job for the same chunk already exists
            self.finish(job, "done", f"superseded by a newer job ({reason})")

    def work(self) -> None:
        while True:
            job = self.claim()
            if job is None:
                time.sleep(1)
                continue
            try:
                if job["kind"] == "repair":
                    result, read, written = self.repair(job)
                else:
                    result, read, written = self.scrub(job), 0, 0
                self.finish(job, "done", result, read, written)
            except Retry as e:
                self.retry(job, str(e))
            except Unrecoverable as e:
                log.error("chunk %s (%s) cannot be repaired: %s", job["chunk_hash"][:12], job["redundancy"], e)
                self.finish(job, "failed", f"unrecoverable: {e}")
            except Exception as e:
                log.exception("job %d failed", job["id"])
                self.retry(job, repr(e))

    # ---- node I/O ------------------------------------------------------------

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

    # ---- repair --------------------------------------------------------------

    def repair(self, job: dict) -> tuple[str, int, int]:
        h, red = job["chunk_hash"], job["redundancy"]
        lock = f"{red}:{h}"
        with self.pool.connection() as conn:
            if not conn.execute("SELECT pg_try_advisory_lock(hashtext(%s)) AS ok", (lock,)).fetchone()["ok"]:
                raise Retry("another worker is repairing this chunk")
            try:
                return self.repair_locked(conn, h, red)
            finally:
                conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock,))

    def repair_locked(self, conn, h: str, red: str) -> tuple[str, int, int]:
        chunk = conn.execute("SELECT * FROM chunks WHERE hash = %s AND redundancy = %s", (h, red)).fetchone()
        if chunk is None:
            return "chunk no longer exists", 0, 0
        nodes = {r["node_id"]: r for r in conn.execute("SELECT node_id, url, status FROM nodes")}
        alive = {n for n, r in nodes.items() if r["status"] == "alive"}
        locs = conn.execute("""SELECT fragment, node_id, blob_hash, blob_size FROM chunk_locations
                               WHERE chunk_hash = %s AND redundancy = %s""", (h, red)).fetchall()
        bad = {(r["fragment"], r["node_id"]) for r in conn.execute(
            "SELECT fragment, node_id FROM bad_copies WHERE chunk_hash = %s AND redundancy = %s", (h, red))}
        healthy = [l for l in locs if l["node_id"] in alive and (l["fragment"], l["node_id"]) not in bad]
        ring_order = HashRing(alive, self.vnodes).nodes_for(h, len(alive)) if alive else []

        if red == "replication":
            keep, new, read, written, want = self.rebuild_replicas(h, chunk, healthy, bad, alive, ring_order, nodes)
            have = len({l["node_id"] for l in keep + new})
        else:
            keep, new, read, written, want = self.rebuild_fragments(h, chunk, healthy, bad, alive, ring_order, nodes,
                                                                     locs)
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

    def rebuild_replicas(self, h, chunk, healthy, bad, alive, ring_order, nodes):
        want = chunk["replicas"]
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
        rewrite_in_place = [n for f, n in bad if n in alive]
        targets = [n for n in dict.fromkeys(rewrite_in_place + ring_order) if n not in have][:want - len(have)]
        new, written = [], 0
        for n in targets:
            if self.put_blob(nodes[n]["url"], h, data):
                new.append({"fragment": -1, "node_id": n, "blob_hash": h, "blob_size": len(data)})
                written += len(data)
        if targets and not new:
            raise Retry("could not write any new replica")
        return keep, new, read, written, want

    def rebuild_fragments(self, h, chunk, healthy, bad, alive, ring_order, nodes, locs):
        k, m = chunk["ec_k"], chunk["ec_m"]
        want = k + m
        by_frag: dict[int, dict] = {}
        for l in healthy:
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
            in_place = [n for f, n in bad if f == i and n in alive]
            candidates = in_place + [n for n in ring_order if n not in used]
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

    # ---- scrub ---------------------------------------------------------------

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
            present = set(self.http.get(f"{node['url']}/chunks").raise_for_status().json()["chunks"])
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

    # ---- background scheduling -------------------------------------------------

    def sweep(self) -> int:
        """Anti-entropy: queue every degraded chunk that has no repair job yet."""
        with self.pool.connection() as conn:
            return conn.execute(
                f"""INSERT INTO repair_jobs (kind, chunk_hash, redundancy, reason)
                    SELECT 'repair', s.hash, s.redundancy, 'degraded (sweep)' FROM ({CHUNK_STATES}) s
                    WHERE s.state = 'degraded' AND NOT EXISTS (
                        SELECT 1 FROM repair_jobs j WHERE j.kind = 'repair' AND j.status IN ('pending', 'running')
                          AND j.chunk_hash = s.hash AND j.redundancy = s.redundancy)
                    ON CONFLICT (chunk_hash, redundancy) WHERE kind = 'repair' AND status = 'pending'
                    DO NOTHING""").rowcount

    def queue_scrubs(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("""INSERT INTO repair_jobs (kind, node_id, reason)
                            SELECT 'scrub', node_id, 'scheduled' FROM nodes WHERE status = 'alive'
                            ON CONFLICT (node_id) WHERE kind = 'scrub' AND status IN ('pending', 'running')
                            DO NOTHING""")

    def queue_counts(self) -> dict:
        with self.pool.connection() as conn:
            rows = conn.execute("SELECT status, count(*) AS n FROM repair_jobs WHERE kind = 'repair' GROUP BY status")
            return {r["status"]: r["n"] for r in rows}

    def run(self) -> None:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
        logging.getLogger("httpx").setLevel(logging.WARNING)
        self.pool.open(wait=True, timeout=120)
        log.info("repair worker started: %d threads, scrub every %s, sweep every %.0f s", self.threads,
                 f"{self.scrub_interval:.0f} s" if self.scrub_interval > 0 else "never (on request only)",
                 self.sweep_interval)
        for _ in range(self.threads):
            threading.Thread(target=self.work, daemon=True).start()
        last_sweep = last_scrub = time.monotonic()
        last_counts = None
        while True:
            time.sleep(5)
            now = time.monotonic()
            try:
                if now - last_sweep >= self.sweep_interval:
                    if queued := self.sweep():
                        log.info("sweep: %d degraded chunks queued for repair", queued)
                    last_sweep = now
                if self.scrub_interval > 0 and now - last_scrub >= self.scrub_interval:
                    self.queue_scrubs()
                    last_scrub = now
                counts = self.queue_counts()
                active = counts.get("pending", 0) + counts.get("running", 0)
                if counts != last_counts and (active or (last_counts and last_counts.get("pending"))):
                    log.info("repair queue: %d pending, %d running, %d done, %d failed", counts.get("pending", 0),
                             counts.get("running", 0), counts.get("done", 0), counts.get("failed", 0))
                last_counts = counts
            except Exception:
                log.exception("scheduler error")


if __name__ == "__main__":
    Worker().run()
