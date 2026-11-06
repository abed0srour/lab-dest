"""Client for the storage cluster: chunking, dedup check, placement, quorum writes and reads,
erasure coding.

Used by the command-line tool and the tests now, and by the gateway in Phase 5.
"""

import hashlib
import time
from concurrent.futures import ThreadPoolExecutor
from typing import BinaryIO
from urllib.parse import quote

import httpx

from . import erasure
from .chunking import chunk_stream, sha256

BATCH = 16  # chunks planned per metadata call; 16 x 4 MB keeps at most 64 MB in memory

# 127.0.0.1, not "localhost": on Windows localhost resolves to IPv6 ::1 first, and Docker
# Desktop's IPv6 port forwarding adds about 40 ms to every request that has a body.
DEFAULT_METADATA_URL = "http://127.0.0.1:9000"

# A node that refused a connection is skipped for this long. The metadata service only marks it
# failed after its heartbeat timeout, and on Windows every refused connection costs about 2 s.
DOWN_FOR = 30.0


class StorageError(Exception):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status  # HTTP status from the metadata service, if it came from there


class StorageClient:
    def __init__(self, metadata_url: str = DEFAULT_METADATA_URL, public: bool = True, timeout: float = 120):
        self.metadata_url = metadata_url.rstrip("/")
        self.public = public  # outside Docker, reach nodes through their host-mapped ports
        self.http = httpx.Client(timeout=httpx.Timeout(timeout, connect=1.0))
        self.pool = ThreadPoolExecutor(max_workers=16)  # parallel requests to the storage nodes
        self.down_until: dict[str, float] = {}

    def _is_down(self, node_id: str) -> bool:
        return self.down_until.get(node_id, 0) > time.monotonic()

    def _mark_down(self, node_id: str) -> None:
        self.down_until[node_id] = time.monotonic() + DOWN_FOR

    def close(self) -> None:
        self.pool.shutdown()
        self.http.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _request(self, method: str, url: str, retry: bool = True, **kwargs) -> httpx.Response:
        """Send a request; retry once if a kept-alive connection turns out to have been closed
        by the server just as it was reused (the request was never processed)."""
        try:
            return self.http.request(method, url, **kwargs)
        except (httpx.RemoteProtocolError, httpx.ReadError):
            if not retry:
                raise
            return self.http.request(method, url, **kwargs)

    def _meta(self, method: str, path: str, **kwargs) -> dict | list:
        # Committing an object is the one request that must not run twice (it would add a version).
        retry = method != "POST" or not path.startswith("/buckets/")
        r = self._request(method, f"{self.metadata_url}{path}", retry=retry, **kwargs)
        if r.status_code >= 400:
            try:
                detail = r.json().get("detail", r.text)
            except ValueError:
                detail = r.text
            raise StorageError(f"{method} {path} -> {r.status_code}: {detail}", r.status_code)
        return r.json()

    def _node(self, method: str, node: dict, blob_hash: str, **kwargs) -> httpx.Response | None:
        """A request to a storage node, or None if the node is down (it is then skipped for DOWN_FOR)."""
        if self._is_down(node["node_id"]):
            return None
        try:
            return self._request(method, f"{self._node_url(node)}/chunks/{blob_hash}", **kwargs)
        except httpx.TransportError:
            self._mark_down(node["node_id"])
            return None

    def _node_url(self, node: dict) -> str:
        return (self.public and node.get("public_url")) or node["url"]

    @staticmethod
    def _object_path(bucket: str, key: str) -> str:
        return f"/buckets/{bucket}/objects/{quote(key)}"

    # ---- cluster and buckets -------------------------------------------------

    def nodes(self) -> list[dict]:
        return self._meta("GET", "/nodes")

    def cluster(self) -> dict:
        return self._meta("GET", "/cluster")

    def stats(self, bucket: str | None = None) -> dict:
        return self._meta("GET", "/stats", params={"bucket": bucket} if bucket else None)

    def repair_stats(self, bucket: str | None = None, since_id: int = 0) -> dict:
        params = {"since_id": since_id, **({"bucket": bucket} if bucket else {})}
        return self._meta("GET", "/repair/stats", params=params)

    def scrub(self) -> dict:
        return self._meta("POST", "/scrub")

    def rebalance(self) -> dict:
        return self._meta("POST", "/rebalance")

    def gc(self) -> dict:
        return self._meta("POST", "/gc")

    def create_bucket(self, bucket: str, redundancy: str = "replication") -> dict:
        return self._meta("PUT", f"/buckets/{bucket}", json={"redundancy": redundancy})

    def get_bucket(self, bucket: str) -> dict:
        return self._meta("GET", f"/buckets/{bucket}")

    def list_buckets(self) -> list[dict]:
        return self._meta("GET", "/buckets")

    def search(self, engine: str = "postgres", **filters) -> dict:
        """Filters: bucket, prefix, min_size, max_size, after, before, tag (list of "key=value"), limit,
        and q (free text, Elasticsearch only)."""
        path = "/search/es" if engine == "elasticsearch" else "/search"
        return self._meta("GET", path, params={k: v for k, v in filters.items() if v not in (None, "", [])})

    # ---- write path ----------------------------------------------------------

    def put_file(self, bucket: str, key: str, path: str, chunking: str = "fixed",
                 tags: dict | None = None, content_type: str | None = None) -> dict:
        with open(path, "rb") as f:
            return self.put_stream(bucket, key, f, chunking, tags, content_type)

    def put_stream(self, bucket: str, key: str, stream: BinaryIO, chunking: str = "fixed",
                   tags: dict | None = None, content_type: str | None = None) -> dict:
        """Chunk the stream, store new chunks on their ring nodes (quorum write), then commit.

        The result includes `sha256`, the hash of the whole object.
        """
        refs: list[dict] = []
        batch: list[bytes] = []
        uploaded: set[str] = set()  # new chunks already sent during this upload
        whole = hashlib.sha256()
        for data in chunk_stream(stream, chunking):
            whole.update(data)
            batch.append(data)
            if len(batch) == BATCH:
                refs += self._store(bucket, batch, uploaded)
                batch = []
        if batch:
            refs += self._store(bucket, batch, uploaded)
        body = {"size": sum(r["size"] for r in refs), "chunking": chunking, "tags": tags or {},
                "sha256": whole.hexdigest(), "content_type": content_type, "chunks": refs}
        return self._meta("POST", self._object_path(bucket, key), json=body)

    def _put_blob(self, node: dict, blob_hash: str, data: bytes) -> bool:
        r = self._node("PUT", node, blob_hash, content=data)
        return r is not None and r.status_code in (200, 201)

    def _store(self, bucket: str, batch: list[bytes], uploaded: set[str]) -> list[dict]:
        hashes = [sha256(d) for d in batch]
        plan = self._meta("POST", "/plan", json={"bucket": bucket, "hashes": hashes})
        refs = []
        for data, h in zip(batch, hashes):
            p = plan["chunks"][h]
            if p["exists"] or h in uploaded:
                refs.append({"hash": h, "size": len(data), "locations": None})
                continue
            if plan["redundancy"] == "ec":
                blobs = erasure.encode(data, plan["ec"]["k"], plan["ec"]["m"])
                fragments = list(range(len(blobs)))
            else:
                blobs = [data] * len(p["nodes"])
                fragments = [-1] * len(p["nodes"])
            blob_hashes = [h if f == -1 else sha256(b) for f, b in zip(fragments, blobs)]
            # Send every copy (or fragment) in parallel; the write succeeds once W of them are stored.
            acks = list(self.pool.map(self._put_blob, p["nodes"], blob_hashes, blobs))
            locations = [{"node_id": n["node_id"], "fragment": f, "blob_hash": bh, "blob_size": len(b)}
                         for n, f, bh, b, ok in zip(p["nodes"], fragments, blob_hashes, blobs, acks) if ok]
            if len(locations) < plan["write_quorum"]:
                raise StorageError(f"chunk {h[:12]} stored on only {len(locations)} of {len(p['nodes'])} nodes; "
                                   f"write quorum W={plan['write_quorum']} not reached, upload aborted")
            uploaded.add(h)
            refs.append({"hash": h, "size": len(data), "locations": locations})
        return refs

    # ---- read path -----------------------------------------------------------

    def get_object(self, bucket: str, key: str, version: int | None = None) -> dict:
        return self._meta("GET", self._object_path(bucket, key), params={"version": version} if version else None)

    def get_file(self, bucket: str, key: str, path: str, version: int | None = None) -> dict:
        """Download to `path`. The result's `sha256` is the hash of the data actually downloaded."""
        meta = self.get_object(bucket, key, version)
        whole = hashlib.sha256()
        with open(path, "wb") as f:
            for data in self.iter_object(meta):
                whole.update(data)
                f.write(data)
        return {**meta, "sha256": whole.hexdigest()}

    def iter_object(self, meta: dict, start: int = 0, end: int | None = None):
        """Yield the bytes start..end (inclusive) of an object, reading only the chunks that overlap them."""
        end = meta["size"] - 1 if end is None else end
        for chunk in meta["chunks"]:
            first, last = chunk["offset"], chunk["offset"] + chunk["size"] - 1
            if last < start or first > end:
                continue
            data = self.read_chunk(chunk, meta["read_quorum"])
            yield data[max(start - first, 0):min(end, last) - first + 1]

    def _get_blob(self, loc: dict, blob_hash: str) -> tuple[bytes | None, str]:
        """(data, "ok"), or (None, "bad") if the node answered with a wrong/missing blob, or (None, "down")."""
        r = self._node("GET", loc, blob_hash)
        if r is None:
            return None, "down"
        if r.status_code == 200 and sha256(r.content) == blob_hash:
            return r.content, "ok"
        return None, "bad"

    def _head_blob(self, loc: dict, blob_hash: str) -> str:
        r = self._node("HEAD", loc, blob_hash)
        if r is None:
            return "down"
        return "ok" if r.status_code == 200 else "bad"

    def _report(self, chunk: dict, loc: dict, reason: str) -> None:
        try:
            self._meta("POST", "/report", json={"chunk_hash": chunk["hash"], "redundancy": chunk["redundancy"],
                                                "fragment": loc["fragment"], "node_id": loc["node_id"],
                                                "reason": reason})
        except (StorageError, httpx.HTTPError):
            pass

    def _usable(self, chunk: dict) -> list[dict]:
        """Copies worth trying, best first: nodes that are up and alive, then the rest."""
        locs = [l for l in chunk["locations"] if l.get("url") and not l.get("bad")]
        return sorted(locs, key=lambda l: (self._is_down(l["node_id"]), not l.get("alive"), l["fragment"]))

    def read_chunk(self, chunk: dict, read_quorum: int = 1) -> bytes:
        if chunk["redundancy"] == "ec":
            return self._read_fragments(chunk)
        return self._read_replica(chunk, read_quorum)

    def _read_replica(self, chunk: dict, read_quorum: int) -> bytes:
        """Quorum read: GET the first replica and confirm R-1 more with HEAD, in parallel.

        Chunks never change and verify themselves (SHA-256), so one valid copy is enough to
        return the data; with W + R > N the R replicas asked always include one that
        acknowledged the write, so the read needs no extra round trip.
        """
        h, locs = chunk["hash"], self._usable(chunk)
        if not locs:
            raise StorageError(f"chunk {h[:12]} has no copies")
        first = self.pool.submit(self._get_blob, locs[0], h)
        heads = [(l, self.pool.submit(self._head_blob, l, h)) for l in locs[1:read_quorum]]
        data, status = first.result()
        for loc, fut in heads:
            if fut.result() == "bad":
                self._report(chunk, loc, "missing on read")
        if status == "bad":
            self._report(chunk, locs[0], "corrupted or missing on read")
        for loc in locs[1:]:
            if data is not None:
                break
            data, status = self._get_blob(loc, h)
            if status == "bad":
                self._report(chunk, loc, "corrupted or missing on read")
        if data is None:
            raise StorageError(f"no readable copy of chunk {h[:12]}")
        return data

    def _read_fragments(self, chunk: dict) -> bytes:
        """Fetch k fragments in parallel (data fragments first, parity only if some fail), then decode."""
        h, k, m = chunk["hash"], chunk["ec_k"], chunk["ec_m"]
        by_frag: dict[int, dict] = {}
        for loc in self._usable(chunk):
            by_frag.setdefault(loc["fragment"], loc)
        order = sorted(by_frag, key=lambda i: (self._is_down(by_frag[i]["node_id"]), i))
        wanted, spare = order[:k], order[k:]
        got: dict[int, bytes] = {}
        while wanted:
            results = list(self.pool.map(lambda i: (i, *self._get_blob(by_frag[i], by_frag[i]["blob_hash"])), wanted))
            wanted = []
            for i, data, status in results:
                if data is not None:
                    got[i] = data
                    continue
                if status == "bad":
                    self._report(chunk, by_frag[i], "corrupted or missing on read")
                if spare:
                    wanted.append(spare.pop(0))
        if len(got) < k:
            raise StorageError(f"chunk {h[:12]}: only {len(got)} of the {k} fragments needed are readable")
        data = erasure.decode(got, k, m, chunk["size"])
        if sha256(data) != h:
            raise StorageError(f"chunk {h[:12]}: decoded data does not match its SHA-256")
        return data

    # ---- listing and delete --------------------------------------------------

    def list_objects(self, bucket: str, prefix: str = "") -> list[dict]:
        return self._meta("GET", f"/buckets/{bucket}/objects", params={"prefix": prefix})

    def delete_object(self, bucket: str, key: str, version: int | None = None) -> dict:
        return self._meta("DELETE", self._object_path(bucket, key), params={"version": version} if version else None)
