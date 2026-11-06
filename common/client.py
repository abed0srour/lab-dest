"""Client for the storage cluster: chunking, dedup check, placement, replica writes and reads.

Used by the command-line tool and the tests now, and by the gateway in Phase 5.
"""

import hashlib
from typing import BinaryIO
from urllib.parse import quote

import httpx

from .chunking import chunk_stream, sha256

BATCH = 16  # chunks planned per metadata call; 16 x 4 MB keeps at most 64 MB in memory

# 127.0.0.1, not "localhost": on Windows localhost resolves to IPv6 ::1 first, and Docker
# Desktop's IPv6 port forwarding adds about 40 ms to every request that has a body.
DEFAULT_METADATA_URL = "http://127.0.0.1:9000"


class StorageError(Exception):
    pass


class StorageClient:
    def __init__(self, metadata_url: str = DEFAULT_METADATA_URL, public: bool = True, timeout: float = 120):
        self.metadata_url = metadata_url.rstrip("/")
        self.public = public  # outside Docker, reach nodes through their host-mapped ports
        self.http = httpx.Client(timeout=timeout)

    def close(self) -> None:
        self.http.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _meta(self, method: str, path: str, **kwargs) -> dict | list:
        r = self.http.request(method, f"{self.metadata_url}{path}", **kwargs)
        if r.status_code >= 400:
            raise StorageError(f"{method} {path} -> {r.status_code}: {r.text}")
        return r.json()

    def _node_url(self, node: dict) -> str:
        return (self.public and node.get("public_url")) or node["url"]

    @staticmethod
    def _object_path(bucket: str, key: str) -> str:
        return f"/buckets/{bucket}/objects/{quote(key)}"

    # ---- cluster and buckets -------------------------------------------------

    def nodes(self) -> list[dict]:
        return self._meta("GET", "/nodes")

    def stats(self, bucket: str | None = None) -> dict:
        return self._meta("GET", "/stats", params={"bucket": bucket} if bucket else None)

    def create_bucket(self, bucket: str, redundancy: str = "replication") -> dict:
        return self._meta("PUT", f"/buckets/{bucket}", json={"redundancy": redundancy})

    def list_buckets(self) -> list[dict]:
        return self._meta("GET", "/buckets")

    # ---- write path ----------------------------------------------------------

    def put_file(self, bucket: str, key: str, path: str, chunking: str = "fixed",
                 tags: dict | None = None) -> dict:
        with open(path, "rb") as f:
            return self.put_stream(bucket, key, f, chunking, tags)

    def put_stream(self, bucket: str, key: str, stream: BinaryIO, chunking: str = "fixed",
                   tags: dict | None = None) -> dict:
        """Chunk the stream, store new chunks on their ring nodes, then commit the object.

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
                refs += self._store(batch, uploaded)
                batch = []
        if batch:
            refs += self._store(batch, uploaded)
        body = {"size": sum(r["size"] for r in refs), "chunking": chunking, "tags": tags or {}, "chunks": refs}
        result = self._meta("POST", self._object_path(bucket, key), json=body)
        return {**result, "sha256": whole.hexdigest()}

    def _store(self, batch: list[bytes], uploaded: set[str]) -> list[dict]:
        hashes = [sha256(d) for d in batch]
        plan = self._meta("POST", "/plan", json={"hashes": hashes})["chunks"]
        refs = []
        for data, h in zip(batch, hashes):
            p = plan[h]
            if p["exists"] or h in uploaded:
                refs.append({"hash": h, "size": len(data), "nodes": None})
                continue
            stored = []
            for node in p["nodes"]:
                try:
                    r = self.http.put(f"{self._node_url(node)}/chunks/{h}", content=data)
                except httpx.HTTPError:
                    continue
                if r.status_code in (200, 201):
                    stored.append(node["node_id"])
            # Phase 3 requires every replica; Phase 4 relaxes this to a write quorum W.
            if len(stored) < len(p["nodes"]):
                raise StorageError(f"chunk {h[:12]} stored on only {len(stored)} of {len(p['nodes'])} nodes")
            uploaded.add(h)
            refs.append({"hash": h, "size": len(data), "nodes": stored})
        return refs

    # ---- read path -----------------------------------------------------------

    def get_object(self, bucket: str, key: str, version: int | None = None) -> dict:
        return self._meta("GET", self._object_path(bucket, key), params={"version": version} if version else None)

    def get_file(self, bucket: str, key: str, path: str, version: int | None = None) -> dict:
        """Download to `path`. The result includes `sha256` of the downloaded data."""
        meta = self.get_object(bucket, key, version)
        whole = hashlib.sha256()
        with open(path, "wb") as f:
            for chunk in meta["chunks"]:
                data = self._read_chunk(chunk)
                whole.update(data)
                f.write(data)
        return {**meta, "sha256": whole.hexdigest()}

    def _read_chunk(self, chunk: dict) -> bytes:
        for loc in sorted(chunk["locations"], key=lambda l: not l.get("alive")):
            if not loc.get("url"):
                continue
            try:
                r = self.http.get(f"{self._node_url(loc)}/chunks/{chunk['hash']}")
            except httpx.HTTPError:
                continue
            if r.status_code == 200 and sha256(r.content) == chunk["hash"]:
                return r.content
        raise StorageError(f"no healthy copy of chunk {chunk['hash'][:12]}")

    # ---- listing and delete --------------------------------------------------

    def list_objects(self, bucket: str, prefix: str = "") -> list[dict]:
        return self._meta("GET", f"/buckets/{bucket}/objects", params={"prefix": prefix})

    def delete_object(self, bucket: str, key: str, version: int | None = None) -> dict:
        return self._meta("DELETE", self._object_path(bucket, key), params={"version": version} if version else None)
