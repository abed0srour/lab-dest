"""API gateway: the public, S3-like HTTP API of the storage cluster.

Every request (except /health and the docs) needs an `X-API-Key` header and is
rate-limited per key. Uploads are streamed: the request body flows through the
chunker to the storage nodes 4 MB at a time, so a large file is never held in
memory. Downloads stream back chunk by chunk and support byte ranges.
"""

import asyncio
import io
import logging
import math
import os
import queue
import re
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime
from email.utils import format_datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import APIKeyHeader
from starlette.requests import ClientDisconnect

from common.client import StorageClient, StorageError

log = logging.getLogger("gateway")


class GatewayIdHeader:
    """Adds X-Gateway-Id to every response, to show which gateway answered (Phase 7 runs two).
    Plain ASGI, so streamed uploads and downloads pass through untouched."""

    def __init__(self, app, gateway_id: str):
        self.app = app
        self.header = (b"x-gateway-id", gateway_id.encode())

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        async def send_with_header(message):
            if message["type"] == "http.response.start":
                message["headers"] = [*message.get("headers", []), self.header]
            await send(message)

        await self.app(scope, receive, send_with_header)


# ---- authentication and rate limiting ---------------------------------------

class ApiKeys:
    """API_KEYS="key:user:requests_per_second,...". Each key has a token bucket of 2 seconds of burst."""

    def __init__(self, spec: str):
        self.keys: dict[str, tuple[str, float]] = {}
        for item in filter(None, (s.strip() for s in spec.split(","))):
            key, user, rate = item.split(":")
            self.keys[key] = (user, float(rate))
        self.buckets: dict[str, tuple[float, float]] = {}  # key -> (tokens, last refill time)
        self.lock = threading.Lock()

    def take(self, key: str, rate: float) -> tuple[bool, float]:
        """Take one token. Returns (allowed, seconds until the next token)."""
        burst = max(2 * rate, 1)
        with self.lock:
            now = time.monotonic()
            tokens, last = self.buckets.get(key, (burst, now))
            tokens = min(burst, tokens + (now - last) * rate)
            allowed = tokens >= 1
            self.buckets[key] = (tokens - 1 if allowed else tokens, now)
            return allowed, 0 if allowed else (1 - tokens) / rate


api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False, description="Your API key")


def parse_tags(text: str | None) -> dict[str, str]:
    if not text:
        return {}
    try:
        return dict(pair.strip().split("=", 1) for pair in text.split(","))
    except ValueError:
        raise HTTPException(400, 'X-Tags must look like "type=video,project=lab"')


def parse_range(header: str, size: int) -> tuple[int, int] | None:
    """A single "bytes=start-end" range (also "start-" and "-suffix"); None means "send everything"."""
    m = re.fullmatch(r"\s*bytes=(\d*)-(\d*)\s*", header)
    if m is None or (m[1] == "" and m[2] == ""):
        return None  # multiple or malformed ranges: RFC 9110 allows answering with the whole object
    if m[1] == "":
        start, end = max(size - int(m[2]), 0), size - 1
    else:
        start = int(m[1])
        end = min(int(m[2]), size - 1) if m[2] else size - 1
    if size == 0 or start >= size or start > end:
        raise HTTPException(416, "range not satisfiable", headers={"Content-Range": f"bytes */{size}"})
    return start, end


class BodyStream(io.RawIOBase):
    """A blocking file-like view of the request body, filled by the event loop.

    The bounded queue gives backpressure: the client upload only advances as fast as
    the chunks are stored on the nodes.
    """

    def __init__(self, maxsize: int = 64):
        self.q: queue.Queue = queue.Queue(maxsize)
        self.buf = bytearray()
        self.ended = False
        self.consumer_done = threading.Event()

    def feed(self, item) -> bool:
        """Called from the event loop side (in a thread). False if the consumer already stopped."""
        while not self.consumer_done.is_set():
            try:
                self.q.put(item, timeout=0.2)
                return True
            except queue.Full:
                continue
        return False

    def readable(self) -> bool:
        return True

    def read(self, n: int = -1) -> bytes:
        while not self.ended and (n < 0 or len(self.buf) < n):
            item = self.q.get()
            if item is None:
                self.ended = True
            elif isinstance(item, BaseException):
                raise item
            else:
                self.buf += item
        n = len(self.buf) if n < 0 else min(n, len(self.buf))
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out


def object_headers(meta: dict) -> dict[str, str]:
    created = datetime.fromisoformat(meta["created_at"])
    headers = {
        "ETag": f'"{meta["sha256"]}"' if meta.get("sha256") else "",
        "Last-Modified": format_datetime(created, usegmt=True),
        "Accept-Ranges": "bytes",
        "X-Object-Version": str(meta["version"]),
        "X-Redundancy": meta["redundancy"],
        "X-Tags": ",".join(f"{k}={v}" for k, v in meta["tags"].items()),
    }
    return {k: v for k, v in headers.items() if v}


def create_app() -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    storage = StorageClient(os.environ.get("METADATA_URL", "http://metadata:8000"), public=False)
    keys = ApiKeys(os.environ.get("API_KEYS", "student-key-2026:student:200"))
    gateway_id = os.environ.get("GATEWAY_ID", socket.gethostname())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Uploads hold a thread each while they stream into the cluster.
        asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(64))
        log.info("gateway %s ready: %d API keys", gateway_id, len(keys.keys))
        yield
        storage.close()

    app = FastAPI(title="Distributed storage gateway",
                  description="S3-like API. Click **Authorize** and enter an API key to try the endpoints.",
                  lifespan=lifespan)

    app.add_middleware(GatewayIdHeader, gateway_id=gateway_id)

    @app.exception_handler(StorageError)
    async def storage_error(request: Request, e: StorageError):
        status = e.status if e.status in (400, 404, 409, 503) else 503 if e.status is None else 502
        return JSONResponse({"detail": str(e).split(": ", 1)[-1]}, status_code=status)

    async def authenticate(request: Request, response: Response,
                           api_key: Annotated[str | None, Depends(api_key_header)]) -> str:
        if api_key not in keys.keys:
            raise HTTPException(401, "missing or invalid API key (send it in the X-API-Key header)",
                                headers={"WWW-Authenticate": "ApiKey"})
        user, rate = keys.keys[api_key]
        allowed, wait = keys.take(api_key, rate)
        if not allowed:
            raise HTTPException(429, f"rate limit of {rate:g} requests per second exceeded",
                                headers={"Retry-After": str(math.ceil(wait)), "X-RateLimit-Limit": f"{rate:g}/s"})
        response.headers["X-RateLimit-Limit"] = f"{rate:g}/s"
        request.state.user = user
        return user

    @app.get("/health", tags=["status"])
    def health():
        return {"status": "ok", "gateway": gateway_id}

    api = APIRouter(dependencies=[Depends(authenticate)])

    # ---- buckets -------------------------------------------------------------

    @api.put("/buckets/{bucket}", status_code=201, tags=["buckets"])
    def create_bucket(bucket: str, response: Response,
                      redundancy: Literal["replication", "ec"] = "replication"):
        """Create a bucket. `redundancy=replication` (3 copies) or `redundancy=ec` (erasure coding 4 + 2)."""
        b = storage.create_bucket(bucket, redundancy)
        if not b["created"]:
            response.status_code = 200
        return b

    @api.get("/buckets", tags=["buckets"])
    def list_buckets():
        return storage.list_buckets()

    # ---- objects -------------------------------------------------------------

    @api.put("/buckets/{bucket}/objects/{key:path}", status_code=201, tags=["objects"])
    async def put_object(bucket: str, key: str, request: Request,
                         chunking: Literal["fixed", "cdc"] = "fixed",
                         x_tags: Annotated[str | None, Header(description='e.g. "type=video,project=lab"')] = None,
                         content_type: Annotated[str | None, Header()] = None):
        """Upload an object. The body is streamed straight into the cluster (any size).
        Uploading the same key again creates a new version."""
        tags = parse_tags(x_tags)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, storage.get_bucket, bucket)  # 404 before reading the body
        body = BodyStream()

        def consume() -> dict:
            try:
                return storage.put_stream(bucket, key, body, chunking, tags, content_type)
            finally:
                body.consumer_done.set()

        stored = loop.run_in_executor(None, consume)
        try:
            async for piece in request.stream():
                if piece and not await loop.run_in_executor(None, body.feed, piece):
                    break  # the upload failed on the cluster side; its error is raised below
            await loop.run_in_executor(None, body.feed, None)
        except ClientDisconnect:
            await loop.run_in_executor(None, body.feed, ConnectionError("client disconnected"))
        result = await stored
        log.info("%s uploaded %s/%s v%d: %d bytes", request.state.user, bucket, key, result["version"], result["size"])
        return result

    @api.get("/buckets/{bucket}/objects", tags=["objects"])
    def list_objects(bucket: str, prefix: str = "", limit: int = Query(1000, ge=1, le=10000)):
        """List the latest version of every object, optionally only keys starting with `prefix`."""
        return storage._meta("GET", f"/buckets/{bucket}/objects", params={"prefix": prefix, "limit": limit})

    @api.get("/buckets/{bucket}/objects/{key:path}", tags=["objects"],
             responses={200: {"description": "the object"}, 206: {"description": "part of the object"}})
    def get_object(bucket: str, key: str, version: int | None = None, metadata: bool = False,
                   range_: Annotated[str | None, Header(alias="Range")] = None):
        """Download an object (or one version). Send `Range: bytes=start-end` for part of it.
        With `?metadata=true` you get its metadata as JSON instead."""
        meta = storage.get_object(bucket, key, version)
        if metadata:
            return {k: v for k, v in meta.items() if k != "chunks"} | {
                "chunks": [{"seq": c["seq"], "hash": c["hash"], "size": c["size"], "offset": c["offset"],
                            "refcount": c["refcount"],
                            "stored_on": [(f"{l['fragment']}:" if l["fragment"] >= 0 else "") + l["node_id"]
                                          for l in c["locations"]]} for c in meta["chunks"]]}
        headers = object_headers(meta)
        media_type = meta.get("content_type") or "application/octet-stream"
        span = parse_range(range_, meta["size"]) if range_ else None
        if span is None:
            headers["Content-Length"] = str(meta["size"])
            return StreamingResponse(storage.iter_object(meta), media_type=media_type, headers=headers)
        start, end = span
        headers["Content-Range"] = f"bytes {start}-{end}/{meta['size']}"
        headers["Content-Length"] = str(end - start + 1)
        return StreamingResponse(storage.iter_object(meta, start, end), status_code=206,
                                 media_type=media_type, headers=headers)

    @api.head("/buckets/{bucket}/objects/{key:path}", tags=["objects"])
    def head_object(bucket: str, key: str, version: int | None = None):
        """Object metadata in the response headers, without the data."""
        meta = storage.get_object(bucket, key, version)
        return Response(headers={**object_headers(meta), "Content-Length": str(meta["size"])},
                        media_type=meta.get("content_type") or "application/octet-stream")

    @api.delete("/buckets/{bucket}/objects/{key:path}", tags=["objects"])
    def delete_object(bucket: str, key: str, version: int | None = None):
        """Delete an object (every version, or only `version`). Chunks no other object uses are freed."""
        return storage.delete_object(bucket, key, version)

    # ---- search and status ---------------------------------------------------

    @api.get("/search", tags=["search"])
    def search(bucket: str | None = None, prefix: str = "", min_size: int | None = None,
               max_size: int | None = None, after: datetime | None = None, before: datetime | None = None,
               tag: Annotated[list[str], Query(description="key=value, repeat for several")] = [],
               q: Annotated[str | None, Query(description="free text (Elasticsearch only)")] = None,
               engine: Literal["postgres", "elasticsearch"] = "postgres",
               limit: int = Query(100, ge=1, le=10000)):
        """Find objects by key prefix, size (bytes), creation date (ISO, e.g. 2026-11-06) and tags.
        `engine=elasticsearch` also takes free text `q` (words or any part of a key, typos allowed)
        and returns facet counts. Elasticsearch shows new uploads after about a second."""
        if q and engine == "postgres":
            raise HTTPException(400, "free-text search (q) needs engine=elasticsearch")
        return storage.search(engine, bucket=bucket, prefix=prefix, min_size=min_size, max_size=max_size,
                              after=after.isoformat() if after else None,
                              before=before.isoformat() if before else None, tag=tag, q=q, limit=limit)

    @api.get("/search/status", tags=["search"])
    def search_status():
        """Elasticsearch health and how many objects are indexed."""
        return storage._meta("GET", "/search/status")

    @api.post("/search/reindex", tags=["search"])
    def search_reindex():
        """Rebuild the Elasticsearch index from the metadata database."""
        return storage._meta("POST", "/search/reindex")

    @api.get("/cluster/status", tags=["status"])
    def cluster_status():
        """Nodes (health, free space, chunks per node), degraded objects and the repair queue."""
        h = storage.cluster()
        nodes = [{"node_id": n["node_id"], "status": n["status"], "free_bytes": n["free_bytes"],
                  "blobs_on_disk": n["chunk_count"], "blobs_in_metadata": n["assigned_blobs"],
                  "last_heartbeat_s": round(n["seconds_since_heartbeat"], 1)} for n in h["nodes"]["list"]]
        return {"nodes": nodes,
                "summary": {"nodes": h["nodes"]["total"], "alive": h["nodes"]["alive"],
                            "failed": h["nodes"]["failed"], "free_bytes": sum(n["free_bytes"] for n in nodes)},
                "objects": h["objects"], "chunks": h["chunks"], "repair_jobs": h["repair_jobs"],
                "settings": h["settings"]}

    @api.get("/cluster/repairs", tags=["status"])
    def cluster_repairs(bucket: str | None = None, since_id: int = 0):
        """Repair jobs and repair traffic (bytes read and written) per redundancy scheme."""
        return storage.repair_stats(bucket, since_id)

    @api.post("/cluster/scrub", tags=["status"])
    def cluster_scrub():
        """Re-check every stored chunk on every node now; bad copies are repaired."""
        return storage.scrub()

    app.include_router(api)
    return app
