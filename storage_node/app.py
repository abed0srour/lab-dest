"""Storage node: stores content-addressed chunks on local disk.

Every chunk is addressed by the SHA-256 of its content. The hash is checked on
write and on every read, so a corrupted chunk is never served.
"""

import asyncio
import hashlib
import logging
import os
import shutil
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi import Path as PathParam

log = logging.getLogger("storage_node")

ChunkHash = Annotated[str, PathParam(pattern=r"^[0-9a-f]{64}$")]


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ChunkStore:
    """Chunks live at <root>/chunks/ab/cd/<hash> so no directory gets too large."""

    def __init__(self, root: Path):
        self.root = root
        self.chunks = root / "chunks"
        self.tmp = root / "tmp"
        self.chunks.mkdir(parents=True, exist_ok=True)
        self.tmp.mkdir(parents=True, exist_ok=True)

    def path(self, h: str) -> Path:
        return self.chunks / h[:2] / h[2:4] / h

    def put(self, h: str, data: bytes) -> bool:
        """Write atomically (temp file + rename). Returns True if the chunk is new.

        An existing copy is overwritten, which is how a corrupted chunk gets repaired.
        """
        dest = self.path(h)
        existed = dest.exists()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=self.tmp, delete=False) as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(f.name, dest)
        return not existed

    def get(self, h: str) -> bytes | None:
        try:
            return self.path(h).read_bytes()
        except FileNotFoundError:
            return None

    def size(self, h: str) -> int | None:
        try:
            return self.path(h).stat().st_size
        except FileNotFoundError:
            return None

    def delete(self, h: str) -> bool:
        try:
            self.path(h).unlink()
            return True
        except FileNotFoundError:
            return False

    def hashes(self) -> list[str]:
        return [p.name for p in self.chunks.glob("*/*/*")]

    def stats(self) -> dict:
        return {
            "free_bytes": shutil.disk_usage(self.root).free,
            "chunk_count": len(self.hashes()),
        }


async def heartbeat_loop(store: ChunkStore, node_id: str, node_url: str, metadata_url: str, interval: float):
    healthy = None
    async with httpx.AsyncClient(timeout=interval) as client:
        while True:
            stats = await asyncio.to_thread(store.stats)
            error = None
            try:
                r = await client.post(f"{metadata_url}/heartbeat", json={"node_id": node_id, "url": node_url, **stats})
                r.raise_for_status()
            except httpx.HTTPError as e:
                error = e
            # Log only when the state changes, not on every beat.
            if (error is None) != healthy:
                healthy = error is None
                if healthy:
                    log.info("heartbeats to %s OK", metadata_url)
                else:
                    log.warning("heartbeats to %s failing: %r", metadata_url, error)
            await asyncio.sleep(interval)


def create_app() -> FastAPI:
    logging.basicConfig(level=logging.INFO)
    node_id = os.environ.get("NODE_ID", "storage-node-local")
    node_url = os.environ.get("NODE_URL", f"http://{node_id}:8000")
    metadata_url = os.environ.get("METADATA_URL")  # unset = no heartbeats
    interval = float(os.environ.get("HEARTBEAT_INTERVAL", "5"))
    store = ChunkStore(Path(os.environ.get("DATA_DIR", "./data")))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task = None
        if metadata_url:
            task = asyncio.create_task(heartbeat_loop(store, node_id, node_url, metadata_url, interval))
        yield
        if task:
            task.cancel()

    app = FastAPI(title=f"Storage node {node_id}", lifespan=lifespan)

    @app.put("/chunks/{h}", status_code=201)
    async def put_chunk(h: ChunkHash, request: Request, response: Response):
        data = await request.body()
        if sha256(data) != h:
            raise HTTPException(400, "content does not match its SHA-256 address")
        created = await asyncio.to_thread(store.put, h, data)
        if not created:
            response.status_code = 200
        return {"hash": h, "size": len(data), "created": created}

    @app.get("/chunks/{h}")
    async def get_chunk(h: ChunkHash):
        data = await asyncio.to_thread(store.get, h)
        if data is None:
            raise HTTPException(404, "chunk not found")
        if sha256(data) != h:
            log.error("corrupted chunk %s", h)
            raise HTTPException(409, "chunk is corrupted")
        return Response(data, media_type="application/octet-stream")

    @app.head("/chunks/{h}")
    async def head_chunk(h: ChunkHash):
        size = await asyncio.to_thread(store.size, h)
        if size is None:
            raise HTTPException(404, "chunk not found")
        return Response(headers={"Content-Length": str(size)})

    @app.delete("/chunks/{h}", status_code=204)
    async def delete_chunk(h: ChunkHash):
        if not await asyncio.to_thread(store.delete, h):
            raise HTTPException(404, "chunk not found")

    @app.get("/chunks")
    async def list_chunks():
        return {"node_id": node_id, "chunks": await asyncio.to_thread(store.hashes)}

    @app.get("/health")
    async def health():
        return {"node_id": node_id, "status": "ok", **await asyncio.to_thread(store.stats)}

    return app
