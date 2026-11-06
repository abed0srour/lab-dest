import hashlib

import pytest
from fastapi.testclient import TestClient

from storage_node.app import create_app


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("NODE_ID", "test-node")
    monkeypatch.delenv("METADATA_URL", raising=False)
    with TestClient(create_app()) as c:
        yield c


def test_put_then_get_returns_same_bytes(client):
    data = b"hello chunk"
    r = client.put(f"/chunks/{sha(data)}", content=data)
    assert r.status_code == 201
    r = client.get(f"/chunks/{sha(data)}")
    assert r.status_code == 200
    assert r.content == data


def test_put_same_chunk_twice_stores_it_once(client):
    data = b"duplicate"
    assert client.put(f"/chunks/{sha(data)}", content=data).status_code == 201
    r = client.put(f"/chunks/{sha(data)}", content=data)
    assert r.status_code == 200
    assert r.json()["created"] is False
    assert client.get("/health").json()["chunk_count"] == 1


def test_put_rejects_content_that_does_not_match_hash(client):
    r = client.put(f"/chunks/{sha(b'expected')}", content=b"something else")
    assert r.status_code == 400


def test_malformed_hash_is_rejected(client):
    assert client.get("/chunks/not-a-hash").status_code == 422
    assert client.put("/chunks/ABC", content=b"x").status_code == 422


def test_head_and_delete(client):
    data = b"x" * 1000
    h = sha(data)
    client.put(f"/chunks/{h}", content=data)
    r = client.head(f"/chunks/{h}")
    assert r.status_code == 200
    assert r.headers["content-length"] == "1000"
    assert client.delete(f"/chunks/{h}").status_code == 204
    assert client.head(f"/chunks/{h}").status_code == 404
    assert client.delete(f"/chunks/{h}").status_code == 404


def test_corrupted_chunk_is_rejected_on_read_and_repaired_by_put(client, tmp_path):
    data = b"precious data"
    h = sha(data)
    client.put(f"/chunks/{h}", content=data)
    (tmp_path / "chunks" / h[:2] / h[2:4] / h).write_bytes(b"bit rot!")
    assert client.get(f"/chunks/{h}").status_code == 409

    client.put(f"/chunks/{h}", content=data)
    assert client.get(f"/chunks/{h}").content == data


def test_health_and_list_report_stored_chunks(client):
    hashes = set()
    for data in (b"one", b"two"):
        hashes.add(sha(data))
        client.put(f"/chunks/{sha(data)}", content=data)
    health = client.get("/health").json()
    assert health["node_id"] == "test-node"
    assert health["chunk_count"] == 2
    assert health["free_bytes"] > 0
    assert set(client.get("/chunks").json()["chunks"]) == hashes
