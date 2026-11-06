"""End-to-end tests against the running cluster (docker compose up -d). Skipped if it is not running."""

import io
import os
import uuid

import httpx
import pytest

from common.chunking import MiB
from common.client import DEFAULT_METADATA_URL, StorageClient

METADATA = os.environ.get("METADATA_URL", DEFAULT_METADATA_URL)


def cluster_running() -> bool:
    try:
        return httpx.get(f"{METADATA}/health", timeout=2).status_code == 200
    except httpx.HTTPError:
        return False


pytestmark = pytest.mark.skipif(not cluster_running(), reason="cluster is not running (docker compose up -d)")


@pytest.fixture
def client():
    with StorageClient(METADATA) as c:
        c.create_bucket("pytest")
        yield c


@pytest.fixture
def key():
    return f"test-{uuid.uuid4().hex[:8]}"


def test_upload_then_download_returns_identical_bytes_with_three_copies(client, key, tmp_path):
    data = os.urandom(9 * MiB)
    src, out = tmp_path / "src.bin", tmp_path / "out.bin"
    src.write_bytes(data)
    r = client.put_file("pytest", key, str(src))
    assert r["chunks"] == 3
    client.get_file("pytest", key, str(out))
    assert out.read_bytes() == data
    chunks = client.get_object("pytest", key)["chunks"]
    assert all(len({l["node_id"] for l in c["locations"]}) == 3 for c in chunks)
    client.delete_object("pytest", key)


def test_duplicate_is_stored_once_and_removed_after_the_last_reference(client, key):
    data = os.urandom(100_000)
    assert client.put_stream("pytest", key + "-a", io.BytesIO(data))["new_chunks"] == 1
    b = client.put_stream("pytest", key + "-b", io.BytesIO(data))
    assert (b["new_chunks"], b["deduplicated_chunks"]) == (0, 1)
    assert client.get_object("pytest", key + "-a")["chunks"][0]["refcount"] == 2

    assert client.delete_object("pytest", key + "-a")["freed_chunks"] == 0
    assert client.get_object("pytest", key + "-b")["chunks"][0]["refcount"] == 1
    r = client.delete_object("pytest", key + "-b")
    assert (r["freed_chunks"], r["removed_copies"]) == (1, 3)


def test_uploading_the_same_key_again_creates_a_new_version(client, key, tmp_path):
    client.put_stream("pytest", key, io.BytesIO(b"version one"))
    assert client.put_stream("pytest", key, io.BytesIO(b"version two"))["version"] == 2
    out = tmp_path / "v1"
    client.get_file("pytest", key, str(out), version=1)
    assert out.read_bytes() == b"version one"
    assert client.delete_object("pytest", key)["deleted_versions"] == 2


def test_cdc_upload_of_an_edited_file_reuses_almost_every_chunk(client, key):
    base = os.urandom(24 * MiB)
    edited = base[:12 * MiB] + b"inserted" + base[12 * MiB:]
    client.put_stream("pytest", key + "-v1", io.BytesIO(base), chunking="cdc")
    r = client.put_stream("pytest", key + "-v2", io.BytesIO(edited), chunking="cdc")
    assert r["new_chunks"] <= 2 and r["deduplicated_chunks"] >= r["chunks"] - 2
    client.delete_object("pytest", key + "-v1")
    client.delete_object("pytest", key + "-v2")
