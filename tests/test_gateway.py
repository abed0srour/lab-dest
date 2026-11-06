"""API tests against the running gateway (docker compose up -d). Skipped if it is not running."""

import hashlib
import os
import uuid
from datetime import date, timedelta

import httpx
import pytest

GATEWAY = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8080")
KEY = {"X-API-Key": os.environ.get("API_KEY", "student-key-2026")}
SLOW_KEY = {"X-API-Key": "demo-key-2026"}  # 5 requests/s, burst 10
MiB = 1024 * 1024


def gateway_running() -> bool:
    try:
        return httpx.get(f"{GATEWAY}/health", timeout=2).status_code == 200
    except httpx.HTTPError:
        return False


pytestmark = pytest.mark.skipif(not gateway_running(), reason="gateway is not running (docker compose up -d)")


@pytest.fixture(scope="module")
def api():
    with httpx.Client(base_url=GATEWAY, headers=KEY, timeout=120) as c:
        assert c.put("/buckets/api-test").status_code in (200, 201)
        yield c


@pytest.fixture
def key():
    return f"test/{uuid.uuid4().hex[:8]}.bin"


def upload(api, key, data, **headers):
    r = api.put(f"/buckets/api-test/objects/{key}", content=data, headers=headers)
    assert r.status_code == 201, r.text
    return r.json()


def test_health_is_public():
    assert httpx.get(f"{GATEWAY}/health").json()["status"] == "ok"


def test_requests_without_a_valid_api_key_are_rejected():
    assert httpx.get(f"{GATEWAY}/buckets").status_code == 401
    assert httpx.get(f"{GATEWAY}/buckets", headers={"X-API-Key": "wrong"}).status_code == 401


def test_create_and_list_buckets(api):
    assert api.put("/buckets/api-test-ec", params={"redundancy": "ec"}).json()["redundancy"] == "ec"
    names = {b["name"] for b in api.get("/buckets").json()}
    assert {"api-test", "api-test-ec"} <= names


def test_streamed_upload_then_download_returns_the_same_bytes(api, key):
    data = os.urandom(9 * MiB)

    def body():  # a generator makes httpx send the body in pieces (chunked transfer encoding)
        for i in range(0, len(data), 256 * 1024):
            yield data[i:i + 256 * 1024]

    r = upload(api, key, body(), **{"Content-Type": "application/x-test"})
    assert (r["size"], r["chunks"]) == (len(data), 3)
    assert r["sha256"] == hashlib.sha256(data).hexdigest()
    got = api.get(f"/buckets/api-test/objects/{key}")
    assert got.status_code == 200 and got.content == data
    assert got.headers["etag"] == f'"{r["sha256"]}"'
    assert got.headers["content-type"] == "application/x-test"
    api.delete(f"/buckets/api-test/objects/{key}")


@pytest.mark.parametrize("header, start, end", [
    ("bytes=0-99", 0, 99),
    ("bytes=4194290-4194309", 4194290, 4194309),  # crosses the boundary between chunk 0 and chunk 1
    ("bytes=-50", 5 * MiB - 50, 5 * MiB - 1),     # the last 50 bytes
    ("bytes=5242000-", 5242000, 5 * MiB - 1),    # from an offset to the end
])
def test_byte_range_requests(api, header, start, end):
    data = os.urandom(5 * MiB)
    key = f"test/range-{uuid.uuid4().hex[:8]}"
    upload(api, key, data)
    r = api.get(f"/buckets/api-test/objects/{key}", headers={"Range": header})
    assert r.status_code == 206
    assert r.headers["content-range"] == f"bytes {start}-{end}/{len(data)}"
    assert r.content == data[start:end + 1]
    api.delete(f"/buckets/api-test/objects/{key}")


def test_range_past_the_end_is_not_satisfiable(api, key):
    upload(api, key, b"tiny")
    r = api.get(f"/buckets/api-test/objects/{key}", headers={"Range": "bytes=100-200"})
    assert r.status_code == 416 and r.headers["content-range"] == "bytes */4"
    api.delete(f"/buckets/api-test/objects/{key}")


def test_head_and_metadata_describe_the_object(api, key):
    upload(api, key, b"hello metadata", **{"X-Tags": "type=note,owner=pytest"})
    h = api.head(f"/buckets/api-test/objects/{key}")
    assert h.status_code == 200 and h.headers["content-length"] == "14"
    assert h.headers["x-tags"] == "type=note,owner=pytest" and h.headers["x-object-version"] == "1"
    m = api.get(f"/buckets/api-test/objects/{key}", params={"metadata": "true"}).json()
    assert (m["size"], m["tags"], len(m["chunks"])) == (14, {"type": "note", "owner": "pytest"}, 1)
    assert len(m["chunks"][0]["stored_on"]) == 3
    api.delete(f"/buckets/api-test/objects/{key}")


def test_list_objects_by_prefix(api):
    prefix = f"list-{uuid.uuid4().hex[:6]}/"
    other = f"other-{uuid.uuid4().hex[:6]}.txt"
    for name in ("a.txt", "b.txt"):
        upload(api, prefix + name, name.encode())
    upload(api, other, b"x")
    keys = [o["key"] for o in api.get("/buckets/api-test/objects", params={"prefix": prefix}).json()]
    assert keys == [prefix + "a.txt", prefix + "b.txt"]
    for k in keys + [other]:
        api.delete(f"/buckets/api-test/objects/{k}")


def test_search_by_prefix_size_date_and_tag(api):
    run = uuid.uuid4().hex[:6]
    upload(api, f"search-{run}/small.txt", b"x" * 100, **{"X-Tags": f"run={run},kind=small"})
    upload(api, f"search-{run}/big.bin", os.urandom(200_000), **{"X-Tags": f"run={run},kind=big"})

    def find(**params):
        return sorted(o["key"].split("/")[1] for o in api.get("/search", params=params).json()["results"])

    assert find(prefix=f"search-{run}/") == ["big.bin", "small.txt"]
    assert find(prefix=f"search-{run}/", min_size=1000) == ["big.bin"]
    assert find(prefix=f"search-{run}/", max_size=1000) == ["small.txt"]
    assert find(tag=[f"run={run}", "kind=small"]) == ["small.txt"]
    today, tomorrow = date.today(), date.today() + timedelta(days=1)
    assert find(prefix=f"search-{run}/", after=str(today - timedelta(days=1))) == ["big.bin", "small.txt"]
    assert find(prefix=f"search-{run}/", after=str(tomorrow)) == []
    for name in ("small.txt", "big.bin"):
        api.delete(f"/buckets/api-test/objects/search-{run}/{name}")


def test_delete_then_get_returns_404(api, key):
    upload(api, key, b"short-lived")
    assert api.delete(f"/buckets/api-test/objects/{key}").json()["deleted_versions"] == 1
    assert api.get(f"/buckets/api-test/objects/{key}").status_code == 404


def test_upload_to_a_missing_bucket_is_404(api):
    assert api.put("/buckets/no-such-bucket/objects/x", content=b"x").status_code == 404


def test_cluster_status_reports_nodes_and_degraded_objects(api):
    s = api.get("/cluster/status").json()
    assert s["summary"]["nodes"] >= 4 and len(s["nodes"]) == s["summary"]["nodes"]
    assert {"free_bytes", "blobs_on_disk", "status"} <= set(s["nodes"][0])
    assert "degraded" in s["objects"]


def test_rate_limit_returns_429_with_retry_after():
    with httpx.Client(base_url=GATEWAY, headers=SLOW_KEY) as c:
        codes = [c.get("/buckets") for _ in range(30)]
    limited = [r for r in codes if r.status_code == 429]
    assert sum(r.status_code == 200 for r in codes) >= 10 and limited
    assert int(limited[0].headers["retry-after"]) >= 1
