"""Client behavior against clients/conformance and a tiny local gRPC stub."""

from __future__ import annotations

import signal
import subprocess
import uuid
from concurrent import futures
from datetime import timedelta
from pathlib import Path

import grpc
import pytest

from supercache import (
    KeyErrors,
    NotFound,
    Session,
    StatusError,
    TLS,
    dial,
    dial_tls,
)
from supercache._gen import cache_pb2 as pb
from supercache._gen import cache_pb2_grpc as pb_grpc


def repo_root() -> Path:
    here = Path(__file__).resolve()
    for p in here.parents:
        if (p / "go.mod").exists():
            return p
    raise RuntimeError("go.mod not found")


class Proc:
    def __init__(self, proc: subprocess.Popen, meta: dict[str, str]) -> None:
        self.proc = proc
        self.meta = meta

    @property
    def addr(self) -> str:
        return self.meta["addr"]

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            self.proc.wait(timeout=15)


def _start(binary: Path, *args: str) -> Proc:
    proc = subprocess.Popen(
        [str(binary), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    meta: dict[str, str] = {}
    need = "client_key" if ("-tls" in args or "-mtls" in args) else "addr"
    while need not in meta:
        line = proc.stdout.readline()
        if not line:
            err = proc.stderr.read() if proc.stderr else ""
            raise RuntimeError(f"conformance exited before {need}: {err}")
        line = line.strip()
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        meta[k] = v
    return Proc(proc, meta)


@pytest.fixture(scope="module")
def servers():
    root = repo_root()
    binary = root / "clients" / "python" / ".venv" / "conformance"
    # The venv directory is gitignored. The binary is a test artifact next to it.
    binary.parent.mkdir(parents=True, exist_ok=True)
    subprocess.check_call(["go", "build", "-o", str(binary), "./clients/conformance"], cwd=root)
    plain = _start(binary)
    tls = _start(binary, "-tls")
    mtls = _start(binary, "-mtls")
    yield {"plain": plain, "tls": tls, "mtls": mtls}
    for proc in (plain, tls, mtls):
        proc.stop()


@pytest.fixture(scope="module")
def client(servers):
    c = dial(servers["plain"].addr)
    yield c
    c.close()


def nid() -> str:
    return uuid.uuid4().hex[:12]


def test_get_put_delete_and_not_found(client):
    key = "k-" + nid()
    with pytest.raises(NotFound) as missed:
        client.get("cacheonly", key)
    assert not isinstance(missed.value, StatusError)
    client.put("cacheonly", key, b"v")
    assert client.get("cacheonly", key) == b"v"
    client.delete("cacheonly", key)
    with pytest.raises(NotFound):
        client.get("cacheonly", key)


def test_ttl_omitted_and_zero(client):
    a, b = "ttl-a-" + nid(), "ttl-b-" + nid()
    client.put("cacheonly", a, b"a")
    client.put("cacheonly", b, b"b", ttl=timedelta(0))
    assert client.get("cacheonly", a) == b"a"
    assert client.get("cacheonly", b) == b"b"


def test_put_many_and_empty_key(client):
    a, b = "m-" + nid(), "n-" + nid()
    client.put_many("cacheonly", [(a, b"1"), (b, b"2")])
    assert client.get("cacheonly", a) == b"1"
    client.delete_many("cacheonly", [a, b])
    with pytest.raises(NotFound):
        client.get("cacheonly", a)
    with pytest.raises(KeyErrors) as exc:
        client.put_many("cacheonly", [("", b"x"), ("", b"y")])
    assert exc.value.errors
    assert all(item.key == "" for item in exc.value.errors)


def test_missing_keyspace_is_status_not_found_class(client):
    with pytest.raises(StatusError) as exc:
        client.get("nope", "k")
    assert exc.value.code == "NOT_FOUND"
    assert not isinstance(exc.value, NotFound)
    with pytest.raises(StatusError) as put_exc:
        client.put("nope", "k", b"v")
    assert put_exc.value.code == "NOT_FOUND"


def test_loadthrough_and_wrong_mode(client):
    assert client.get("loadthrough", "seeded") == b"from-source"
    with pytest.raises(NotFound):
        client.get("loadthrough", "missing")
    with pytest.raises(StatusError) as exc:
        client.get("set", "anything")
    assert exc.value.code == "INVALID_ARGUMENT"


def test_structures(client):
    name = nid()
    assert client.bloom_test("bloom", name, b"ghost") is False
    client.bloom_add("bloom", name, b"alice")
    assert client.bloom_test("bloom", name, b"alice") is True

    client.set_add("set", name, b"x")
    assert client.set_contains("set", name, b"x") is True
    assert client.set_card("set", name) == 1
    assert b"x" in client.set_members("set", name)
    client.set_remove("set", name, b"x")
    assert client.set_contains("set", name, b"x") is False

    client.zadd("zset", name, b"alice", 10)
    client.zadd("zset", name, b"bob", 20)
    assert client.zscore("zset", name, b"alice") == 10
    assert client.zcard("zset", name) == 2
    assert [m.member for m in client.zrange("zset", name, 0, -1)] == [b"alice", b"bob"]
    assert [m.member for m in client.zrange_by_score("zset", name, 10, 10)] == [b"alice"]
    client.zrem("zset", name, b"bob")
    assert client.zscore("zset", name, b"bob") is None

    client.geo_add("geo", name, b"a", -74.0, 40.7)
    client.geo_add("geo", name, b"b", -74.1, 40.8)
    assert client.geo_pos("geo", name, b"a") == (-74.0, 40.7)
    dist = client.geo_dist("geo", name, b"a", b"b")
    assert dist is not None and dist > 0
    assert client.geo_card("geo", name) == 2
    hits = client.geo_radius("geo", name, -74.0, 40.7, 50_000, 10)
    assert any(h.member == b"a" for h in hits)
    client.geo_rem("geo", name, b"b")
    assert client.geo_pos("geo", name, b"b") is None

    client.lpush("list", name, b"b")
    client.lpush("list", name, b"a")
    client.rpush("list", name, b"c")
    assert client.llen("list", name) == 3
    assert client.lindex("list", name, 0) == b"a"
    assert client.lrange("list", name, 0, -1) == [b"a", b"b", b"c"]
    assert client.lpop("list", name) == b"a"
    assert client.rpop("list", name) == b"c"

    client.hset("hash", name, "email", b"a@b")
    assert client.hget("hash", name, "email") == b"a@b"
    assert client.hexists("hash", name, "email") is True
    assert client.hlen("hash", name) == 1
    assert client.hgetall("hash", name)[0].field == b"email"
    client.hdel("hash", name, "email")
    assert client.hget("hash", name, "email") is None

    assert client.incr("counter", name, 2) == 2
    assert client.incr("counter", name, 3) == 5
    got = client.counter_get("counter", name)
    assert got.found and got.value == 5

    client.json_set("json", name, "$.name", b'"Ada"')
    assert client.json_get("json", name, "$.name") == b'"Ada"'
    client.json_del("json", name, "$.name")
    assert client.json_get("json", name, "$.name") is None

    client.bit_set("bitmap", name, 0, True)
    bit = client.bit_get("bitmap", name, 0)
    assert bit.found and bit.value is True
    assert client.bit_count("bitmap", name, 0, -1) >= 1
    pos = client.bit_pos("bitmap", name, True, 0, -1)
    assert pos.found and pos.pos == 0

    client.hll_add("hll", name, b"alice")
    hll = client.hll_count("hll", name)
    assert hll.found and hll.value >= 1

    client.topk_add("topk", name, b"t001")
    client.topk_add("topk", name, b"t001")
    top = client.topk_list("topk", name)
    assert top.found and any(e.item == b"t001" for e in top.entries)

    client.cms_incr("cms", name, b"t003", 4)
    cms = client.cms_query("cms", name, b"t003")
    assert cms.found and cms.value >= 4

    client.vadd("vectorset", name, b"east", [1, 0, 0, 0])
    dim = client.vdim("vectorset", name)
    assert dim.found and dim.value == 4
    card = client.vcard("vectorset", name)
    assert card.found and card.value == 1
    emb = client.vemb("vectorset", name, b"east")
    assert emb is not None and len(emb) == 4
    sim = client.vsim("vectorset", name, [1, 0, 0, 0], 1)
    assert sim and sim[0].member == b"east"
    client.vrem("vectorset", name, b"east")

    sid = client.xadd("stream", name, b"hello")
    assert sid
    length = client.xlen("stream", name)
    assert length.found and length.value >= 1
    rows = client.xrange("stream", name, "-", "+", 10)
    assert rows and rows[0].payload == b"hello"
    rev = client.xrevrange("stream", name, "-", "+", 10)
    assert rev and rev[0].id == sid
    client.xdel("stream", name, sid)
    client.xtrim("stream", name, 1)


def test_tls_and_mtls(servers):
    tls = servers["tls"]
    key = "tls-" + nid()
    c = dial_tls(tls.addr, tls.meta["ca"], "localhost")
    try:
        c.put("cacheonly", key, b"secret")
        assert c.get("cacheonly", key) == b"secret"
    finally:
        c.close()

    with pytest.raises(ValueError):
        dial_tls(tls.addr, tls.meta["ca"], "")

    mtls = servers["mtls"]
    secure = dial_tls(
        mtls.addr,
        mtls.meta["ca"],
        "localhost",
        mtls.meta["client_cert"],
        mtls.meta["client_key"],
    )
    try:
        key = "mtls-" + nid()
        secure.put("cacheonly", key, b"m")
        assert secure.get("cacheonly", key) == b"m"
    finally:
        secure.close()

    bare = dial_tls(mtls.addr, mtls.meta["ca"], "localhost")
    try:
        with pytest.raises(StatusError):
            bare.put("cacheonly", "nope", b"x")
    finally:
        bare.close()


def test_session_skips_closed_port(servers):
    live = servers["plain"].addr
    s = Session(["127.0.0.1:1", live], timeout=timedelta(seconds=2))
    try:
        with pytest.raises(NotFound):
            s.get("cacheonly", "missing-" + nid())
        assert s.connected_addr == live
    finally:
        s.close()


def test_session_retries_unavailable_once_and_not_notfound():
    class Once(pb_grpc.CacheServicer):
        def __init__(self) -> None:
            self.gets = 0

        def Get(self, request, context):
            self.gets += 1
            if request.key == "flaky" and self.gets == 1:
                context.abort(grpc.StatusCode.UNAVAILABLE, "down")
            if request.key == "miss":
                return pb.GetResponse(found=False)
            return pb.GetResponse(found=True, value=b"ok")

    svc = Once()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    pb_grpc.add_CacheServicer_to_server(svc, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    s = Session([f"127.0.0.1:{port}"])
    try:
        assert s.get("cacheonly", "flaky") == b"ok"
        assert svc.gets == 2
        before = svc.gets
        with pytest.raises(NotFound):
            s.get("cacheonly", "miss")
        assert svc.gets == before + 1
    finally:
        s.close()
        server.stop(0)


def test_close_is_idempotent(servers):
    c = dial(servers["plain"].addr)
    c.close()
    c.close()
