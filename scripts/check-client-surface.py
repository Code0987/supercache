#!/usr/bin/env python3
"""Fail unless every Cache RPC has a Python method, a Node method, and an API.md mention.

The table is the facade contract from docs/design/2026-10-08-polyglot-clients.md.
A new rpc in api/proto/cache.proto fails until this table, both clients, and
docs/API.md all grow the method.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# (rpc, python method, node method)
SURFACE = [
    # TEMP: Get removed to prove clients-up-to-date fails. Revert this commit.
    ("Put", "put", "put"),
    ("PutMany", "put_many", "putMany"),
    ("Delete", "delete", "delete"),
    ("DeleteMany", "delete_many", "deleteMany"),
    ("BloomAdd", "bloom_add", "bloomAdd"),
    ("BloomTest", "bloom_test", "bloomTest"),
    ("SetAdd", "set_add", "setAdd"),
    ("SetRemove", "set_remove", "setRemove"),
    ("SetContains", "set_contains", "setContains"),
    ("SetCard", "set_card", "setCard"),
    ("SetMembers", "set_members", "setMembers"),
    ("ZAdd", "zadd", "zAdd"),
    ("ZRem", "zrem", "zRem"),
    ("ZScore", "zscore", "zScore"),
    ("ZCard", "zcard", "zCard"),
    ("ZRange", "zrange", "zRange"),
    ("ZRangeByScore", "zrange_by_score", "zRangeByScore"),
    ("GeoAdd", "geo_add", "geoAdd"),
    ("GeoRem", "geo_rem", "geoRem"),
    ("GeoPos", "geo_pos", "geoPos"),
    ("GeoCard", "geo_card", "geoCard"),
    ("GeoDist", "geo_dist", "geoDist"),
    ("GeoRadius", "geo_radius", "geoRadius"),
    ("LPush", "lpush", "lPush"),
    ("RPush", "rpush", "rPush"),
    ("LPop", "lpop", "lPop"),
    ("RPop", "rpop", "rPop"),
    ("LLen", "llen", "lLen"),
    ("LIndex", "lindex", "lIndex"),
    ("LRange", "lrange", "lRange"),
    ("HSet", "hset", "hSet"),
    ("HGet", "hget", "hGet"),
    ("HDel", "hdel", "hDel"),
    ("HExists", "hexists", "hExists"),
    ("HLen", "hlen", "hLen"),
    ("HGetAll", "hgetall", "hGetAll"),
    ("Incr", "incr", "incr"),
    ("CounterGet", "counter_get", "counterGet"),
    ("JsonSet", "json_set", "jsonSet"),
    ("JsonGet", "json_get", "jsonGet"),
    ("JsonDel", "json_del", "jsonDel"),
    ("BitSet", "bit_set", "bitSet"),
    ("BitGet", "bit_get", "bitGet"),
    ("BitCount", "bit_count", "bitCount"),
    ("BitPos", "bit_pos", "bitPos"),
    ("HLLAdd", "hll_add", "hllAdd"),
    ("HLLCount", "hll_count", "hllCount"),
    ("TopKAdd", "topk_add", "topKAdd"),
    ("TopKList", "topk_list", "topKList"),
    ("CMSIncr", "cms_incr", "cmsIncr"),
    ("CMSQuery", "cms_query", "cmsQuery"),
    ("VAdd", "vadd", "vAdd"),
    ("VRem", "vrem", "vRem"),
    ("VSim", "vsim", "vSim"),
    ("VCard", "vcard", "vCard"),
    ("VDim", "vdim", "vDim"),
    ("VEmb", "vemb", "vEmb"),
    ("XAdd", "xadd", "xAdd"),
    ("XRange", "xrange", "xRange"),
    ("XRevRange", "xrevrange", "xRevRange"),
    ("XLen", "xlen", "xLen"),
    ("XDel", "xdel", "xDel"),
    ("XTrim", "xtrim", "xTrim"),
]


def repo_root() -> Path:
    here = Path(__file__).resolve()
    for p in here.parents:
        if (p / "go.mod").exists() and (p / "api" / "proto" / "cache.proto").exists():
            return p
    sys.exit("check-client-surface: repo root not found")


def proto_rpcs(proto: Path) -> list[str]:
    text = proto.read_text(encoding="utf-8")
    service = re.search(r"service Cache \{(?P<body>.*?)\n\}", text, re.S)
    if not service:
        sys.exit("check-client-surface: service Cache not found")
    return re.findall(r"^\s*rpc\s+([A-Za-z0-9_]+)\s*\(", service.group("body"), re.M)


def python_sources(root: Path) -> str:
    base = root / "clients" / "python" / "src" / "supercache"
    parts = []
    for path in sorted(base.rglob("*.py")):
        if "_gen" in path.parts:
            continue
        parts.append(path.read_text(encoding="utf-8"))
    return "\n".join(parts)


def node_sources(root: Path) -> str:
    base = root / "clients" / "node" / "src"
    parts = []
    for path in sorted(base.rglob("*.ts")):
        if "gen" in path.parts or path.name.endswith(".test.ts"):
            continue
        parts.append(path.read_text(encoding="utf-8"))
    return "\n".join(parts)


def has_python(src: str, name: str) -> bool:
    return re.search(rf"^\s*def {re.escape(name)}\s*\(", src, re.M) is not None


def has_node(src: str, name: str) -> bool:
    # A real method, or a CacheApi signature (name followed by '(').
    return re.search(rf"\b{re.escape(name)}\s*\(", src) is not None


def main() -> int:
    root = repo_root()
    proto = proto_rpcs(root / "api" / "proto" / "cache.proto")
    table = [row[0] for row in SURFACE]
    errors = []
    if proto != table:
        missing = [name for name in proto if name not in table]
        extra = [name for name in table if name not in proto]
        if missing:
            errors.append("proto RPCs missing from SURFACE: " + ", ".join(missing))
        if extra:
            errors.append("SURFACE RPCs not in cache.proto: " + ", ".join(extra))
        if not missing and not extra:
            errors.append("SURFACE order does not match service Cache")
    py = python_sources(root)
    node = node_sources(root)
    api = (root / "docs" / "API.md").read_text(encoding="utf-8")
    if not py.strip():
        errors.append("Python facade sources are empty")
    if not node.strip():
        errors.append("Node facade sources are empty")
    for rpc, py_name, node_name in SURFACE:
        if py.strip() and not has_python(py, py_name):
            errors.append(f"Python facade missing def {py_name} ({rpc})")
        if node.strip() and not has_node(node, node_name):
            errors.append(f"Node facade missing {node_name} ({rpc})")
        if not re.search(rf"\b{re.escape(rpc)}\b", api):
            errors.append(f"docs/API.md does not mention {rpc}")
    if errors:
        print("client surface is behind cache.proto:", file=sys.stderr)
        for err in errors:
            print("  " + err, file=sys.stderr)
        return 1
    print(f"client surface covers {len(proto)} Cache RPCs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
