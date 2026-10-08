#!/usr/bin/env bash
# Regenerate Python and Node Cache stubs from api/proto/cache.proto.
#
# Pins (do not float these without re-checking the committed stubs):
#   protoc        28.3   (libprotoc 28.3)
#   grpcio-tools  1.69.0
#   ts-proto      2.7.7  (clients/node devDependency, plugin protoc-gen-ts_proto)
#
# Does not touch api/gen (Go stubs stay on their own generators).
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

protoc_ver=$(protoc --version || true)
case "$protoc_ver" in
  "libprotoc 28.3") ;;
  *)
    echo "gen-clients: want protoc 28.3, got ${protoc_ver:-missing}" >&2
    exit 1
    ;;
esac

if ! python3 -c 'import grpc_tools' >/dev/null 2>&1; then
  echo "gen-clients: python module grpc_tools is not installed (grpcio-tools==1.69.0)" >&2
  exit 1
fi

plugin="$root/clients/node/node_modules/.bin/protoc-gen-ts_proto"
if [[ ! -x "$plugin" ]]; then
  echo "gen-clients: missing $plugin (run npm ci in clients/node)" >&2
  exit 1
fi

py_out="$root/clients/python/src/supercache/_gen"
node_out="$root/clients/node/src/gen"
mkdir -p "$py_out" "$node_out"
rm -f "$py_out"/cache_pb2.py "$py_out"/cache_pb2_grpc.py "$py_out"/cache_pb2.pyi \
  "$node_out"/cache.ts

python3 -m grpc_tools.protoc \
  -I api/proto \
  --python_out="$py_out" \
  --grpc_python_out="$py_out" \
  api/proto/cache.proto

# grpc_tools emits a top-level import. Make it package-relative.
python3 - "$py_out/cache_pb2_grpc.py" <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
old = "import cache_pb2 as cache__pb2"
new = "from . import cache_pb2 as cache__pb2"
if old not in text:
    sys.exit("gen-clients: expected grpc import to rewrite in cache_pb2_grpc.py")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
PY

if [[ ! -f "$py_out/__init__.py" ]]; then
  printf '"""Generated Cache stubs. Do not edit."""\n' > "$py_out/__init__.py"
fi

protoc \
  -I api/proto \
  --plugin=protoc-gen-ts_proto="$plugin" \
  --ts_proto_out="$node_out" \
  --ts_proto_opt=outputServices=grpc-js,esModuleInterop=true,env=node,useExactTypes=false,forceLong=number,outputIndex=false \
  api/proto/cache.proto

echo "gen-clients: wrote $py_out and $node_out"
