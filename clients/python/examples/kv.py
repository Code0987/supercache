"""Minimal CacheOnly put/get. Start supercache-node with -demo-keyspace first."""

import supercache

def main() -> None:
    client = supercache.dial("127.0.0.1:9000")
    try:
        client.put("cacheonly", "greeting", b"hello")
        print(client.get("cacheonly", "greeting").decode())
    finally:
        client.close()


if __name__ == "__main__":
    main()
