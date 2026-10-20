import { spawn, type ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import { createInterface } from "node:readline";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";

import * as grpc from "@grpc/grpc-js";

import { CacheService, GetResponse } from "../src/gen/cache.js";
import { dial, dialTls, KeyErrors, NotFound, Session, StatusError, type Client } from "../src/index.js";

function repoRoot(): string {
  let dir = path.dirname(fileURLToPath(import.meta.url));
  for (let i = 0; i < 8; i++) {
    if (existsSync(path.join(dir, "go.mod"))) return dir;
    dir = path.dirname(dir);
  }
  throw new Error("go.mod not found");
}

interface Proc {
  proc: ChildProcess;
  meta: Record<string, string>;
}

function nid(): string {
  return randomUUID().replace(/-/g, "").slice(0, 12);
}

function text(value: Uint8Array): string {
  return Buffer.from(value).toString("utf8");
}

function start(binary: string, args: string[] = []): Promise<Proc> {
  const proc = spawn(binary, args, { stdio: ["ignore", "pipe", "pipe"] });
  const meta: Record<string, string> = {};
  const need = args.includes("-tls") || args.includes("-mtls") ? "client_key" : "addr";
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`conformance timeout waiting for ${need}`)), 20_000);
    const rl = createInterface({ input: proc.stdout! });
    rl.on("line", (line) => {
      const eq = line.indexOf("=");
      if (eq < 0) return;
      meta[line.slice(0, eq)] = line.slice(eq + 1);
      if (meta[need]) {
        clearTimeout(timer);
        resolve({ proc, meta });
      }
    });
    proc.once("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`conformance exited ${code} before ${need}`));
    });
  });
}

function stop(proc: Proc | undefined): Promise<void> {
  if (!proc || proc.proc.exitCode != null) return Promise.resolve();
  return new Promise((resolve) => {
    proc.proc.once("exit", () => resolve());
    proc.proc.kill("SIGTERM");
  });
}

let plain: Proc;
let tls: Proc;
let mtls: Proc;
let client: Client;

before(async () => {
  const root = repoRoot();
  const binary = path.join(root, "clients", "node", "dist", "conformance");
  await new Promise<void>((resolve, reject) => {
    const build = spawn("go", ["build", "-o", binary, "./clients/conformance"], { cwd: root, stdio: "inherit" });
    build.on("exit", (code) => (code === 0 ? resolve() : reject(new Error(`go build exited ${code}`))));
  });
  plain = await start(binary);
  tls = await start(binary, ["-tls"]);
  mtls = await start(binary, ["-mtls"]);
  client = dial(plain.meta.addr!);
});

after(async () => {
  client?.close();
  await stop(plain);
  await stop(tls);
  await stop(mtls);
});

test("get put delete and NotFound", async () => {
  const key = "k-" + nid();
  await assert.rejects(() => client.get("cacheonly", key), NotFound);
  await client.put("cacheonly", key, "v");
  assert.equal(text(await client.get("cacheonly", key)), "v");
  await client.delete("cacheonly", key);
  await assert.rejects(() => client.get("cacheonly", key), NotFound);
});

test("ttl omitted and zero", async () => {
  const a = "ttl-a-" + nid();
  const b = "ttl-b-" + nid();
  await client.put("cacheonly", a, "a");
  await client.put("cacheonly", b, "b", { ttlMs: 0 });
  assert.equal(text(await client.get("cacheonly", a)), "a");
  assert.equal(text(await client.get("cacheonly", b)), "b");
});

test("putMany and empty key", async () => {
  const a = "m-" + nid();
  const b = "n-" + nid();
  await client.putMany("cacheonly", [
    { key: a, value: "1" },
    { key: b, value: "2" },
  ]);
  assert.equal(text(await client.get("cacheonly", a)), "1");
  await client.deleteMany("cacheonly", [a, b]);
  await assert.rejects(() => client.get("cacheonly", a), NotFound);
  await assert.rejects(
    () =>
      client.putMany("cacheonly", [
        { key: "", value: "x" },
        { key: "", value: "y" },
      ]),
    (err: unknown) => {
      assert.ok(err instanceof KeyErrors);
      assert.ok(err.errors.every((item) => item.key === ""));
      return true;
    },
  );
});

test("missing keyspace is status NOT_FOUND", async () => {
  await assert.rejects(() => client.get("nope", "k"), (err: unknown) => {
    assert.ok(err instanceof StatusError);
    assert.equal(err.code, "NOT_FOUND");
    assert.ok(!(err instanceof NotFound));
    return true;
  });
  await assert.rejects(() => client.put("nope", "k", "v"), (err: unknown) => {
    assert.ok(err instanceof StatusError);
    assert.equal(err.code, "NOT_FOUND");
    return true;
  });
});

test("loadthrough and wrong mode", async () => {
  assert.equal(text(await client.get("loadthrough", "seeded")), "from-source");
  await assert.rejects(() => client.get("loadthrough", "missing"), NotFound);
  await assert.rejects(() => client.get("set", "anything"), (err: unknown) => {
    assert.ok(err instanceof StatusError);
    assert.equal(err.code, "INVALID_ARGUMENT");
    return true;
  });
});

test("structures", async () => {
  const name = nid();
  assert.equal(await client.bloomTest("bloom", name, "ghost"), false);
  await client.bloomAdd("bloom", name, "alice");
  assert.equal(await client.bloomTest("bloom", name, "alice"), true);

  await client.setAdd("set", name, "x");
  assert.equal(await client.setContains("set", name, "x"), true);
  assert.equal(await client.setCard("set", name), 1);
  assert.ok((await client.setMembers("set", name)).some((m) => text(m) === "x"));
  await client.setRemove("set", name, "x");
  assert.equal(await client.setContains("set", name, "x"), false);

  await client.zAdd("zset", name, "alice", 10);
  await client.zAdd("zset", name, "bob", 20);
  assert.equal(await client.zScore("zset", name, "alice"), 10);
  assert.equal(await client.zCard("zset", name), 2);
  assert.deepEqual(
    (await client.zRange("zset", name, 0, -1)).map((m) => text(m.member)),
    ["alice", "bob"],
  );
  assert.deepEqual(
    (await client.zRangeByScore("zset", name, 10, 10)).map((m) => text(m.member)),
    ["alice"],
  );
  await client.zRem("zset", name, "bob");
  assert.equal(await client.zScore("zset", name, "bob"), null);

  await client.geoAdd("geo", name, "a", -74, 40.7);
  await client.geoAdd("geo", name, "b", -74.1, 40.8);
  assert.deepEqual(await client.geoPos("geo", name, "a"), [-74, 40.7]);
  const dist = await client.geoDist("geo", name, "a", "b");
  assert.ok(dist != null && dist > 0);
  assert.equal(await client.geoCard("geo", name), 2);
  const hits = await client.geoRadius("geo", name, -74, 40.7, 50_000, 10);
  assert.ok(hits.some((h) => text(h.member) === "a"));
  await client.geoRem("geo", name, "b");
  assert.equal(await client.geoPos("geo", name, "b"), null);

  await client.lPush("list", name, "b");
  await client.lPush("list", name, "a");
  await client.rPush("list", name, "c");
  assert.equal(await client.lLen("list", name), 3);
  assert.equal(text((await client.lIndex("list", name, 0))!), "a");
  assert.deepEqual(
    (await client.lRange("list", name, 0, -1)).map((item) => text(item)),
    ["a", "b", "c"],
  );
  assert.equal(text((await client.lPop("list", name))!), "a");
  assert.equal(text((await client.rPop("list", name))!), "c");

  await client.hSet("hash", name, "email", "a@b");
  assert.equal(text((await client.hGet("hash", name, "email"))!), "a@b");
  assert.equal(await client.hExists("hash", name, "email"), true);
  assert.equal(await client.hLen("hash", name), 1);
  assert.equal(text((await client.hGetAll("hash", name))[0]!.field), "email");
  await client.hDel("hash", name, "email");
  assert.equal(await client.hGet("hash", name, "email"), null);

  assert.equal(await client.incr("counter", name, 2), 2);
  assert.equal(await client.incr("counter", name, 3), 5);
  const counter = await client.counterGet("counter", name);
  assert.equal(counter.found, true);
  assert.equal(counter.value, 5);

  await client.jsonSet("json", name, "$.name", '"Ada"');
  assert.equal(text((await client.jsonGet("json", name, "$.name"))!), '"Ada"');
  await client.jsonDel("json", name, "$.name");
  assert.equal(await client.jsonGet("json", name, "$.name"), null);

  await client.bitSet("bitmap", name, 0, true);
  const bit = await client.bitGet("bitmap", name, 0);
  assert.equal(bit.found, true);
  assert.equal(bit.value, true);
  assert.ok((await client.bitCount("bitmap", name, 0, -1)) >= 1);
  const pos = await client.bitPos("bitmap", name, true, 0, -1);
  assert.equal(pos.found, true);
  assert.equal(pos.pos, 0);

  await client.hllAdd("hll", name, "alice");
  const hll = await client.hllCount("hll", name);
  assert.equal(hll.found, true);
  assert.ok(hll.value >= 1);

  await client.topKAdd("topk", name, "t001");
  await client.topKAdd("topk", name, "t001");
  const top = await client.topKList("topk", name);
  assert.equal(top.found, true);
  assert.ok(top.entries.some((e) => text(e.item) === "t001"));

  await client.cmsIncr("cms", name, "t003", 4);
  const cms = await client.cmsQuery("cms", name, "t003");
  assert.equal(cms.found, true);
  assert.ok(cms.value >= 4);

  await client.vAdd("vectorset", name, "east", [1, 0, 0, 0]);
  const dim = await client.vDim("vectorset", name);
  assert.equal(dim.found, true);
  assert.equal(dim.value, 4);
  const card = await client.vCard("vectorset", name);
  assert.equal(card.found, true);
  assert.equal(card.value, 1);
  const emb = await client.vEmb("vectorset", name, "east");
  assert.ok(emb && emb.length === 4);
  const sim = await client.vSim("vectorset", name, [1, 0, 0, 0], 1);
  assert.equal(text(sim[0]!.member), "east");
  await client.vRem("vectorset", name, "east");

  const sid = await client.xAdd("stream", name, "hello");
  assert.ok(sid);
  const length = await client.xLen("stream", name);
  assert.equal(length.found, true);
  assert.ok(length.value >= 1);
  const rows = await client.xRange("stream", name, "-", "+", 10);
  assert.equal(text(rows[0]!.payload), "hello");
  const rev = await client.xRevRange("stream", name, "-", "+", 10);
  assert.equal(rev[0]!.id, sid);
  await client.xDel("stream", name, sid);
  await client.xTrim("stream", name, 1);
});

test("tls and mtls", async () => {
  const key = "tls-" + nid();
  const secure = dialTls(tls.meta.addr!, {
    caFile: tls.meta.ca!,
    serverName: "localhost",
  });
  try {
    await secure.put("cacheonly", key, "secret");
    assert.equal(text(await secure.get("cacheonly", key)), "secret");
  } finally {
    secure.close();
  }
  assert.throws(() => dialTls(tls.meta.addr!, { caFile: tls.meta.ca!, serverName: "" }), TypeError);

  const mutual = dialTls(mtls.meta.addr!, {
    caFile: mtls.meta.ca!,
    serverName: "localhost",
    clientCert: mtls.meta.client_cert,
    clientKey: mtls.meta.client_key,
  });
  try {
    const mkey = "mtls-" + nid();
    await mutual.put("cacheonly", mkey, "m");
    assert.equal(text(await mutual.get("cacheonly", mkey)), "m");
  } finally {
    mutual.close();
  }

  const bare = dialTls(mtls.meta.addr!, { caFile: mtls.meta.ca!, serverName: "localhost" });
  try {
    await assert.rejects(() => bare.put("cacheonly", "nope", "x"), StatusError);
  } finally {
    bare.close();
  }
});

test("session skips a closed port", async () => {
  const live = plain.meta.addr!;
  const session = new Session(["127.0.0.1:1", live], { timeoutMs: 2000 });
  try {
    await assert.rejects(() => session.get("cacheonly", "missing-" + nid()), NotFound);
    assert.equal(session.connectedAddr(), live);
  } finally {
    session.close();
  }
});

test("session retries UNAVAILABLE once and does not retry NotFound", async () => {
  let gets = 0;
  const server = new grpc.Server();
  server.addService(CacheService, {
    get: (call: { request: { key: string } }, cb: (err: grpc.ServiceError | null, res: GetResponse | null) => void) => {
      gets++;
      if (call.request.key === "flaky" && gets === 1) {
        cb({ code: grpc.status.UNAVAILABLE, details: "down" } as grpc.ServiceError, null);
        return;
      }
      if (call.request.key === "miss") {
        cb(null, GetResponse.create({ found: false, value: Buffer.alloc(0) }));
        return;
      }
      cb(null, GetResponse.create({ found: true, value: Buffer.from("ok") }));
    },
  } as never);
  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, bound) => {
      if (err) reject(err);
      else resolve(bound);
    });
  });
  const session = new Session([`127.0.0.1:${port}`]);
  try {
    assert.equal(text(await session.get("cacheonly", "flaky")), "ok");
    assert.equal(gets, 2);
    const before = gets;
    await assert.rejects(() => session.get("cacheonly", "miss"), NotFound);
    assert.equal(gets, before + 1);
  } finally {
    session.close();
    server.forceShutdown();
  }
});
