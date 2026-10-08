/**
 * Async Cache client. Semantics match pkg/client in the Go module.
 * Dial the cache port, not the peer port.
 *
 * Return types live in ./types.js. This file is the call path only.
 */

import { readFileSync } from "node:fs";

import {
  type ChannelCredentials,
  Client as GrpcClient,
  credentials,
  Metadata,
  status,
  type ServiceError,
} from "@grpc/grpc-js";

import * as pb from "./gen/cache.js";
import { KeyErrors, NotFound, PeerFailures, StatusError, type KeyErrorItem, type PeerFailure } from "./errors.js";
import type {
  BitPos,
  BitValue,
  Bytes,
  GeoMember,
  HashField,
  Present,
  PutOptions,
  StreamEntry,
  TlsOptions,
  TopKResult,
  VSimHit,
  ZMember,
} from "./types.js";

function toBytes(value: Bytes, what: string): Buffer {
  if (typeof value === "string") return Buffer.from(value, "utf8");
  if (Buffer.isBuffer(value)) return value;
  if (value instanceof Uint8Array) return Buffer.from(value);
  throw new TypeError(`${what} must be a string or Uint8Array`);
}

function bytesOut(value: Uint8Array | undefined): Uint8Array {
  return value ? Uint8Array.from(value) : new Uint8Array();
}

function vec(value: readonly number[]): number[] {
  if (typeof value === "string" || value instanceof Uint8Array) {
    throw new TypeError("vector must be a sequence of numbers");
  }
  return Array.from(value, (n) => {
    if (typeof n !== "number" || !Number.isFinite(n)) throw new TypeError("vector must be finite numbers");
    return n;
  });
}

function ttlFields(ttlMs: number | undefined): { ttlNanos: number; ttlSet: boolean } {
  if (ttlMs === undefined) return { ttlNanos: 0, ttlSet: false };
  if (!Number.isFinite(ttlMs) || ttlMs < 0) throw new TypeError("ttlMs must be >= 0");
  return { ttlNanos: Math.round(ttlMs * 1_000_000), ttlSet: true };
}

function statusFrom(err: ServiceError): StatusError {
  const name = status[err.code];
  const code = typeof name === "string" ? name : String(err.code);
  return new StatusError(code, err.details || err.message);
}

function keyErrors(errors: { key: string; message: string; peerFailures?: { peerId: string; message: string }[] }[]): void {
  if (!errors || errors.length === 0) return;
  const items: KeyErrorItem[] = errors.map((e) => ({
    key: e.key,
    message: e.message,
    peerFailures: (e.peerFailures ?? []).map((p) => ({ peerId: p.peerId, message: p.message })),
  }));
  throw new KeyErrors(items);
}

function peerFailures(failures: { peerId: string; message: string }[] | undefined): void {
  if (!failures || failures.length === 0) return;
  const items: PeerFailure[] = failures.map((p) => ({ peerId: p.peerId, message: p.message }));
  throw new PeerFailures(items);
}

export class Client {
  private closed = false;

  constructor(
    readonly raw: InstanceType<typeof pb.CacheClient>,
    readonly timeoutMs?: number,
  ) {}

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.raw.close();
  }

  ready(timeoutMs: number): Promise<void> {
    return new Promise((resolve, reject) => {
      (this.raw as GrpcClient).waitForReady(new Date(Date.now() + timeoutMs), (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  private rpc<T>(method: string, request: object, timeoutMs?: number): Promise<T> {
    const ms = timeoutMs ?? this.timeoutMs;
    return new Promise((resolve, reject) => {
      const cb = (err: ServiceError | null, res: T) => {
        if (err) reject(statusFrom(err));
        else resolve(res);
      };
      const fn = (this.raw as unknown as Record<string, (...args: unknown[]) => void>)[method];
      if (ms == null) {
        fn.call(this.raw, request, cb);
        return;
      }
      fn.call(this.raw, request, new Metadata(), { deadline: new Date(Date.now() + ms) }, cb);
    });
  }

  get(keyspace: string, key: string, timeoutMs?: number): Promise<Uint8Array> {
    return this.rpc<pb.GetResponse>("get", pb.GetRequest.fromPartial({ keyspace, key }), timeoutMs).then((resp) => {
      if (!resp.found) throw new NotFound(`${keyspace}/${key}`);
      return bytesOut(resp.value);
    });
  }

  put(keyspace: string, key: string, value: Bytes, opts?: PutOptions): Promise<void> {
    const ttl = ttlFields(opts?.ttlMs);
    return this.rpc(
      "put",
      pb.PutRequest.fromPartial({ keyspace, key, value: toBytes(value, "value"), ttlNanos: ttl.ttlNanos, ttlSet: ttl.ttlSet }),
      opts?.timeoutMs,
    ).then(() => undefined);
  }

  putMany(keyspace: string, items: ReadonlyArray<{ key: string; value: Bytes }>, opts?: PutOptions): Promise<void> {
    const ttl = ttlFields(opts?.ttlMs);
    return this.rpc<pb.PutManyResponse>(
      "putMany",
      pb.PutManyRequest.fromPartial({
        keyspace,
        items: items.map((it) => ({ key: it.key, value: toBytes(it.value, "value") })),
        ttlNanos: ttl.ttlNanos,
        ttlSet: ttl.ttlSet,
      }),
      opts?.timeoutMs,
    ).then((resp) => keyErrors(resp.errors));
  }

  delete(keyspace: string, key: string, timeoutMs?: number): Promise<void> {
    return this.rpc<pb.DeleteResponse>("delete", pb.DeleteRequest.fromPartial({ keyspace, key }), timeoutMs).then((resp) => {
      peerFailures(resp.peerFailures);
    });
  }

  deleteMany(keyspace: string, keys: readonly string[], timeoutMs?: number): Promise<void> {
    return this.rpc<pb.DeleteManyResponse>(
      "deleteMany",
      pb.DeleteManyRequest.fromPartial({ keyspace, keys: [...keys] }),
      timeoutMs,
    ).then((resp) => keyErrors(resp.errors));
  }

  bloomAdd(keyspace: string, name: string, item: Bytes, timeoutMs?: number): Promise<void> {
    return this.rpc("bloomAdd", pb.BloomAddRequest.fromPartial({ keyspace, name, item: toBytes(item, "item") }), timeoutMs).then(
      () => undefined,
    );
  }

  bloomTest(keyspace: string, name: string, item: Bytes, timeoutMs?: number): Promise<boolean> {
    return this.rpc<pb.BloomTestResponse>(
      "bloomTest",
      pb.BloomTestRequest.fromPartial({ keyspace, name, item: toBytes(item, "item") }),
      timeoutMs,
    ).then((resp) => resp.maybe);
  }

  setAdd(keyspace: string, name: string, item: Bytes, timeoutMs?: number): Promise<void> {
    return this.rpc("setAdd", pb.SetAddRequest.fromPartial({ keyspace, name, item: toBytes(item, "item") }), timeoutMs).then(
      () => undefined,
    );
  }

  setRemove(keyspace: string, name: string, item: Bytes, timeoutMs?: number): Promise<void> {
    return this.rpc(
      "setRemove",
      pb.SetRemoveRequest.fromPartial({ keyspace, name, item: toBytes(item, "item") }),
      timeoutMs,
    ).then(() => undefined);
  }

  setContains(keyspace: string, name: string, item: Bytes, timeoutMs?: number): Promise<boolean> {
    return this.rpc<pb.SetContainsResponse>(
      "setContains",
      pb.SetContainsRequest.fromPartial({ keyspace, name, item: toBytes(item, "item") }),
      timeoutMs,
    ).then((resp) => resp.present);
  }

  setCard(keyspace: string, name: string, timeoutMs?: number): Promise<number> {
    return this.rpc<pb.SetCardResponse>("setCard", pb.SetCardRequest.fromPartial({ keyspace, name }), timeoutMs).then(
      (resp) => resp.card,
    );
  }

  setMembers(keyspace: string, name: string, timeoutMs?: number): Promise<Uint8Array[]> {
    return this.rpc<pb.SetMembersResponse>("setMembers", pb.SetMembersRequest.fromPartial({ keyspace, name }), timeoutMs).then(
      (resp) => resp.members.map((m) => bytesOut(m)),
    );
  }

  zAdd(keyspace: string, name: string, member: Bytes, score: number, timeoutMs?: number): Promise<void> {
    return this.rpc(
      "zAdd",
      pb.ZAddRequest.fromPartial({ keyspace, name, member: toBytes(member, "member"), score }),
      timeoutMs,
    ).then(() => undefined);
  }

  zRem(keyspace: string, name: string, member: Bytes, timeoutMs?: number): Promise<void> {
    return this.rpc("zRem", pb.ZRemRequest.fromPartial({ keyspace, name, member: toBytes(member, "member") }), timeoutMs).then(
      () => undefined,
    );
  }

  zScore(keyspace: string, name: string, member: Bytes, timeoutMs?: number): Promise<number | null> {
    return this.rpc<pb.ZScoreResponse>(
      "zScore",
      pb.ZScoreRequest.fromPartial({ keyspace, name, member: toBytes(member, "member") }),
      timeoutMs,
    ).then((resp) => (resp.present ? resp.score : null));
  }

  zCard(keyspace: string, name: string, timeoutMs?: number): Promise<number> {
    return this.rpc<pb.ZCardResponse>("zCard", pb.ZCardRequest.fromPartial({ keyspace, name }), timeoutMs).then((resp) => resp.card);
  }

  zRange(keyspace: string, name: string, start: number, stop: number, timeoutMs?: number): Promise<ZMember[]> {
    return this.rpc<pb.ZRangeResponse>(
      "zRange",
      pb.ZRangeRequest.fromPartial({ keyspace, name, start, stop }),
      timeoutMs,
    ).then((resp) => resp.members.map((m) => ({ member: bytesOut(m.member), score: m.score })));
  }

  zRangeByScore(keyspace: string, name: string, min: number, max: number, timeoutMs?: number): Promise<ZMember[]> {
    return this.rpc<pb.ZRangeResponse>(
      "zRangeByScore",
      pb.ZRangeByScoreRequest.fromPartial({ keyspace, name, min, max }),
      timeoutMs,
    ).then((resp) => resp.members.map((m) => ({ member: bytesOut(m.member), score: m.score })));
  }

  geoAdd(keyspace: string, name: string, member: Bytes, lon: number, lat: number, timeoutMs?: number): Promise<void> {
    return this.rpc(
      "geoAdd",
      pb.GeoAddRequest.fromPartial({ keyspace, name, member: toBytes(member, "member"), lon, lat }),
      timeoutMs,
    ).then(() => undefined);
  }

  geoRem(keyspace: string, name: string, member: Bytes, timeoutMs?: number): Promise<void> {
    return this.rpc(
      "geoRem",
      pb.GeoRemRequest.fromPartial({ keyspace, name, member: toBytes(member, "member") }),
      timeoutMs,
    ).then(() => undefined);
  }

  geoPos(keyspace: string, name: string, member: Bytes, timeoutMs?: number): Promise<[number, number] | null> {
    return this.rpc<pb.GeoPosResponse>(
      "geoPos",
      pb.GeoPosRequest.fromPartial({ keyspace, name, member: toBytes(member, "member") }),
      timeoutMs,
    ).then((resp) => (resp.present ? [resp.lon, resp.lat] : null));
  }

  geoCard(keyspace: string, name: string, timeoutMs?: number): Promise<number> {
    return this.rpc<pb.GeoCardResponse>("geoCard", pb.GeoCardRequest.fromPartial({ keyspace, name }), timeoutMs).then(
      (resp) => resp.card,
    );
  }

  geoDist(keyspace: string, name: string, a: Bytes, b: Bytes, timeoutMs?: number): Promise<number | null> {
    return this.rpc<pb.GeoDistResponse>(
      "geoDist",
      pb.GeoDistRequest.fromPartial({ keyspace, name, a: toBytes(a, "a"), b: toBytes(b, "b") }),
      timeoutMs,
    ).then((resp) => (resp.present ? resp.meters : null));
  }

  geoRadius(
    keyspace: string,
    name: string,
    lon: number,
    lat: number,
    radiusM: number,
    limit: number,
    timeoutMs?: number,
  ): Promise<GeoMember[]> {
    return this.rpc<pb.GeoRadiusResponse>(
      "geoRadius",
      pb.GeoRadiusRequest.fromPartial({ keyspace, name, lon, lat, radiusMeters: radiusM, limit }),
      timeoutMs,
    ).then((resp) =>
      resp.members.map((m) => ({ member: bytesOut(m.member), lon: m.lon, lat: m.lat, dist: m.distMeters })),
    );
  }

  lPush(keyspace: string, name: string, item: Bytes, timeoutMs?: number): Promise<void> {
    return this.rpc("lPush", pb.LPushRequest.fromPartial({ keyspace, name, item: toBytes(item, "item") }), timeoutMs).then(
      () => undefined,
    );
  }

  rPush(keyspace: string, name: string, item: Bytes, timeoutMs?: number): Promise<void> {
    return this.rpc("rPush", pb.RPushRequest.fromPartial({ keyspace, name, item: toBytes(item, "item") }), timeoutMs).then(
      () => undefined,
    );
  }

  lPop(keyspace: string, name: string, timeoutMs?: number): Promise<Uint8Array | null> {
    return this.rpc<pb.LPopResponse>("lPop", pb.LPopRequest.fromPartial({ keyspace, name }), timeoutMs).then((resp) =>
      resp.present ? bytesOut(resp.item) : null,
    );
  }

  rPop(keyspace: string, name: string, timeoutMs?: number): Promise<Uint8Array | null> {
    return this.rpc<pb.RPopResponse>("rPop", pb.RPopRequest.fromPartial({ keyspace, name }), timeoutMs).then((resp) =>
      resp.present ? bytesOut(resp.item) : null,
    );
  }

  lLen(keyspace: string, name: string, timeoutMs?: number): Promise<number> {
    return this.rpc<pb.LLenResponse>("lLen", pb.LLenRequest.fromPartial({ keyspace, name }), timeoutMs).then((resp) => resp.len);
  }

  lIndex(keyspace: string, name: string, index: number, timeoutMs?: number): Promise<Uint8Array | null> {
    return this.rpc<pb.LIndexResponse>("lIndex", pb.LIndexRequest.fromPartial({ keyspace, name, index }), timeoutMs).then(
      (resp) => (resp.present ? bytesOut(resp.item) : null),
    );
  }

  lRange(keyspace: string, name: string, start: number, stop: number, timeoutMs?: number): Promise<Uint8Array[]> {
    return this.rpc<pb.LRangeResponse>("lRange", pb.LRangeRequest.fromPartial({ keyspace, name, start, stop }), timeoutMs).then(
      (resp) => resp.items.map((item) => bytesOut(item)),
    );
  }

  hSet(keyspace: string, name: string, field: Bytes, value: Bytes, timeoutMs?: number): Promise<void> {
    return this.rpc(
      "hSet",
      pb.HSetRequest.fromPartial({ keyspace, name, field: toBytes(field, "field"), value: toBytes(value, "value") }),
      timeoutMs,
    ).then(() => undefined);
  }

  hGet(keyspace: string, name: string, field: Bytes, timeoutMs?: number): Promise<Uint8Array | null> {
    return this.rpc<pb.HGetResponse>(
      "hGet",
      pb.HGetRequest.fromPartial({ keyspace, name, field: toBytes(field, "field") }),
      timeoutMs,
    ).then((resp) => (resp.present ? bytesOut(resp.value) : null));
  }

  hDel(keyspace: string, name: string, field: Bytes, timeoutMs?: number): Promise<void> {
    return this.rpc("hDel", pb.HDelRequest.fromPartial({ keyspace, name, field: toBytes(field, "field") }), timeoutMs).then(
      () => undefined,
    );
  }

  hExists(keyspace: string, name: string, field: Bytes, timeoutMs?: number): Promise<boolean> {
    return this.rpc<pb.HExistsResponse>(
      "hExists",
      pb.HExistsRequest.fromPartial({ keyspace, name, field: toBytes(field, "field") }),
      timeoutMs,
    ).then((resp) => resp.present);
  }

  hLen(keyspace: string, name: string, timeoutMs?: number): Promise<number> {
    return this.rpc<pb.HLenResponse>("hLen", pb.HLenRequest.fromPartial({ keyspace, name }), timeoutMs).then((resp) => resp.len);
  }

  hGetAll(keyspace: string, name: string, timeoutMs?: number): Promise<HashField[]> {
    return this.rpc<pb.HGetAllResponse>("hGetAll", pb.HGetAllRequest.fromPartial({ keyspace, name }), timeoutMs).then((resp) =>
      resp.fields.map((f) => ({ field: bytesOut(f.field), value: bytesOut(f.value) })),
    );
  }

  incr(keyspace: string, name: string, delta: number, timeoutMs?: number): Promise<number> {
    return this.rpc<pb.IncrResponse>("incr", pb.IncrRequest.fromPartial({ keyspace, name, delta }), timeoutMs).then(
      (resp) => resp.value,
    );
  }

  counterGet(keyspace: string, name: string, timeoutMs?: number): Promise<Present> {
    return this.rpc<pb.CounterGetResponse>("counterGet", pb.CounterGetRequest.fromPartial({ keyspace, name }), timeoutMs).then(
      (resp) => ({ value: resp.value, found: resp.present }),
    );
  }

  jsonSet(keyspace: string, name: string, path: string, value: Bytes, timeoutMs?: number): Promise<void> {
    return this.rpc(
      "jsonSet",
      pb.JsonSetRequest.fromPartial({ keyspace, name, path, value: toBytes(value, "value") }),
      timeoutMs,
    ).then(() => undefined);
  }

  jsonGet(keyspace: string, name: string, path: string, timeoutMs?: number): Promise<Uint8Array | null> {
    return this.rpc<pb.JsonGetResponse>("jsonGet", pb.JsonGetRequest.fromPartial({ keyspace, name, path }), timeoutMs).then(
      (resp) => (resp.present ? bytesOut(resp.value) : null),
    );
  }

  jsonDel(keyspace: string, name: string, path: string, timeoutMs?: number): Promise<void> {
    return this.rpc("jsonDel", pb.JsonDelRequest.fromPartial({ keyspace, name, path }), timeoutMs).then(() => undefined);
  }

  bitSet(keyspace: string, name: string, offset: number, bit: boolean, timeoutMs?: number): Promise<void> {
    return this.rpc("bitSet", pb.BitSetRequest.fromPartial({ keyspace, name, offset, bit }), timeoutMs).then(() => undefined);
  }

  bitGet(keyspace: string, name: string, offset: number, timeoutMs?: number): Promise<BitValue> {
    return this.rpc<pb.BitGetResponse>("bitGet", pb.BitGetRequest.fromPartial({ keyspace, name, offset }), timeoutMs).then(
      (resp) => ({ value: resp.bit, found: resp.present }),
    );
  }

  bitCount(keyspace: string, name: string, start: number, end: number, timeoutMs?: number): Promise<number> {
    return this.rpc<pb.BitCountResponse>(
      "bitCount",
      pb.BitCountRequest.fromPartial({ keyspace, name, start, end }),
      timeoutMs,
    ).then((resp) => resp.count);
  }

  bitPos(keyspace: string, name: string, bit: boolean, start: number, end: number, timeoutMs?: number): Promise<BitPos> {
    return this.rpc<pb.BitPosResponse>(
      "bitPos",
      pb.BitPosRequest.fromPartial({ keyspace, name, bit, start, end }),
      timeoutMs,
    ).then((resp) => ({ pos: resp.pos, found: resp.found }));
  }

  hllAdd(keyspace: string, name: string, item: Bytes, timeoutMs?: number): Promise<void> {
    return this.rpc("hllAdd", pb.HLLAddRequest.fromPartial({ keyspace, name, item: toBytes(item, "item") }), timeoutMs).then(
      () => undefined,
    );
  }

  hllCount(keyspace: string, name: string, timeoutMs?: number): Promise<Present> {
    return this.rpc<pb.HLLCountResponse>("hllCount", pb.HLLCountRequest.fromPartial({ keyspace, name }), timeoutMs).then(
      (resp) => ({ value: resp.count, found: resp.present }),
    );
  }

  topKAdd(keyspace: string, name: string, item: Bytes, timeoutMs?: number): Promise<void> {
    return this.rpc("topKAdd", pb.TopKAddRequest.fromPartial({ keyspace, name, item: toBytes(item, "item") }), timeoutMs).then(
      () => undefined,
    );
  }

  topKList(keyspace: string, name: string, timeoutMs?: number): Promise<TopKResult> {
    return this.rpc<pb.TopKListResponse>("topKList", pb.TopKListRequest.fromPartial({ keyspace, name }), timeoutMs).then(
      (resp) => ({
        found: resp.present,
        entries: resp.entries.map((e) => ({ item: bytesOut(e.item), count: e.count })),
      }),
    );
  }

  cmsIncr(keyspace: string, name: string, item: Bytes, n: number, timeoutMs?: number): Promise<void> {
    return this.rpc(
      "cmsIncr",
      pb.CMSIncrRequest.fromPartial({ keyspace, name, item: toBytes(item, "item"), n }),
      timeoutMs,
    ).then(() => undefined);
  }

  cmsQuery(keyspace: string, name: string, item: Bytes, timeoutMs?: number): Promise<Present> {
    return this.rpc<pb.CMSQueryResponse>(
      "cmsQuery",
      pb.CMSQueryRequest.fromPartial({ keyspace, name, item: toBytes(item, "item") }),
      timeoutMs,
    ).then((resp) => ({ value: resp.count, found: resp.present }));
  }

  vAdd(keyspace: string, name: string, member: Bytes, vector: readonly number[], timeoutMs?: number): Promise<void> {
    return this.rpc(
      "vAdd",
      pb.VAddRequest.fromPartial({ keyspace, name, member: toBytes(member, "member"), vec: vec(vector) }),
      timeoutMs,
    ).then(() => undefined);
  }

  vRem(keyspace: string, name: string, member: Bytes, timeoutMs?: number): Promise<void> {
    return this.rpc("vRem", pb.VRemRequest.fromPartial({ keyspace, name, member: toBytes(member, "member") }), timeoutMs).then(
      () => undefined,
    );
  }

  vSim(keyspace: string, name: string, vector: readonly number[], k: number, timeoutMs?: number): Promise<VSimHit[]> {
    return this.rpc<pb.VSimResponse>(
      "vSim",
      pb.VSimRequest.fromPartial({ keyspace, name, vec: vec(vector), k }),
      timeoutMs,
    ).then((resp) => resp.hits.map((h) => ({ member: bytesOut(h.member), score: h.score })));
  }

  vCard(keyspace: string, name: string, timeoutMs?: number): Promise<Present> {
    return this.rpc<pb.VCardResponse>("vCard", pb.VCardRequest.fromPartial({ keyspace, name }), timeoutMs).then((resp) => ({
      value: resp.n,
      found: resp.present,
    }));
  }

  vDim(keyspace: string, name: string, timeoutMs?: number): Promise<Present> {
    return this.rpc<pb.VDimResponse>("vDim", pb.VDimRequest.fromPartial({ keyspace, name }), timeoutMs).then((resp) => ({
      value: resp.dim,
      found: resp.present,
    }));
  }

  vEmb(keyspace: string, name: string, member: Bytes, timeoutMs?: number): Promise<number[] | null> {
    return this.rpc<pb.VEmbResponse>(
      "vEmb",
      pb.VEmbRequest.fromPartial({ keyspace, name, member: toBytes(member, "member") }),
      timeoutMs,
    ).then((resp) => (resp.found ? [...resp.vec] : null));
  }

  xAdd(keyspace: string, name: string, payload: Bytes, timeoutMs?: number): Promise<string> {
    return this.rpc<pb.XAddResponse>(
      "xAdd",
      pb.XAddRequest.fromPartial({ keyspace, name, payload: toBytes(payload, "payload") }),
      timeoutMs,
    ).then((resp) => resp.id);
  }

  xRange(keyspace: string, name: string, start: string, end: string, count: number, timeoutMs?: number): Promise<StreamEntry[]> {
    return this.rpc<pb.XRangeResponse>(
      "xRange",
      pb.XRangeRequest.fromPartial({ keyspace, name, start, end, count }),
      timeoutMs,
    ).then((resp) => resp.entries.map((e) => ({ id: e.id, payload: bytesOut(e.payload) })));
  }

  xRevRange(
    keyspace: string,
    name: string,
    start: string,
    end: string,
    count: number,
    timeoutMs?: number,
  ): Promise<StreamEntry[]> {
    return this.rpc<pb.XRangeResponse>(
      "xRevRange",
      pb.XRevRangeRequest.fromPartial({ keyspace, name, start, end, count }),
      timeoutMs,
    ).then((resp) => resp.entries.map((e) => ({ id: e.id, payload: bytesOut(e.payload) })));
  }

  xLen(keyspace: string, name: string, timeoutMs?: number): Promise<Present> {
    return this.rpc<pb.XLenResponse>("xLen", pb.XLenRequest.fromPartial({ keyspace, name }), timeoutMs).then((resp) => ({
      value: resp.n,
      found: resp.present,
    }));
  }

  xDel(keyspace: string, name: string, id: string, timeoutMs?: number): Promise<void> {
    return this.rpc("xDel", pb.XDelRequest.fromPartial({ keyspace, name, id }), timeoutMs).then(() => undefined);
  }

  xTrim(keyspace: string, name: string, maxLen: number, timeoutMs?: number): Promise<void> {
    return this.rpc("xTrim", pb.XTrimRequest.fromPartial({ keyspace, name, maxLen }), timeoutMs).then(() => undefined);
  }
}

/** Public method names. Session forwards each of these with one transport retry. */
export const clientMethods = [
  "get",
  "put",
  "putMany",
  "delete",
  "deleteMany",
  "bloomAdd",
  "bloomTest",
  "setAdd",
  "setRemove",
  "setContains",
  "setCard",
  "setMembers",
  "zAdd",
  "zRem",
  "zScore",
  "zCard",
  "zRange",
  "zRangeByScore",
  "geoAdd",
  "geoRem",
  "geoPos",
  "geoCard",
  "geoDist",
  "geoRadius",
  "lPush",
  "rPush",
  "lPop",
  "rPop",
  "lLen",
  "lIndex",
  "lRange",
  "hSet",
  "hGet",
  "hDel",
  "hExists",
  "hLen",
  "hGetAll",
  "incr",
  "counterGet",
  "jsonSet",
  "jsonGet",
  "jsonDel",
  "bitSet",
  "bitGet",
  "bitCount",
  "bitPos",
  "hllAdd",
  "hllCount",
  "topKAdd",
  "topKList",
  "cmsIncr",
  "cmsQuery",
  "vAdd",
  "vRem",
  "vSim",
  "vCard",
  "vDim",
  "vEmb",
  "xAdd",
  "xRange",
  "xRevRange",
  "xLen",
  "xDel",
  "xTrim",
] as const;

export type CacheApi = Pick<Client, (typeof clientMethods)[number]>;

export function dial(addr: string, timeoutMs?: number): Client {
  return new Client(new pb.CacheClient(addr, credentials.createInsecure()), timeoutMs);
}

export function dialTls(addr: string, tls: TlsOptions, timeoutMs?: number): Client {
  if (!tls.serverName) throw new TypeError("serverName is required");
  if (Boolean(tls.clientCert) !== Boolean(tls.clientKey)) {
    throw new TypeError("clientCert and clientKey must both be set");
  }
  const creds: ChannelCredentials = credentials.createSsl(
    readFileSync(tls.caFile),
    tls.clientKey ? readFileSync(tls.clientKey) : null,
    tls.clientCert ? readFileSync(tls.clientCert) : null,
  );
  const raw = new pb.CacheClient(addr, creds, { "grpc.ssl_target_name_override": tls.serverName });
  return new Client(raw, timeoutMs);
}
