export type LocalKind = "missing" | "live" | "tombstone" | "negative";

export type NodeView = {
  id: string;
  cache: string;
  kind: LocalKind;
  version: number;
  flags: number;
  bytes: number;
  role: "owner" | "replica" | "other";
};

export type ClusterNode = {
  id: string;
  cache: string;
  peer: string;
  ready: boolean;
  ring: number;
};

export type KeyspaceSnap = {
  name: string;
  mode: string;
  replication_factor: number;
};

export type ClusterInfo = {
  mode: "disconnected" | "in_process" | "remote";
  connected: boolean;
  addrs: string[];
  nodes: ClusterNode[];
  keyspaces: KeyspaceSnap[];
  ring_gen: number;
  rf: number;
  sot_loads: number;
  sot_latency: string;
};

export type View = {
  ks: string;
  key: string;
  owner: string;
  rf: number;
  ring_gen: number;
  nodes: NodeView[];
};

export type OpResp = {
  ok: boolean;
  error?: string;
  invalid_argument?: boolean;
  result?: unknown;
  via?: string;
  owner?: string;
  before?: View;
  after?: View;
  trace?: string[];
  sot_loads?: number;
  sot_delta?: number;
};

export type Chapter = {
  id: string;
  label: string;
  ks: string;
  name: string;
};

export const CHAPTERS: Chapter[] = [
  { id: "anatomy", label: "Anatomy", ks: "cacheonly", name: "session" },
  { id: "kv-write", label: "KV write", ks: "cacheonly", name: "session" },
  { id: "loadthrough", label: "LoadThrough", ks: "loadthrough", name: "chart" },
  { id: "tombstone", label: "Tombstone", ks: "cacheonly", name: "session" },
  { id: "bloom", label: "Bloom", ks: "bloom", name: "users" },
  { id: "set", label: "Set", ks: "set", name: "flags" },
  { id: "zset", label: "ZSet", ks: "zset", name: "board" },
  { id: "geo", label: "Geo", ks: "geo", name: "places" },
  { id: "list", label: "List", ks: "list", name: "inbox" },
  { id: "hash", label: "Hash", ks: "hash", name: "profile" },
  { id: "counter", label: "Counter", ks: "counter", name: "rl" },
  { id: "json", label: "JSON", ks: "json", name: "doc" },
  { id: "bitmap", label: "Bitmap", ks: "bitmap", name: "seen" },
  { id: "hll", label: "HLL", ks: "hll", name: "uniques" },
  { id: "topk", label: "TopK", ks: "topk", name: "hot" },
  { id: "cms", label: "CMS", ks: "cms", name: "freq" },
  { id: "vectorset", label: "VectorSet", ks: "vectorset", name: "items" },
  { id: "stream", label: "Stream", ks: "stream", name: "logs" },
];

export async function getCluster(): Promise<ClusterInfo> {
  const r = await fetch("/v1/cluster");
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function connectBackend(opts: { addrs?: string; in_process?: boolean }): Promise<ClusterInfo> {
  const r = await fetch("/v1/connect", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(opts),
  });
  const body = await r.json();
  if (!r.ok) throw new Error(body.error || r.statusText);
  return body as ClusterInfo;
}

export async function disconnectBackend(): Promise<ClusterInfo> {
  const r = await fetch("/v1/disconnect", { method: "POST" });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export type BloomViz = {
  m: number;
  k: number;
  present: boolean;
  bits: boolean[];
  positions: number[];
  maybe: boolean;
  item: string;
  name: string;
};

export async function getBloom(ks: string, name: string, item: string): Promise<BloomViz> {
  const q = new URLSearchParams({ ks, name, item });
  const r = await fetch(`/v1/bloom?${q}`);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function getView(ks: string, key: string): Promise<View> {
  const r = await fetch(`/v1/view?ks=${encodeURIComponent(ks)}&key=${encodeURIComponent(key)}`);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function runOp(
  ks: string,
  op: string,
  name: string,
  args: Record<string, unknown> = {},
  via = "",
): Promise<OpResp> {
  const r = await fetch("/v1/op", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ks, op, name, via, args }),
  });
  const body = (await r.json()) as OpResp;
  return body;
}

export async function runScene(id: string): Promise<{ id: string; blurb: string; steps: { resp: OpResp }[] }> {
  const r = await fetch(`/v1/scene/${encodeURIComponent(id)}`, { method: "POST" });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function resetLab(names: string[] = []): Promise<void> {
  await fetch("/v1/reset", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ names }),
  });
}

export function chapterFromURL(): string {
  const q = new URLSearchParams(window.location.search).get("chapter");
  if (q && CHAPTERS.some((c) => c.id === q)) return q;
  return "anatomy";
}

export function setChapterURL(id: string) {
  const u = new URL(window.location.href);
  u.searchParams.set("chapter", id);
  window.history.replaceState(null, "", u.toString());
}
