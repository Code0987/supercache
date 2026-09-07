import { useCallback, useEffect, useMemo, useState } from "react";
import { ChapterNav } from "./ChapterNav";
import { Inspector } from "./Inspector";
import { Playground } from "./Playground";
import {
  CHAPTERS,
  chapterFromURL,
  connectBackend,
  disconnectBackend,
  getCluster,
  getView,
  resetLab,
  runOp,
  runScene,
  setChapterURL,
  type ClusterInfo,
  type OpResp,
  type View,
} from "./api";
import { ClusterCanvas } from "./cluster/Canvas";

export function App() {
  const [chapterId, setChapterId] = useState(chapterFromURL);
  const chapter = useMemo(() => CHAPTERS.find((c) => c.id === chapterId) ?? CHAPTERS[0], [chapterId]);
  const [cluster, setCluster] = useState<ClusterInfo | null>(null);
  const [view, setView] = useState<View | null>(null);
  const [last, setLast] = useState<OpResp | null>(null);
  const [via, setVia] = useState("");
  const [name, setName] = useState(chapter.name);
  const [slowMo, setSlowMo] = useState(false);
  const [busy, setBusy] = useState(false);
  const [addrs, setAddrs] = useState(() => localStorage.getItem("lab.addrs") || "127.0.0.1:9000");
  const [backendErr, setBackendErr] = useState("");

  const refreshCluster = useCallback(async () => {
    const c = await getCluster();
    setCluster(c);
    if (c.nodes[0] && !c.nodes.some((n) => n.id === via)) setVia(c.nodes[0].id);
    if (c.addrs?.length) setAddrs(c.addrs.join(","));
    return c;
  }, [via]);

  const refreshView = useCallback(async (ks = chapter.ks, key = name) => {
    if (!ks || !key) return;
    setView(await getView(ks, key));
  }, [chapter.ks, name]);

  useEffect(() => {
    void refreshCluster().then(() => refreshView());
  }, [refreshCluster, refreshView]);

  function pickChapter(id: string) {
    const ch = CHAPTERS.find((c) => c.id === id) ?? CHAPTERS[0];
    setChapterId(ch.id);
    setChapterURL(ch.id);
    setName(ch.name);
    setLast(null);
    void refreshView(ch.ks, ch.name);
  }

  async function doOp(op: string, args: Record<string, unknown> = {}) {
    setBusy(true);
    try {
      if (slowMo) await new Promise((r) => setTimeout(r, 400));
      const resp = await runOp(chapter.ks, op, name, args, via);
      setLast(resp);
      if (resp.after) setView(resp.after);
      else await refreshView();
      await refreshCluster();
      return resp;
    } finally {
      setBusy(false);
    }
  }

  async function doScene() {
    setBusy(true);
    try {
      const scene = await runScene(chapter.id);
      const lastStep = scene.steps[scene.steps.length - 1];
      if (lastStep?.resp) {
        setLast(lastStep.resp);
        if (lastStep.resp.after) setView(lastStep.resp.after);
      }
      await refreshCluster();
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="shell">
      <header className="top">
        <h1>SuperCache Lab</h1>
        <span className="muted">
          {cluster?.mode ?? "disconnected"}
          {cluster?.connected ? ` · ${cluster.nodes.length} node(s)` : ""}
        </span>
        <input
          className="top-addrs"
          value={addrs}
          onChange={(e) => setAddrs(e.target.value)}
          placeholder="cache gRPC, e.g. 127.0.0.1:9000,127.0.0.1:9010"
          title={backendErr || "Comma-separated cache gRPC addresses"}
        />
        <button
          type="button"
          disabled={busy}
          onClick={() => {
            setBusy(true);
            setBackendErr("");
            localStorage.setItem("lab.addrs", addrs);
            void connectBackend({ addrs })
              .then((c) => {
                setCluster(c);
                setVia(c.nodes[0]?.id ?? "");
                setLast(null);
                return refreshView();
              })
              .catch((e: Error) => setBackendErr(e.message))
              .finally(() => setBusy(false));
          }}
        >
          Connect
        </button>
        <button
          type="button"
          className="ghost"
          disabled={busy}
          onClick={() => {
            setBusy(true);
            setBackendErr("");
            void connectBackend({ in_process: true })
              .then((c) => {
                setCluster(c);
                setVia(c.nodes[0]?.id ?? "");
                setLast(null);
                return refreshView();
              })
              .catch((e: Error) => setBackendErr(e.message))
              .finally(() => setBusy(false));
          }}
        >
          Local 3-node
        </button>
        <button
          type="button"
          className="ghost"
          disabled={busy || !cluster?.connected}
          onClick={() => {
            setBusy(true);
            setBackendErr("");
            void disconnectBackend()
              .then((c) => {
                setCluster(c);
                setVia("");
                setView(null);
                setLast(null);
              })
              .catch((e: Error) => setBackendErr(e.message))
              .finally(() => setBusy(false));
          }}
        >
          Disconnect
        </button>
        {backendErr && <span className="bloom-miss">{backendErr}</span>}
        <span className="grow" />
        <label className="muted">
          <input type="checkbox" checked={slowMo} onChange={(e) => setSlowMo(e.target.checked)} /> slow-mo
        </label>
        <button
          className="ghost"
          disabled={busy}
          onClick={() => {
            void resetLab().then(() => {
              setLast(null);
              void refreshView();
              void refreshCluster();
            });
          }}
        >
          Reset
        </button>
      </header>
      <ChapterNav current={chapter.id} onPick={pickChapter} />
      <ClusterCanvas view={view} last={last} />
      <aside className="sidebar">
        <Playground
          chapter={chapter}
          via={via}
          setVia={setVia}
          cluster={cluster}
          name={name}
          setName={setName}
          run={doOp}
          last={last}
          onScene={() => void doScene()}
        />
        <Inspector
          cluster={cluster}
          view={view}
          last={last}
          name={name}
          setName={setName}
          onRefresh={() => void refreshView()}
        />
      </aside>
    </div>
  );
}
