import type { ClusterInfo, OpResp, View } from "./api";

export function Inspector({
  cluster,
  view,
  last,
  name,
  setName,
  onRefresh,
}: {
  cluster: ClusterInfo | null;
  view: View | null;
  last: OpResp | null;
  name: string;
  setName: (s: string) => void;
  onRefresh: () => void;
}) {
  return (
    <aside className="inspector">
      <h2>Results</h2>
      <dl>
        <dt>name</dt>
        <dd>
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            onBlur={onRefresh}
            placeholder="key or name, e.g. session"
          />
        </dd>
        <dt>owner</dt>
        <dd>
          <code>{view?.owner || "—"}</code>
        </dd>
        <dt>RF</dt>
        <dd>{view?.rf ?? cluster?.rf ?? "—"}</dd>
        <dt>ring gen</dt>
        <dd>{view?.ring_gen ?? cluster?.ring_gen ?? "—"}</dd>
        <dt>SoT loads</dt>
        <dd>
          {cluster?.sot_loads ?? 0} <span className="muted">({cluster?.sot_latency})</span>
        </dd>
      </dl>
      {last?.trace && last.trace.length > 0 && (
        <>
          <h2>Trace</h2>
          <div className="trace">
            {last.trace.map((line) => (
              <div key={line}>{line}</div>
            ))}
          </div>
        </>
      )}
      {last && (
        <>
          <h2>Last result</h2>
          {last.error && <p>{last.invalid_argument ? "invalid argument" : last.error}</p>}
          <pre className="result">{JSON.stringify(last.result ?? last, null, 2)}</pre>
        </>
      )}
    </aside>
  );
}
