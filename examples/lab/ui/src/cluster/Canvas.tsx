import type { OpResp, View } from "../api";

export function ClusterCanvas({ view, last }: { view: View | null; last: OpResp | null }) {
  const nodes = view?.nodes ?? [];
  return (
    <section className="canvas">
      <div className="nodes">
        {nodes.map((n) => {
          const cls = ["node", n.role, n.kind].join(" ");
          return (
            <article key={n.id} className={cls}>
              <div className="id">{n.id}</div>
              <div className="meta">{n.cache}</div>
              <div className="meta">
                {n.role} · v{n.version} · {n.bytes}B
              </div>
              <span className={"kind " + n.kind}>{n.kind}</span>
            </article>
          );
        })}
        {nodes.length === 0 && (
          <p className="muted">No backend. Connect cache gRPC addresses above, or start a local 3-node mesh.</p>
        )}
      </div>
      <div className="trace">
        {(last?.trace ?? ["Run a chapter or a verb — the mesh observation lands here."]).map((line) => (
          <div key={line}>{line}</div>
        ))}
      </div>
    </section>
  );
}
