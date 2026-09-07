import { useState } from "react";
import { NameRow, Note, type WidgetProps } from "./shared";

type Pt = { member: string; x: number; y: number };

export function VectorSetWidget({ name, setName, run, last }: WidgetProps) {
  const [member, setMember] = useState("east");
  const [x, setX] = useState(1);
  const [y, setY] = useState(0);
  const [pts, setPts] = useState<Pt[]>([]);
  const hits = ((last?.result as { hits?: { member: string; score: number }[] } | undefined)?.hits) ?? [];
  return (
    <div>
      <Note when="small in-memory K-NN." not="HNSW / Faiss — brute force under the store mutex." />
      <NameRow name={name} setName={setName} placeholder="set name, e.g. items" />
      <label className="stack">
        member
        <input value={member} onChange={(e) => setMember(e.target.value)} placeholder="vector id, e.g. east" />
      </label>
      <label className="stack">
        x (dim 0)
        <input
          type="number"
          step="0.1"
          value={x}
          onChange={(e) => setX(Number(e.target.value))}
          placeholder="float32, e.g. 1"
        />
      </label>
      <label className="stack">
        y (dim 1)
        <input
          type="number"
          step="0.1"
          value={y}
          onChange={(e) => setY(Number(e.target.value))}
          placeholder="float32, e.g. 0"
        />
      </label>
      <div className="row">
        <button
          onClick={async () => {
            await run("vadd", { member, vec: [x, y] });
            setPts((p) => [...p.filter((q) => q.member !== member), { member, x, y }]);
          }}
        >
          VAdd
        </button>
        <button className="ghost" onClick={() => run("vsim", { vec: [x, y], k: 3 })}>
          VSim
        </button>
        <button className="ghost" onClick={() => run("vrem", { member })}>
          VRem
        </button>
      </div>
      <svg className="plot" viewBox="-1.2 -1.2 2.4 2.4">
        <line x1="-1.2" y1="0" x2="1.2" y2="0" stroke="#334155" />
        <line x1="0" y1="-1.2" x2="0" y2="1.2" stroke="#334155" />
        {pts.map((p) => (
          <circle key={p.member} cx={p.x} cy={-p.y} r="0.07" fill="#38bdf8" />
        ))}
        <line x1="0" y1="0" x2={x} y2={-y} stroke="#fbbf24" strokeWidth="0.03" />
      </svg>
      {hits.length > 0 && <p className="note">{hits.map((h) => `${h.member} ${h.score.toFixed(3)}`).join(" · ")}</p>}
    </div>
  );
}
