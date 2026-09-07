import { useState } from "react";
import { NameRow, Note, type WidgetProps } from "./shared";

export function TopKWidget({ name, setName, run, last }: WidgetProps) {
  const [item, setItem] = useState("t001");
  const entries = ((last?.result as { entries?: { item: string; count: number }[] } | undefined)?.entries) ?? [];
  const max = Math.max(1, ...entries.map((e) => e.count));
  return (
    <div>
      <Note when="heavy hitters from a stream." not="ZAdd — you do not write the score." />
      <NameRow name={name} setName={setName} placeholder="table name, e.g. hot" />
      <label className="stack">
        item
        <input value={item} onChange={(e) => setItem(e.target.value)} placeholder="observation id, e.g. t001" />
      </label>
      <div className="row">
        <button onClick={() => run("topkadd", { item })}>Observe</button>
        <button className="ghost" onClick={() => run("topklist")}>
          List
        </button>
      </div>
      <div className="bars">
        {entries.map((e) => (
          <div key={e.item} className="bar" style={{ height: `${(e.count / max) * 100}%` }} title={`${e.item} ${e.count}`} />
        ))}
      </div>
    </div>
  );
}
