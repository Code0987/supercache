import { useState } from "react";
import { NameRow, Note, type WidgetProps } from "./shared";

export function CMSWidget({ name, setName, run, last }: WidgetProps) {
  const [item, setItem] = useState("t003");
  const [n, setN] = useState(1);
  const count = (last?.result as { count?: number } | undefined)?.count;
  return (
    <div>
      <Note when="approximate frequency of any item." not="TopK — CMS still answers after eviction." />
      <NameRow name={name} setName={setName} placeholder="sketch name, e.g. freq" />
      <label className="stack">
        item
        <input value={item} onChange={(e) => setItem(e.target.value)} placeholder="item to count, e.g. t003" />
      </label>
      <label className="stack">
        n (0 means 1)
        <input
          type="number"
          value={n}
          onChange={(e) => setN(Number(e.target.value))}
          placeholder="increment count, e.g. 1"
        />
      </label>
      <div className="row">
        <button onClick={() => run("cmsincr", { item, n })}>Incr</button>
        <button className="ghost" onClick={() => run("cmsquery", { item })}>
          Query
        </button>
        {count !== undefined && <strong>{count}</strong>}
      </div>
    </div>
  );
}
