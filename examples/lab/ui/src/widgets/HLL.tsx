import { useState } from "react";
import { NameRow, Note, type WidgetProps } from "./shared";

export function HLLWidget({ name, setName, run, last }: WidgetProps) {
  const [item, setItem] = useState("user-1");
  const [exact, setExact] = useState<Set<string>>(new Set());
  const est = (last?.result as { count?: number } | undefined)?.count;
  return (
    <div>
      <Note when="approximate distinct count." not="an exact set — items are hashed away." />
      <NameRow name={name} setName={setName} placeholder="sketch name, e.g. uniques" />
      <label className="stack">
        item
        <input value={item} onChange={(e) => setItem(e.target.value)} placeholder="hashed item, e.g. user-1" />
      </label>
      <div className="row">
        <button
          onClick={async () => {
            await run("hlladd", { item });
            setExact((s) => new Set(s).add(item));
          }}
        >
          Add
        </button>
        <button className="ghost" onClick={() => run("hllcount")}>
          Count
        </button>
      </div>
      <p className="note">
        estimate {est ?? "—"} · exact UI set {exact.size}
      </p>
    </div>
  );
}
