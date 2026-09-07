import { useState } from "react";
import { NameRow, Note, type WidgetProps } from "./shared";

export function CounterWidget({ name, setName, run, last }: WidgetProps) {
  const [delta, setDelta] = useState(1);
  const n = (last?.result as { value?: number } | undefined)?.value;
  return (
    <div>
      <Note when="a single int64 (rate-limit windows, tallies)." not="a float score — that is ZSet." />
      <NameRow name={name} setName={setName} placeholder="counter name, e.g. rl" />
      <label className="stack">
        delta
        <input
          type="number"
          value={delta}
          onChange={(e) => setDelta(Number(e.target.value))}
          placeholder="int64 to add, e.g. 1 or -1"
        />
      </label>
      <div className="row">
        <button onClick={() => run("incr", { delta })}>Incr</button>
        <button className="ghost" onClick={() => run("cget")}>
          Get
        </button>
        <button className="ghost" onClick={() => run("delete")}>
          Delete
        </button>
        {n !== undefined && <strong style={{ fontSize: "1.6rem" }}>{n}</strong>}
      </div>
    </div>
  );
}
