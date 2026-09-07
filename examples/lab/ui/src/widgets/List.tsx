import { useState } from "react";
import { NameRow, Note, type WidgetProps } from "./shared";

export function ListWidget({ name, setName, run, last }: WidgetProps) {
  const [item, setItem] = useState("event1");
  const items = ((last?.result as { items?: string[] } | undefined)?.items) ?? [];
  return (
    <div>
      <Note when="a queue or timeline." not="a set — order and duplicates matter." />
      <NameRow name={name} setName={setName} placeholder="list name, e.g. inbox" />
      <label className="stack">
        item
        <input value={item} onChange={(e) => setItem(e.target.value)} placeholder="list element, e.g. event1" />
      </label>
      <div className="row">
        <button onClick={() => run("lpush", { item })}>LPush</button>
        <button onClick={() => run("rpush", { item })}>RPush</button>
        <button className="ghost" onClick={() => run("lpop")}>
          LPop
        </button>
        <button className="ghost" onClick={() => run("rpop")}>
          RPop
        </button>
        <button className="ghost" onClick={() => run("lrange", { start: 0, stop: -1 })}>
          LRange
        </button>
      </div>
      <div className="row">
        {items.map((it, i) => (
          <code key={i}>{it}</code>
        ))}
      </div>
    </div>
  );
}
