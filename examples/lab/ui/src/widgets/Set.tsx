import { useState } from "react";
import { NameRow, Note, type WidgetProps } from "./shared";

export function SetWidget({ name, setName, run, last }: WidgetProps) {
  const [item, setItem] = useState("dark_mode");
  const members = ((last?.result as { members?: string[] } | undefined)?.members) ?? [];
  return (
    <div>
      <Note when="exact membership." not="a Bloom filter, and not Get/Put." />
      <NameRow name={name} setName={setName} placeholder="set name, e.g. flags" />
      <label className="stack">
        item
        <input value={item} onChange={(e) => setItem(e.target.value)} placeholder="exact member, e.g. dark_mode" />
      </label>
      <div className="row">
        <button onClick={() => run("sadd", { item })}>Add</button>
        <button className="ghost" onClick={() => run("srem", { item })}>
          Remove
        </button>
        <button className="ghost" onClick={() => run("sismember", { item })}>
          Contains
        </button>
        <button className="ghost" onClick={() => run("smembers")}>
          Members
        </button>
        <button className="ghost" onClick={() => run("get")}>
          Wrong verb (Get)
        </button>
      </div>
      {members.length > 0 && <p className="note">{members.join(", ")}</p>}
    </div>
  );
}
