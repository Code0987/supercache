import { useState } from "react";
import { NameRow, Note, type WidgetProps } from "./shared";

export function CacheOnlyWidget({ name, setName, run }: WidgetProps) {
  const [value, setValue] = useState("hello");
  return (
    <div>
      <Note when="opaque KV with no backend." not="a source of truth — writes ACK on the owner." />
      <NameRow name={name} setName={setName} placeholder="key, e.g. session" />
      <label className="stack">
        value (bytes as text)
        <input value={value} onChange={(e) => setValue(e.target.value)} placeholder="opaque string, e.g. hello" />
      </label>
      <div className="row">
        <button onClick={() => run("put", { value })}>Put</button>
        <button className="ghost" onClick={() => run("get")}>
          Get
        </button>
        <button className="ghost" onClick={() => run("delete")}>
          Delete
        </button>
      </div>
    </div>
  );
}
