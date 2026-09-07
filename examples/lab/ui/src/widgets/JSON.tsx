import { useState } from "react";
import { NameRow, Note, type WidgetProps } from "./shared";

export function JSONWidget({ name, setName, run, last }: WidgetProps) {
  const [path, setPath] = useState("$.name");
  const [value, setValue] = useState('"Ada"');
  const raw = (last?.result as { raw?: string } | undefined)?.raw;
  return (
    <div>
      <Note when="a nested document with path updates." not="per-field concurrency — use Hash." />
      <NameRow name={name} setName={setName} placeholder="document name, e.g. doc" />
      <label className="stack">
        path
        <input value={path} onChange={(e) => setPath(e.target.value)} placeholder='JSON path, e.g. $.name or $' />
      </label>
      <label className="stack">
        JSON value
        <input value={value} onChange={(e) => setValue(e.target.value)} placeholder='raw JSON, e.g. "Ada" or {"ok":true}' />
      </label>
      <div className="row">
        <button onClick={() => run("jsonset", { path, value })}>Set</button>
        <button className="ghost" onClick={() => run("jsonget", { path: "$" })}>
          Get $
        </button>
        <button className="ghost" onClick={() => run("jsondel", { path })}>
          Del path
        </button>
      </div>
      {raw && <pre className="result">{raw}</pre>}
    </div>
  );
}
