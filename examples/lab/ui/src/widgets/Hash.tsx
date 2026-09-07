import { useState } from "react";
import { NameRow, Note, type WidgetProps } from "./shared";

export function HashWidget({ name, setName, run, last }: WidgetProps) {
  const [field, setField] = useState("email");
  const [value, setValue] = useState("a@b");
  const fields = ((last?.result as { fields?: { field: string; value: string }[] } | undefined)?.fields) ?? [];
  return (
    <div>
      <Note when="many fields that change independently." not="one JSON Put — that is a single LWW blob." />
      <NameRow name={name} setName={setName} placeholder="hash name, e.g. profile" />
      <label className="stack">
        field
        <input value={field} onChange={(e) => setField(e.target.value)} placeholder="field name, e.g. email" />
      </label>
      <label className="stack">
        value
        <input value={value} onChange={(e) => setValue(e.target.value)} placeholder="field value, e.g. a@b" />
      </label>
      <div className="row">
        <button onClick={() => run("hset", { field, value })}>HSet</button>
        <button className="ghost" onClick={() => run("hget", { field })}>
          HGet
        </button>
        <button className="ghost" onClick={() => run("hdel", { field })}>
          HDel
        </button>
        <button className="ghost" onClick={() => run("hgetall")}>
          HGetAll
        </button>
      </div>
      {fields.map((f) => (
        <div key={f.field} className="note">
          {f.field} = {f.value}
        </div>
      ))}
    </div>
  );
}
