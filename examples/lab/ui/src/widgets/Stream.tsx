import { useState } from "react";
import { NameRow, Note, type WidgetProps } from "./shared";

type Row = { id: string; payload: string };

export function StreamWidget({ name, setName, run, last }: WidgetProps) {
  const [payload, setPayload] = useState("hello");
  const [start, setStart] = useState("-");
  const [end, setEnd] = useState("+");
  const [count, setCount] = useState(0);
  const [id, setId] = useState("");
  const [maxLen, setMaxLen] = useState(2);
  const minted = (last?.result as { id?: string } | undefined)?.id;
  const entries = ((last?.result as { entries?: Row[] } | undefined)?.entries) ?? [];
  const n = (last?.result as { n?: number; present?: boolean } | undefined)?.n;
  const present = (last?.result as { present?: boolean } | undefined)?.present;
  return (
    <div>
      <Note when="an append-only log you resume by id." not="a List — pop removes and indexes shift." />
      <NameRow name={name} setName={setName} placeholder="stream name, e.g. logs" />
      <label className="stack">
        payload
        <input value={payload} onChange={(e) => setPayload(e.target.value)} placeholder="opaque bytes, e.g. hello" />
      </label>
      <div className="row">
        <button
          onClick={async () => {
            const r = await run("xadd", { value: payload });
            const next = (r.result as { id?: string } | undefined)?.id;
            if (next) setId(next);
          }}
        >
          XAdd
        </button>
      </div>
      <label className="stack">
        start
        <input value={start} onChange={(e) => setStart(e.target.value)} placeholder="- or millis-seq or (id" />
      </label>
      <label className="stack">
        end
        <input value={end} onChange={(e) => setEnd(e.target.value)} placeholder="+ or millis-seq" />
      </label>
      <label className="stack">
        count
        <input
          type="number"
          value={count}
          onChange={(e) => setCount(Number(e.target.value))}
          placeholder="0 = no cap, e.g. 10"
        />
      </label>
      <div className="row">
        <button onClick={() => run("xrange", { start, end, count })}>XRange</button>
        <button className="ghost" onClick={() => run("xrevrange", { start, end, count })}>
          XRevRange
        </button>
        <button className="ghost" onClick={() => run("xlen")}>
          XLen
        </button>
      </div>
      <label className="stack">
        id
        <input value={id} onChange={(e) => setId(e.target.value)} placeholder="millis-seq, e.g. 1710000000000-0" />
      </label>
      <label className="stack">
        maxLen
        <input
          type="number"
          value={maxLen}
          onChange={(e) => setMaxLen(Number(e.target.value))}
          placeholder="keep newest N, e.g. 2"
        />
      </label>
      <div className="row">
        <button className="ghost" onClick={() => run("xdel", { id })}>
          XDel
        </button>
        <button className="ghost" onClick={() => run("xtrim", { max_len: maxLen })}>
          XTrim
        </button>
        <button className="ghost" onClick={() => run("delete")}>
          Delete stream
        </button>
      </div>
      {minted && <p className="note">id {minted}</p>}
      {present !== undefined && n !== undefined && (
        <p className="note">
          len {n}
          {!present ? " (missing)" : ""}
        </p>
      )}
      <div className="stack">
        {entries.map((e) => (
          <code key={e.id}>
            {e.id} {e.payload}
          </code>
        ))}
      </div>
    </div>
  );
}
