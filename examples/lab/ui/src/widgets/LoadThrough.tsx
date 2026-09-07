import { NameRow, Note, type WidgetProps } from "./shared";

export function LoadThroughWidget({ name, setName, run, cluster }: WidgetProps) {
  return (
    <div>
      <Note when="cache a backend (SoT) on miss, with singleflight." not="CacheOnly — a miss here loads." />
      <NameRow name={name} setName={setName} placeholder="key to load, e.g. chart" />
      <div className="row">
        <button onClick={() => run("get")}>Get (may load SoT)</button>
        <button className="ghost" onClick={() => run("delete")}>
          Delete / invalidate
        </button>
        <span className="note">SoT loads: {cluster?.sot_loads ?? 0}</span>
      </div>
    </div>
  );
}
