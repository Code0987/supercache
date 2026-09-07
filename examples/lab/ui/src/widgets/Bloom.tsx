import { useEffect, useState } from "react";
import { getBloom, type BloomViz } from "../api";
import { NameRow, Note, type WidgetProps } from "./shared";

export function BloomWidget({ name, setName, run, last }: WidgetProps) {
  const [item, setItem] = useState("alice");
  const [viz, setViz] = useState<BloomViz | null>(null);
  const test = last?.result as { maybe?: boolean } | undefined;
  const tested = last && "maybe" in (last.result ?? {});

  useEffect(() => {
    let cancel = false;
    void getBloom("bloom", name, item)
      .then((v) => {
        if (!cancel) setViz(v);
      })
      .catch(() => {
        if (!cancel) setViz(null);
      });
    return () => {
      cancel = true;
    };
  }, [name, item, last]);

  async function add(value = item) {
    const v = value.trim();
    if (!v) return;
    setItem(v);
    await run("bloomadd", { item: v });
  }

  async function testItem(value = item) {
    const v = value.trim();
    if (!v) return;
    setItem(v);
    await run("bloomtest", { item: v });
  }

  async function wipe() {
    await run("delete");
  }

  const bits = viz?.bits ?? Array(64).fill(false);
  const probes = new Set(viz?.positions ?? []);
  const maybe = tested ? test?.maybe : viz?.maybe;

  return (
    <div>
      <Note when="cheap maybe-membership." not="exact — this is not a bitmap you toggle." />
      <NameRow name={name} setName={setName} placeholder="filter name, e.g. users" />
      <label className="stack">
        item
        <input
          value={item}
          onChange={(e) => setItem(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") void add();
          }}
          placeholder="member to add/test, e.g. alice"
        />
      </label>
      <div className="stack-actions">
        <button type="button" onClick={() => void add()}>
          Add to filter
        </button>
        <button type="button" className="ghost" onClick={() => void testItem()}>
          Test membership
        </button>
        <button type="button" className="ghost" onClick={() => void wipe()}>
          Delete filter
        </button>
      </div>
      {maybe !== undefined && item.trim() && (
        <p className={maybe ? "bloom-hit" : "bloom-miss"}>
          {item} → {maybe ? "maybe" : "no"}
          {viz && ` · k=${viz.k} hashes`}
        </p>
      )}
      <p className="note">
        Grid is the 64-bit filter (same idea as Bitmap). Yellow ring = hash slots for the item in the box.
      </p>
      <div className="bits">
        {bits.map((on, i) => (
          <span
            key={i}
            className={"bit" + (on ? " on" : "") + (probes.has(i) ? " probe" : "")}
            title={`bit ${i}${probes.has(i) ? " (hash)" : ""}${on ? " set" : ""}`}
          >
            {i}
          </span>
        ))}
      </div>
    </div>
  );
}
