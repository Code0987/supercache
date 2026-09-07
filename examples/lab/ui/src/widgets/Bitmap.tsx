import { useState } from "react";
import { NameRow, Note, type WidgetProps } from "./shared";

export function BitmapWidget({ name, setName, run, last }: WidgetProps) {
  const [bits, setBits] = useState<boolean[]>(Array(64).fill(false));
  const [offset, setOffset] = useState(0);
  const result = last?.result as { bit?: boolean; present?: boolean; count?: number } | undefined;

  async function setBit(i: number, on: boolean) {
    const r = await run("bitset", { offset: i, bit: on });
    if (r.ok) {
      setOffset(i);
      if (i < bits.length) setBits((prev) => prev.map((b, j) => (j === i ? on : b)));
    }
  }

  async function toggle(i: number) {
    await setBit(i, !bits[i]);
  }

  async function getBit(i = offset) {
    setOffset(i);
    await run("bitget", { offset: i });
  }

  async function wipe() {
    await run("delete");
    setBits(Array(64).fill(false));
  }

  return (
    <div>
      <Note when="packed flags / presence bits." not="a Bloom filter — this is exact and MSB-first." />
      <NameRow name={name} setName={setName} placeholder="bitmap name, e.g. seen" />
      <label className="stack">
        offset
        <input
          type="number"
          min={0}
          value={offset}
          onChange={(e) => setOffset(Math.max(0, Number(e.target.value) || 0))}
          placeholder="bit index from 0, e.g. 0"
        />
      </label>
      <div className="stack-actions">
        <button type="button" onClick={() => void setBit(offset, true)}>
          Set bit 1
        </button>
        <button type="button" className="ghost" onClick={() => void setBit(offset, false)}>
          Set bit 0
        </button>
        <button type="button" className="ghost" onClick={() => void getBit()}>
          Get bit
        </button>
        <button type="button" className="ghost" onClick={() => void run("bitcount", { start: 0, end: -1 })}>
          Count ones
        </button>
        <button type="button" className="ghost" onClick={() => void wipe()}>
          Delete bitmap
        </button>
      </div>
      {result?.count !== undefined && <p className="bloom-hit">count = {result.count}</p>}
      {result?.present !== undefined && result.bit !== undefined && (
        <p className={result.bit ? "bloom-hit" : "bloom-miss"}>
          offset {offset} → {result.present ? (result.bit ? "1" : "0") : "missing"}
        </p>
      )}
      <p className="note">Click a cell to toggle that offset (0–63).</p>
      <div className="bits">
        {bits.map((on, i) => (
          <button
            key={i}
            type="button"
            className={on ? "bit on" : "bit"}
            onClick={() => void toggle(i)}
            title={`offset ${i}`}
          >
            {i}
          </button>
        ))}
      </div>
    </div>
  );
}
