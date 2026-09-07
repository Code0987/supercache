import { useState } from "react";
import { NameRow, Note, type WidgetProps } from "./shared";

export function ZSetWidget({ name, setName, run, last }: WidgetProps) {
  const [member, setMember] = useState("alice");
  const [score, setScore] = useState(100);
  const rows = ((last?.result as { members?: { member: string; score: number }[] } | undefined)?.members) ?? [];
  return (
    <div>
      <Note when="an exact scored ranking you write." not="TopK — those are observations, not scores." />
      <NameRow name={name} setName={setName} placeholder="zset name, e.g. board" />
      <label className="stack">
        member
        <input value={member} onChange={(e) => setMember(e.target.value)} placeholder="member id, e.g. alice" />
      </label>
      <label className="stack">
        score
        <input
          type="number"
          value={score}
          onChange={(e) => setScore(Number(e.target.value))}
          placeholder="float score, e.g. 100"
        />
      </label>
      <div className="row">
        <button onClick={() => run("zadd", { member, score })}>ZAdd</button>
        <button className="ghost" onClick={() => run("zrem", { member })}>
          ZRem
        </button>
        <button className="ghost" onClick={() => run("zrange", { start: 0, stop: -1 })}>
          ZRange
        </button>
      </div>
      {rows.length > 0 && (
        <table>
          <tbody>
            {rows.map((r) => (
              <tr key={r.member}>
                <td>{r.member}</td>
                <td>{r.score}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
