import { CHAPTERS } from "./api";

export function ChapterNav({ current, onPick }: { current: string; onPick: (id: string) => void }) {
  return (
    <nav className="nav">
      {CHAPTERS.map((c) => (
        <button key={c.id} className={c.id === current ? "active" : ""} onClick={() => onPick(c.id)}>
          {c.label}
        </button>
      ))}
    </nav>
  );
}
