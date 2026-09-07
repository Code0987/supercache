import type { ClusterInfo, OpResp } from "../api";

export type WidgetProps = {
  name: string;
  setName: (s: string) => void;
  via: string;
  run: (op: string, args?: Record<string, unknown>) => Promise<OpResp>;
  last: OpResp | null;
  cluster: ClusterInfo | null;
};

export function Note({ when, not }: { when: string; not: string }) {
  return (
    <p className="note">
      <strong>Use when</strong> {when} · <strong>Not</strong> {not}
    </p>
  );
}

export function NameRow({
  name,
  setName,
  placeholder = "structure name, e.g. session",
}: {
  name: string;
  setName: (s: string) => void;
  placeholder?: string;
}) {
  return (
    <label className="stack">
      name
      <input value={name} onChange={(e) => setName(e.target.value)} placeholder={placeholder} />
    </label>
  );
}
