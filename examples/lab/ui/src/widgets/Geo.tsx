import { useState } from "react";
import { NameRow, Note, type WidgetProps } from "./shared";

type Hit = { member: string; lon: number; lat: number; dist_meters?: number };

const DOT_COLORS = ["#38bdf8", "#34d399", "#fbbf24", "#f472b6", "#a78bfa", "#fb7185", "#2dd4bf", "#f97316"];

function dotColor(member: string): string {
  let h = 0;
  for (let i = 0; i < member.length; i++) h = (h * 31 + member.charCodeAt(i)) >>> 0;
  return DOT_COLORS[h % DOT_COLORS.length];
}

export function GeoWidget({ name, setName, run, last }: WidgetProps) {
  const [member, setMember] = useState("shop");
  const [lon, setLon] = useState(-74);
  const [lat, setLat] = useState(40.7);
  const [radiusKm, setRadiusKm] = useState(20);
  const [limit, setLimit] = useState(10);
  const hits = ((last?.result as { members?: Hit[] } | undefined)?.members) ?? [];
  const rKm = Math.max(radiusKm, 0.001);
  const ring = 0.35;

  function meters() {
    return Math.max(0, radiusKm) * 1000;
  }

  function toXY(hLon: number, hLat: number) {
    const kmLat = (hLat - lat) * 111.32;
    const kmLon = (hLon - lon) * 111.32 * Math.cos((lat * Math.PI) / 180);
    return { x: (kmLon / rKm) * ring, y: (-kmLat / rKm) * ring };
  }

  return (
    <div>
      <Note when="lon/lat radius queries." not="an embedding space — use VectorSet." />
      <NameRow name={name} setName={setName} placeholder="index name, e.g. places" />
      <label className="stack">
        member
        <input value={member} onChange={(e) => setMember(e.target.value)} placeholder="point id, e.g. shop" />
      </label>
      <div className="pair">
        <label className="stack">
          lat
          <input
            type="number"
            step="0.01"
            value={lat}
            onChange={(e) => setLat(Number(e.target.value))}
            placeholder="e.g. 40.7"
          />
        </label>
        <label className="stack">
          lng
          <input
            type="number"
            step="0.01"
            value={lon}
            onChange={(e) => setLon(Number(e.target.value))}
            placeholder="e.g. -74.0"
          />
        </label>
      </div>
      <label className="stack">
        radius (km)
        <input
          type="number"
          min={0}
          step="0.1"
          value={radiusKm}
          onChange={(e) => setRadiusKm(Number(e.target.value))}
          placeholder="search radius in km, e.g. 20"
        />
      </label>
      <label className="stack">
        limit (0 = all)
        <input
          type="number"
          min={0}
          value={limit}
          onChange={(e) => setLimit(Number(e.target.value))}
          placeholder="max hits, e.g. 10"
        />
      </label>
      <div className="stack-actions">
        <button type="button" onClick={() => void run("geoadd", { member, lon, lat })}>
          Add point
        </button>
        <button
          type="button"
          className="ghost"
          onClick={() => void run("georadius", { lon, lat, radius: meters(), limit })}
        >
          Radius query
        </button>
        <button type="button" className="ghost" onClick={() => void run("georem", { member })}>
          Remove member
        </button>
      </div>
      {hits.length > 0 && (
        <p className="note">
          {hits.length} hit{hits.length === 1 ? "" : "s"} within {radiusKm} km
        </p>
      )}
      <svg className="plot" viewBox="-1 -1 2 2">
        <circle cx="0" cy="0" r={ring} fill="none" stroke="#334155" />
        {hits.map((h) => {
          const p = toXY(h.lon, h.lat);
          return (
            <circle
              key={h.member}
              cx={p.x}
              cy={p.y}
              r="0.028"
              fill={dotColor(h.member)}
              fillOpacity={0.5}
            />
          );
        })}
      </svg>
    </div>
  );
}
