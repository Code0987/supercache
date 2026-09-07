import type { Chapter, ClusterInfo, OpResp } from "./api";
import { BitmapWidget } from "./widgets/Bitmap";
import { BloomWidget } from "./widgets/Bloom";
import { CMSWidget } from "./widgets/CMS";
import { CacheOnlyWidget } from "./widgets/CacheOnly";
import { CounterWidget } from "./widgets/Counter";
import { GeoWidget } from "./widgets/Geo";
import { HLLWidget } from "./widgets/HLL";
import { HashWidget } from "./widgets/Hash";
import { JSONWidget } from "./widgets/JSON";
import { ListWidget } from "./widgets/List";
import { LoadThroughWidget } from "./widgets/LoadThrough";
import { SetWidget } from "./widgets/Set";
import { TopKWidget } from "./widgets/TopK";
import { VectorSetWidget } from "./widgets/VectorSet";
import { ZSetWidget } from "./widgets/ZSet";
import type { WidgetProps } from "./widgets/shared";

export function Playground(props: {
  chapter: Chapter;
  via: string;
  setVia: (s: string) => void;
  cluster: ClusterInfo | null;
  name: string;
  setName: (s: string) => void;
  run: (op: string, args?: Record<string, unknown>) => Promise<OpResp>;
  last: OpResp | null;
  onScene: () => void;
}) {
  const w: WidgetProps = {
    name: props.name,
    setName: props.setName,
    via: props.via,
    run: props.run,
    last: props.last,
    cluster: props.cluster,
  };
  return (
    <section className="play">
      <div className="section-label">Controls</div>
      <div className="row">
        <h2>{props.chapter.label}</h2>
        <label>
          ingress
          <select value={props.via} onChange={(e) => props.setVia(e.target.value)}>
            {(props.cluster?.nodes ?? []).map((n) => (
              <option key={n.id} value={n.id}>
                {n.id}
              </option>
            ))}
          </select>
        </label>
        <button onClick={props.onScene}>Run scene</button>
      </div>
      <ModeWidget id={props.chapter.id} w={w} />
    </section>
  );
}

function ModeWidget({ id, w }: { id: string; w: WidgetProps }) {
  switch (id) {
    case "loadthrough":
      return <LoadThroughWidget {...w} />;
    case "bloom":
      return <BloomWidget {...w} />;
    case "set":
      return <SetWidget {...w} />;
    case "zset":
      return <ZSetWidget {...w} />;
    case "geo":
      return <GeoWidget {...w} />;
    case "list":
      return <ListWidget {...w} />;
    case "hash":
      return <HashWidget {...w} />;
    case "counter":
      return <CounterWidget {...w} />;
    case "json":
      return <JSONWidget {...w} />;
    case "bitmap":
      return <BitmapWidget {...w} />;
    case "hll":
      return <HLLWidget {...w} />;
    case "topk":
      return <TopKWidget {...w} />;
    case "cms":
      return <CMSWidget {...w} />;
    case "vectorset":
      return <VectorSetWidget {...w} />;
    default:
      return <CacheOnlyWidget {...w} />;
  }
}
