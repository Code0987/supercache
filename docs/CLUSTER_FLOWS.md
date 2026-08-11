# SuperCache cluster flows (N nodes)

Mermaid-only flow diagrams. Render on GitHub or any Mermaid viewer.

---

## Topology

```mermaid
flowchart TB
  subgraph Apps["Application tier — not in ring"]
    A1["App / pkg/client"]
    A2["App / pkg/client"]
    A3["…"]
  end

  subgraph Cluster["Cache mesh — N × supercache-node"]
    subgraph N1["Node 1"]
      C1["Cache gRPC"]
      E1["Engine + store"]
      P1["Peer gRPC"]
      W1["Warmup"]
      AD1["Admin HTTP"]
    end
    subgraph N2["Node 2"]
      C2["Cache gRPC"]
      E2["Engine + store"]
      P2["Peer gRPC"]
      W2["Warmup"]
    end
    subgraph Nn["Node N"]
      Cn["Cache gRPC"]
      En["Engine + store"]
      Pn["Peer gRPC"]
      Wn["Warmup"]
    end
    GOS["Gossip memberlist"]
    RING["Consistent-hash ring<br/>owner per key"]
  end

  DS["DataSource backends<br/>LoadThrough only"]

  A1 & A2 & A3 -->|"gRPC Cache"| C1 & C2 & Cn
  C1 --> E1
  C2 --> E2
  Cn --> En
  E1 & E2 & En --> RING
  P1 <-->|"ApplyPut / ApplyDelete<br/>ForwardPut / ForwardDelete<br/>GetOrLoad"| P2
  P2 <--> Pn
  P1 <--> Pn
  GOS <--> N1 & N2 & Nn
  GOS --> RING
  GOS --> W1 & W2 & Wn
  W1 --> E1
  W2 --> E2
  Wn --> En
  E1 & E2 & En -.->|"LoadThrough miss"| DS
```

---

## Ownership

```mermaid
flowchart LR
  K["key"] --> H["hash(key)"]
  H --> R["ring with N peers<br/>× virtual nodes"]
  R --> O["Owner node O ∈ 1..N"]
  O --> D1["Coordinates Put ACK<br/>+ version mint"]
  O --> D2["Preferred GetOrLoad<br/>/ miss coalescing"]
  O --> D3["Does NOT solely store<br/>mesh may hold copies"]
```

---

## Put

```mermaid
flowchart TD
  START["Put(ks, key, value) on node Nx"] --> OWN{"Nx == ring.Owner(key)?"}

  OWN -->|yes| APPLY["nextVersion(key)<br/>store.AcceptIfNewer"]
  APPLY --> ACK["ACK to client"]
  ACK --> FAN["async ApplyPut to R−1 replicas<br/>no retry — log/metric on fail"]

  OWN -->|no| FWD["ForwardPut to owner<br/>hop_count = 0"]
  FWD --> OL["Owner PutLocalAtHop"]
  OL --> OACK["ACK back to Nx → client"]
  OACK --> FAN2["Owner async ApplyPut × R−1"]

  FAN --> PEER["Each peer: AcceptIfNewer LWW"]
  FAN2 --> PEER
```

---

## Put — forward hop limit

```mermaid
flowchart TD
  A["PutLocalAtHop on node S<br/>with hopCount"] --> B{"Owner(key) == S?"}
  B -->|yes| C["putLocalApply + fan-out"]
  B -->|no| D{"hopCount < maxForwardHops<br/>const = 1?"}
  D -->|yes| E["ForwardPut owner hop+1"]
  E -->|ok| F["return"]
  E -->|fail| G["putLocalApply + forceFanout"]
  D -->|no| G
  G --> H["Local apply + force ApplyPut<br/>to replica set"]
```

---

## PutMany

```mermaid
flowchart TD
  PM["PutMany(items)"] --> LOOP["For each key independently"]
  LOOP --> PUT["Same Put path<br/>owner route / ForwardPut"]
  PUT --> ERR["Collect per-key errors<br/>not atomic"]
  ERR --> CAP["Max batch default 100"]
```

---

## Get

```mermaid
flowchart TD
  G["Get(ks, key) on node N"] --> L{"Local store hit<br/>not expired?"}

  L -->|positive| HIT["return value<br/>RecordHit → hot tracker"]
  L -->|negative| NEG["return NotFound"]
  L -->|miss| M{"Mode?"}

  M -->|CacheOnly| CO["return NotFound"]
  M -->|LoadThrough| SF["singleflight get:key"]

  SF --> O{"N is owner?"}
  O -->|yes| LT["loadThrough<br/>protect → DataSource"]
  O -->|no| GOL["peer.GetOrLoad(owner)"]

  GOL -->|ok| INST["AcceptIfNewer / AcceptNegative<br/>same version"]
  INST --> RET["return value or NotFound"]
  GOL -->|owner down| FB["local loadThrough<br/>no fan-out"]

  LT -->|value| FILL["mint version + store<br/>async ApplyPut × R−1"]
  LT -->|NotFound| NFIL["optional negative + fan-out"]
  LT -->|error| E["error / Unavailable"]
  FILL --> R2["return value"]
  NFIL --> R3["return NotFound"]
  FB --> R2
```

---

## Get — LoadThrough miss sequence

```mermaid
flowchart TD
  subgraph NonOwner["Node N non-owner"]
    A["local miss"] --> B["GetOrLoad → owner O"]
    B --> C{"response"}
    C -->|entry| D["AcceptIfNewer"]
    C -->|owner fail| E["local DataSource<br/>no fan-out"]
  end

  subgraph Owner["Owner O"]
    F["local hit?"] -->|yes| G["return Entry"]
    F -->|miss| H["DataSource.Load"]
    H --> I["version + store"]
    I --> J["async ApplyPut peers"]
    J --> K["return Entry"]
  end

  B -.-> F
  G -.-> C
  K -.-> C
```

---

## Delete

```mermaid
flowchart TD
  D["Delete(ks, key) on Nx"] --> O{"Nx is owner?"}

  O -->|yes| V["mint delete_version<br/>DeleteIfVersion tombstone"]
  V --> P["parallel ApplyDelete<br/>to all other peers"]
  P --> R{"any peer fail?"}
  R -->|no| OK["return OK"]
  R -->|yes| ME["return MultiError"]

  O -->|no| FD["ForwardDelete → owner"]
  FD -->|ok| OWN["owner DeleteAsOwner"]
  FD -->|owner down| FB["local DeleteAsOwner fallback"]
  OWN --> P2["owner ApplyDelete × peers"]
  FB --> P2
  P2 --> R
```

---

## Peer RPCs

```mermaid
flowchart LR
  subgraph PeerAPI["Peer gRPC — mesh only"]
    AP["ApplyPut"] --> LWW["AcceptIfNewer / AcceptNegative"]
    AD["ApplyDelete"] --> TB["DeleteIfVersion tombstone"]
    FP["ForwardPut"] --> PL["PutLocalAtHop"]
    FDel["ForwardDelete"] --> DO["DeleteAsOwner"]
    GL["GetOrLoad"] --> OL["GetOrLoadLocal<br/>may load DataSource"]
  end

  PUT["Put fan-out / handoff"] --> AP
  DEL["Delete path"] --> AD
  NPUT["Non-owner Put"] --> FP
  NDEL["Non-owner Delete"] --> FDel
  GET["LoadThrough / CacheOnly miss"] --> GL
```

---

## Membership

```mermaid
flowchart TD
  EV["memberlist<br/>Join / Leave / Update"] --> ASYNC["async rebuildRing<br/>avoid gossip lock deadlock"]
  ASYNC --> META["read peer_grpc from node meta"]
  META --> SP["ring.SetPeers(all members)<br/>bump ring_generation"]
  SP --> EVT["emit ClusterEvent"]
  EVT --> NT["Engine.NotifyTopologyChange"]
  NT --> WM["warmup.OnTopologyChange"]
```

---

## Topology handoff — hot then rest

```mermaid
flowchart TD
  T["OnTopologyChange on each of N nodes"] --> P1["HOT queue: prefetch<br/>WarmKeys ∪ TopN hot"]
  T --> DH{"DisableHandoff?"}

  DH -->|yes| SKIP["no push"]
  DH -->|no| LE["LocalEntries(ks)<br/>Range live non-tombstone"]

  LE --> SPLIT{"key in hot set?"}
  SPLIT -->|yes| HOT["enqueue HOT jobHandoff"]
  SPLIT -->|no| REST["enqueue REST jobHandoff<br/>HandoffMaxEntries cap"]

  P1 --> W["Workers prefer hotJobs<br/>over restJobs"]
  HOT --> W
  REST --> W

  W --> REP["ReplicateToPeers<br/>fanoutPut force=true"]
  REP --> AP["ApplyPut → replica set except self"]
  AP --> J["Receivers AcceptIfNewer<br/>joiner warms async"]
```

---

## Join N → N+1

```mermaid
flowchart TD
  S["Stable N nodes<br/>data fanned out"] --> J["Node N+1 joins gossip"]
  J --> R["All nodes rebuild ring"]
  R --> COLD["New node store empty<br/>cold window"]
  COLD --> H["Existing nodes handoff<br/>hot keys first then rest"]
  H --> W["New node AcceptIfNewer"]
  W --> WARM["New node serves local hits<br/>LoadThrough avoids extra DS<br/>if entry arrived"]
```

---

## Leave N → N−1

```mermaid
flowchart TD
  L["Node L leaves"] --> R["Survivors rebuild ring"]
  R --> OWN["Ownership remaps off L"]
  OWN --> KEEP["Survivors keep local copies<br/>until TTL / LRU"]
  KEEP --> H["OnTopologyChange handoff<br/>re-push inventory among survivors"]
```

---

## Warmup & refresh-ahead

```mermaid
flowchart TD
  HIT["Get hit"] --> TR["Tracker.Hit"]
  TR --> TOP["TopN per keyspace"]

  TOPO["Topology change"] --> PRE["Prefetch WarmKeys + TopN"]
  PRE --> OW{"owns key?"}
  OW -->|yes| GOL["GetOrLoadLocal"]
  OW -->|no| GET["Get may remote"]

  TICK["1s refresh loop"] --> RI{"LoadThrough and<br/>RefreshInterval elapsed?"}
  RI -->|yes| FL["ForceLoad on owner only"]
  FL --> DS["DataSource reload + fan-out"]
```

---

## Protect DataSource

```mermaid
flowchart TD
  M["LoadThrough miss path"] --> G["Guard.Allow<br/>keyspace ± global"]
  G -->|ok| L["DataSource.Load"]
  G -->|rate limited| RL["Unavailable"]
  G -->|circuit open| CO["Unavailable"]
  L -->|success| S["OnSuccess"]
  L -->|failure| F["OnFailure → may open breaker"]
```

---

## End-to-end ops

```mermaid
flowchart TB
  subgraph Client
    OPS["Get / Put / PutMany<br/>Delete / DeleteMany"]
  end

  subgraph Node["Any of N nodes"]
    CG["Cache gRPC"]
    EN["Engine"]
    ST["Local store"]
    KS["KeySpaces"]
    WM["Warmup + handoff"]
  end

  subgraph Mesh
    RING["Hash ring"]
    PG["Peer gRPC mesh"]
    GOS["Gossip"]
  end

  DS["DataSource"]
  ADM["Admin HTTP"]

  OPS --> CG --> EN
  EN --> ST
  EN --> KS
  EN --> RING
  EN --> PG
  KS --> DS
  GOS --> RING
  GOS --> WM --> EN
  EN --> ADM
  PG <--> PG
```

---

## Key lifecycle

```mermaid
flowchart LR
  P0["Put on owner"] --> P1["async ApplyPut × R−1"]
  P1 --> P2["Steady: local Get hits<br/>on nodes that received fan-out"]
  P2 --> P3["Join: ring remap<br/>joiner cold then handoff"]
  P3 --> P4["Leave: remap<br/>survivors keep copies"]
  P4 --> P5["Delete: tombstone<br/>ApplyDelete × peers"]
  P5 --> P6["TTL / LRU eviction"]
```

---

## Which path

```mermaid
flowchart TD
  E{"Event"} --> E1["App Put"]
  E --> E2["App Get hit"]
  E --> E3["App Get miss CacheOnly"]
  E --> E4["App Get miss LoadThrough"]
  E --> E5["App Delete"]
  E --> E6["Peer join / leave"]
  E --> E7["Handoff job"]
  E --> E8["Owner down on Get"]
  E --> E9["Owner down on ForwardPut"]

  E1 --> A1["Owner apply + async ApplyPut × R−1"]
  E2 --> A2["Local store only"]
  E3 --> A3["NotFound"]
  E4 --> A4["Owner GetOrLoad → DS; install on caller"]
  E5 --> A5["Owner mint version + sync ApplyDelete"]
  E6 --> A6["Gossip → SetPeers → handoff hot then rest"]
  E7 --> A7["force ApplyPut of local entry to peers"]
  E8 --> A8["Local loadThrough no fan-out"]
  E9 --> A9["Error or force local apply path"]
```
