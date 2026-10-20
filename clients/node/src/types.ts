/** Values returned by Client. These are not the generated wire messages. */

export type Bytes = Uint8Array | string;

export interface ZMember {
  member: Uint8Array;
  score: number;
}

export interface GeoMember {
  member: Uint8Array;
  lon: number;
  lat: number;
  dist: number;
}

export interface HashField {
  field: Uint8Array;
  value: Uint8Array;
}

export interface StreamEntry {
  id: string;
  payload: Uint8Array;
}

export interface VSimHit {
  member: Uint8Array;
  score: number;
}

export interface TopKEntry {
  item: Uint8Array;
  count: number;
}

/** A read that distinguishes a missing structure from a real zero. */
export interface Present {
  value: number;
  found: boolean;
}

export interface TopKResult {
  entries: TopKEntry[];
  found: boolean;
}

export interface BitValue {
  value: boolean;
  found: boolean;
}

export interface BitPos {
  pos: number;
  found: boolean;
}

/** Undefined ttlMs leaves ttl_set false. 0 sets ttl_set and stores no expiry. */
export interface PutOptions {
  ttlMs?: number;
  timeoutMs?: number;
}

export interface TlsOptions {
  caFile: string;
  serverName: string;
  clientCert?: string;
  clientKey?: string;
}
