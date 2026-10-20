export { Client, clientMethods, dial, dialTls } from "./client.js";
export type { CacheApi } from "./client.js";
export type {
  BitPos,
  BitValue,
  Bytes,
  GeoMember,
  HashField,
  Present,
  PutOptions,
  StreamEntry,
  TlsOptions,
  TopKEntry,
  TopKResult,
  VSimHit,
  ZMember,
} from "./types.js";
export {
  KeyErrorItem,
  KeyErrors,
  NotFound,
  PeerFailure,
  PeerFailures,
  StatusError,
  isTransport,
} from "./errors.js";
export { Session, type SessionOptions } from "./session.js";
