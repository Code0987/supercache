/** Get found=false. A gRPC status is StatusError, even when the code is NOT_FOUND. */
export class NotFound extends Error {
  constructor(message = "supercache: not found") {
    super(message);
    this.name = "NotFound";
  }
}

/** gRPC status. code is the name, for example NOT_FOUND or UNAVAILABLE. */
export class StatusError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(`${code}: ${message}`);
    this.name = "StatusError";
  }
}

export interface PeerFailure {
  peerId: string;
  message: string;
}

export class PeerFailures extends Error {
  constructor(readonly failures: PeerFailure[]) {
    super(`${failures.length} peer failure(s)`);
    this.name = "PeerFailures";
  }
}

export interface KeyErrorItem {
  key: string;
  message: string;
  peerFailures: PeerFailure[];
}

export class KeyErrors extends Error {
  constructor(readonly errors: KeyErrorItem[]) {
    super(`${errors.length} key error(s)`);
    this.name = "KeyErrors";
  }
}

const transportSnips = [
  "connection refused",
  "connection reset",
  "broken pipe",
  "socket hang up",
  "econnrefused",
  "econnreset",
];

/** Dead connection. A call deadline is not transport: the RPC may already have landed. */
export function isTransport(err: unknown): boolean {
  if (!(err instanceof StatusError)) return false;
  if (err.code === "UNAVAILABLE") return true;
  const msg = err.message.toLowerCase();
  return transportSnips.some((needle) => msg.includes(needle));
}
