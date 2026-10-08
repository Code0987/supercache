/** Sticky multi-seed dialing, same policy as cmd/sc. */

import { Client, clientMethods, dial, dialTls, type CacheApi } from "./client.js";
import type { TlsOptions } from "./types.js";
import { StatusError, isTransport } from "./errors.js";

export interface SessionOptions {
  tls?: TlsOptions;
  timeoutMs?: number;
  /** How long to wait for a seed to accept a connection. Default 500ms. */
  probeMs?: number;
}

/**
 * Walk seeds on dial. Retry a call once after UNAVAILABLE or a reset connection.
 * A call-level deadline is not retried: the server may already have applied it.
 */
export class Session {
  private readonly addrs: string[];
  private readonly tls?: TlsOptions;
  private readonly timeoutMs?: number;
  private readonly probeMs: number;
  private idx = 0;
  private current: Client | undefined;
  private addr: string | undefined;
  private chain: Promise<void> = Promise.resolve();

  constructor(addrs: readonly string[], opts: SessionOptions = {}) {
    if (addrs.length === 0) throw new TypeError("at least one address is required");
    this.addrs = [...addrs];
    this.tls = opts.tls;
    this.timeoutMs = opts.timeoutMs;
    this.probeMs = opts.probeMs ?? 500;
    for (const name of clientMethods) {
      (this as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>)[name] = (...args: unknown[]) =>
        this.run((client) => (client[name] as (...a: unknown[]) => Promise<unknown>)(...args));
    }
  }

  connectedAddr(): string | undefined {
    return this.addr;
  }

  close(): void {
    this.current?.close();
    this.current = undefined;
    this.addr = undefined;
  }

  async run<T>(fn: (client: Client) => Promise<T>): Promise<T> {
    const cli = await this.ensure();
    try {
      return await fn(cli);
    } catch (err) {
      if (!isTransport(err)) throw err;
      const first = err;
      await this.invalidate();
      let next: Client;
      try {
        next = await this.ensure();
      } catch (err2) {
        throw new StatusError("UNAVAILABLE", `${messageOf(first)} (re-dial: ${messageOf(err2)})`);
      }
      return fn(next);
    }
  }

  private dial(addr: string): Client {
    if (!this.tls) return dial(addr, this.timeoutMs);
    return dialTls(addr, this.tls, this.timeoutMs);
  }

  private async ensure(): Promise<Client> {
    return this.exclusive(async () => {
      if (this.current) return this.current;
      const errors: string[] = [];
      const n = this.addrs.length;
      for (let i = 0; i < n; i++) {
        const idx = (this.idx + i) % n;
        const addr = this.addrs[idx]!;
        let cli: Client | undefined;
        try {
          cli = this.dial(addr);
          await cli.ready(this.probeMs);
        } catch (err) {
          errors.push(`${addr}: ${messageOf(err)}`);
          cli?.close();
          continue;
        }
        this.current = cli;
        this.idx = idx;
        this.addr = addr;
        return cli;
      }
      throw new StatusError("UNAVAILABLE", `all cache seeds failed:\n  ${errors.join("\n  ")}`);
    });
  }

  private async invalidate(): Promise<void> {
    return this.exclusive(async () => {
      this.current?.close();
      this.current = undefined;
      this.idx = (this.idx + 1) % this.addrs.length;
      this.addr = undefined;
    });
  }

  private async exclusive<T>(fn: () => Promise<T>): Promise<T> {
    const prev = this.chain;
    let release: () => void = () => {};
    this.chain = new Promise<void>((resolve) => {
      release = resolve;
    });
    await prev;
    try {
      return await fn();
    } finally {
      release();
    }
  }
}

function messageOf(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export interface Session extends CacheApi {}
