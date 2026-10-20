/**
 * Minimal CacheOnly put/get.
 * Build the package first (`npm run build`) and run with Node 22+
 * from clients/node: `node --experimental-strip-types examples/kv.ts`
 * after the demo keyspace node is listening on 127.0.0.1:9000.
 */
import { dial } from "../dist/src/index.js";

const client = dial("127.0.0.1:9000");
try {
  await client.put("cacheonly", "greeting", "hello");
  const value = await client.get("cacheonly", "greeting");
  console.log(Buffer.from(value).toString("utf8"));
} finally {
  client.close();
}
