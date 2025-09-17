import { createReadStream } from "node:fs";
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { createGunzip } from "node:zlib";
import { Queue } from "../src/Queue";
import { Table } from "../src/Table";
import { OrderedColumn } from "../src/Column";
import { MemoryStore } from "../src/Store";

/* -------------------------------------------------------------------------- */
/*                               Type helpers                                 */
/* -------------------------------------------------------------------------- */

type SchemaType = "integer" | "string" | "timestamp" | "boolean";
export type SchemaDefinition = Record<
  string,
  SchemaType | Record<string, SchemaType>
>;
export type FlattenedRow = Record<string, string | number | undefined>;

/* -------------------------------------------------------------------------- */
/*                               Pure helpers                                 */
/* -------------------------------------------------------------------------- */

const loadSchema = async (dataDir: string): Promise<SchemaDefinition> => {
  return JSON.parse(await readFile(join(dataDir, "schema.json"), "utf8"));
};

const convertValue = (
  value: unknown,
  type: SchemaType,
): string | number | undefined => {
  if (value === null || value === undefined) return undefined;

  switch (type) {
    case "integer": {
      const num = Number(value);
      return Number.isNaN(num) ? undefined : num;
    }
    case "timestamp": {
      if (typeof value === "string") {
        const date = new Date(value);
        return Number.isNaN(date.getTime()) ? undefined : date.getTime();
      }
      const num = Number(value);
      return Number.isNaN(num) ? undefined : num;
    }
    case "boolean":
      return typeof value === "boolean"
        ? value
          ? 1
          : 0
        : String(value).toLowerCase() === "true"
          ? 1
          : 0;
    default:
      return String(value);
  }
};

const flattenObject = (
  obj: unknown,
  schema: SchemaDefinition,
  prefix = "",
): FlattenedRow => {
  const out: FlattenedRow = {};

  for (const [key, schemaVal] of Object.entries(schema)) {
    const flatKey = prefix ? `${prefix}_${key}` : key;
    const val = (obj as Record<string, unknown>)?.[key];

    if (typeof schemaVal === "object") {
      Object.assign(out, flattenObject(val ?? {}, schemaVal, flatKey));
    } else {
      out[flatKey] = convertValue(val, schemaVal);
    }
  }

  return out;
};

/* -------------------------------------------------------------------------- */
/*                             Streaming helpers                              */
/* -------------------------------------------------------------------------- */

const readDataFiles = async (dataDir: string): Promise<string[]> => {
  const files = (await readdir(dataDir))
    .filter((f) => f.endsWith(".jsonl.gz"))
    .map((name) => {
      const num = Number.parseInt(name.split(".")[0], 10);
      if (Number.isNaN(num)) {
        throw new Error(`Invalid file name: ${name}`);
      }
      return { name, num };
    })
    .sort((a, b) => a.num - b.num)
    .map(({ name }) => join(dataDir, name));

  console.log(`Found ${files.length} data files.`);
  return files;
};

const processFile = async (
  file: string,
  schema: SchemaDefinition,
  table: Table<number>,
  batchSize: number,
  maxRecords?: number,
): Promise<number> => {
  let batch: FlattenedRow[] = [];
  let count = 0;
  let lastBatchTime = Date.now();

  const flushBatch = async () => {
    if (!batch.length) return;

    const size = batch.length;

    // Ensure deterministic order by timestamp before insertion
    batch.sort(
      (a, b) => (a.data_timestamp as number) - (b.data_timestamp as number),
    );

    await table.insert(batch);
    await table.flush("test");

    // Performance metrics
    const now = Date.now();
    const sec = (now - lastBatchTime) / 1000;
    const rps = sec > 0 ? size / sec : 0;
    console.log(
      `  Inserted batch of ${size} records - ${rps.toFixed(0)} rows/sec`,
    );
    lastBatchTime = now;

    batch = [];
  };

  for await (const line of createInterface({
    input: createReadStream(file).pipe(createGunzip()),
    crlfDelay: Number.POSITIVE_INFINITY,
  })) {
    if (!line.trim()) continue;
    if (maxRecords && count >= maxRecords) break;

    batch.push(flattenObject(JSON.parse(line), schema));
    count++;
    if (batch.length >= batchSize) await flushBatch();
  }

  await flushBatch();
  console.log(`Processed ${count} rows from ${file}`);
  return count;
};

/* -------------------------------------------------------------------------- */
/*                                 Ingest                                     */
/* -------------------------------------------------------------------------- */

interface IngestOptions {
  dataDir: string;
  table: Table<number>;
  batchSize: number;
  maxFiles?: number;
  maxRecordsPerFile?: number;
}

const ingest = async (opts: IngestOptions): Promise<void> => {
  const schema = await loadSchema(opts.dataDir);
  const files = await readDataFiles(opts.dataDir);
  const targets = opts.maxFiles ? files.slice(0, opts.maxFiles) : files;

  let total = 0;
  const start = Date.now();

  for (const file of targets) {
    total += await processFile(
      file,
      schema,
      opts.table,
      opts.batchSize,
      opts.maxRecordsPerFile,
    );
  }

  const seconds = (Date.now() - start) / 1000;
  console.log(
    `\nIngested ${total} rows in ${seconds.toFixed(2)}s (${(total / seconds).toFixed(0)} rows/sec).`,
  );
};

/* -------------------------------------------------------------------------- */
/*                                   Main                                     */
/* -------------------------------------------------------------------------- */

async function main() {
  const dataDir = join(import.meta.dirname, "project_published_7m");
  const sortColumn = "data_timestamp";
  const storeDir = `data/ingest-${Date.now()}`;
  // Redis URL will be determined by environment variables (VALKEY_ENDPOINT, VALKEY_PORT, VALKEY_TLS)
  // or VALKEY_URL if provided - see getRedisClient() for details
  console.log(`Using store directory: ${storeDir}`);

  // const queue = new Queue(32);
  // // const redisStore = new RedisStore().withPrefix(
  // // 	`data/ingest-${Date.now()}`,
  // // );
  // // const fileStore = new FileStore(storeDir);
  // // const brotliStore = new BrotliStore(fileStore);
  // // const queueStore = new QueueStore(brotliStore, queue);
  const store = new MemoryStore();
  const table = new Table<number>(store, { key: sortColumn, column: new OrderedColumn<number>(store, { segmentCount: 200_000 }) });

  await ingest({ dataDir, table, batchSize: 100_000 });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
