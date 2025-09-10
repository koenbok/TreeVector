import { Table } from "../src/Table";
import {
  OrderedColumn,
  IndexedColumn,
  type IndexedColumnInterface,
  type OrderedColumnInterface,
} from "../src/Column";
import { MemoryStore, type IStore } from "../src/Store";

type Row = { id: number; a: number; b: number; c: number };

const TOTAL_ROWS = 300_000;
const BATCH_SIZE = 10_000;

function mulberry32(seed: number): () => number {
  let t = seed >>> 0;
  return function rand(): number {
    t += 0x6d2b79f5;
    let x = t;
    x = Math.imul(x ^ (x >>> 15), x | 1);
    x ^= x + Math.imul(x ^ (x >>> 7), x | 61);
    return ((x ^ (x >>> 14)) >>> 0) / 4294967296;
  };
}

function hash32(input: number): number {
  let x = input;
  x = x ^ 61 ^ (x >>> 16);
  x = x + (x << 3);
  x = x ^ (x >>> 4);
  x = Math.imul(x, 0x27d4eb2d);
  x = x ^ (x >>> 15);
  return x >>> 0;
}

function formatNumber(n: number): string {
  return Intl.NumberFormat("en-US").format(n);
}

function buildRows(): Row[] {
  const seed = 0xc0ffee ^ 64 ^ 10_000;
  const rng = mulberry32(seed);
  const ids = new Array<number>(TOTAL_ROWS);
  for (let i = 0; i < TOTAL_ROWS; i++) {
    let v: number;
    if (i > 0 && i % 20 === 0) {
      const j = Math.floor(rng() * i);
      v = ids[j] as number;
    } else {
      v = hash32(i ^ seed);
    }
    ids[i] = v;
  }
  const rows: Row[] = new Array<Row>(TOTAL_ROWS);
  for (let i = 0; i < TOTAL_ROWS; i++) {
    rows[i] = {
      id: ids[i] as number,
      a: hash32(i) & 0xffff,
      b: hash32(i + 1) & 0xffff,
      c: hash32(i + 2) & 0xffff,
    };
  }
  return rows;
}

async function runScenario(
  label: string,
  orderFactory: (store: IStore) => OrderedColumnInterface<number>,
  indexedFactory: () => Record<string, IndexedColumnInterface<number>>,
  rows: Row[],
): Promise<{ ms: number; rps: number }> {
  const store = new MemoryStore();
  const table = new Table<number>(
    store,
    { key: "id", column: orderFactory(store) },
    indexedFactory(),
  );

  const start = performance.now();
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const batchIdx = Math.floor(i / BATCH_SIZE);
    const t0 = performance.now();
    await table.insert(batch);
    const t1 = performance.now();
    const msBatch = t1 - t0;
    const rpsBatch = batch.length / (msBatch / 1000);
    console.log(
      `${label}: batch ${batchIdx} — ${msBatch.toFixed(2)} ms, rows/s: ${formatNumber(
        Math.floor(rpsBatch),
      )}`,
    );
  }
  const end = performance.now();
  const ms = end - start;
  const secs = ms / 1000;
  const rps = rows.length / secs;
  return { ms, rps };
}

async function main(): Promise<void> {
  const rows = buildRows();
  const segArg = process.argv[2];
  const chunkArg = process.argv[3];
  const segList = (segArg ? segArg.split(",") : ["8192"]).map((s) => Number(s));
  const chunkList = (chunkArg ? chunkArg.split(",") : ["128"]).map((s) =>
    Number(s),
  );

  console.log("\n=== Table Bench (Fenwick) ===");
  console.log(`rows: ${formatNumber(TOTAL_ROWS)}`);

  for (const seg of segList) {
    for (const chk of chunkList) {
      const label = `fenwick(seg=${seg},chunk=${chk})`;
      const res = await runScenario(
        label,
        (store) =>
          new OrderedColumn<number>(store, {
            segmentCount: seg,
            chunkCount: chk,
          }),
        () => ({
          a: new IndexedColumn<number>(new MemoryStore(), {
            segmentCount: seg,
            chunkCount: chk,
          }),
          b: new IndexedColumn<number>(new MemoryStore(), {
            segmentCount: seg,
            chunkCount: chk,
          }),
          c: new IndexedColumn<number>(new MemoryStore(), {
            segmentCount: seg,
            chunkCount: chk,
          }),
        }),
        rows,
      );
      console.log(
        `${label}: total ${res.ms.toFixed(2)} ms (${(res.ms / 1000).toFixed(
          2,
        )} s), rows/s: ${formatNumber(Math.floor(res.rps))}`,
      );
    }
  }
}

// eslint-disable-next-line unicorn/prefer-top-level-await
main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
