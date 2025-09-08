import {
  FenwickColumn,
  FenwickOrderedColumn,
  type IndexedColumnInterface,
  type OrderedColumnInterface,
} from "../src/Column";
import { MemoryStore } from "../src/Store";

// Defaults (override with CLI)
// bun run bench/columns.ts [TOTAL] [SEGMENT_MAX] [CHUNK_SEGMENTS] [DUP_RATE]
const TOTAL = Number(process.argv[2] ?? 300_000);
const MAX_PER_SEGMENT = Number(process.argv[3] ?? 8192);
const SEGMENTS_PER_CHUNK = process.argv[4]
  ? Number(process.argv[4])
  : undefined;
const DUP_RATE = Number(process.argv[5] ?? 0.05);

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
  let x = input >>> 0;
  x ^= x >>> 16;
  x = Math.imul(x, 0x7feb352d);
  x ^= x >>> 15;
  x = Math.imul(x, 0x846ca68b);
  x ^= x >>> 16;
  return x >>> 0;
}

function buildValues(count: number, dupRate: number): number[] {
  const seed = 0x1ee7_c0de ^ count;
  const rng = mulberry32(seed);
  const arr = new Array<number>(count);
  for (let i = 0; i < count; i++) {
    if (i > 0 && rng() < dupRate) {
      const j = Math.floor(rng() * i);
      arr[i] = arr[j] as number;
    } else {
      arr[i] = hash32(i ^ seed);
    }
  }
  return arr;
}

function formatNumber(n: number): string {
  return Intl.NumberFormat("en-US").format(n);
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(
    sorted.length - 1,
    Math.max(0, Math.floor((p / 100) * (sorted.length - 1))),
  );
  return sorted[idx] as number;
}

function makeHistogram(values: number[], targetBins = 12): { ranges: [number, number][]; counts: number[] } {
    if (values.length === 0) return { ranges: [], counts: [] };
    const sorted = [...values].sort((a, b) => a - b);
    const n = sorted.length;
    const bins = Math.min(targetBins, Math.max(1, Math.ceil(Math.log2(n + 1))));
    const counts = new Array<number>(bins).fill(0);
    const ranges: [number, number][] = new Array(bins);
    // Quantile-based bins for more meaningful distribution
    for (let i = 0; i < bins; i++) {
        const loIdx = Math.floor((i / bins) * (n - 1));
        const hiIdx = Math.floor(((i + 1) / bins) * (n - 1));
        const lo = sorted[loIdx] as number;
        const hi = sorted[hiIdx] as number;
        ranges[i] = [lo, hi];
    }
    // Tally counts by range
    let bin = 0;
    for (let i = 0; i < n; i++) {
        const v = sorted[i] as number;
        while (bin < bins - 1 && v > (ranges[bin]![1] as number)) bin += 1;
        counts[bin] = ((counts[bin] ?? 0) as number) + 1;
    }
    return { ranges, counts };
}

function collectSegmentSizesFromMeta<T>(
    column: { getMeta(): { segments: { count: number }[] } },
): number[] {
    return column.getMeta().segments.map((s) => s.count);
}

async function benchOrdered(
    column: OrderedColumnInterface<number>,
    values: number[],
    mode: "append" | "random",
    store: MemoryStore,
): Promise<void> {
  console.log("\n=== OrderedColumn (FenwickOrderedColumn) ===");
  console.log(
    `values: ${formatNumber(values.length)} (dupRate=${DUP_RATE}, mode=${mode})`,
  );

  const vals = mode === "append" ? [...values].sort((a, b) => a - b) : values;

  const t0 = performance.now();
  for (let i = 0; i < vals.length; i++) {
    // eslint-disable-next-line no-await-in-loop
    await column.insert(vals[i] as number);
    if ((i + 1) % 100_000 === 0) {
      const dt = performance.now() - t0;
      const ips = (i + 1) / (dt / 1000);
      console.log(
        `inserted ${formatNumber(i + 1)} in ${dt.toFixed(0)} ms — inserts/s ${formatNumber(Math.floor(ips))}`,
      );
    }
  }
  const insertMs = performance.now() - t0;
  console.log(
    `Insert total: ${insertMs.toFixed(0)} ms — inserts/s ${formatNumber(
      Math.floor(vals.length / (insertMs / 1000)),
    )}`,
  );

  // Range timings (10k)
  const RANGES = 10_000;
  const rng = mulberry32(0xcafebabe);
  const tRangeStart = performance.now();
  let totalRangeLen = 0;
  for (let i = 0; i < RANGES; i++) {
    const start = Math.floor(rng() * vals.length);
    const lenRand = rng();
    const len =
      lenRand < 0.6
        ? Math.floor(rng() * 10) + 1
        : lenRand < 0.9
          ? Math.floor(rng() * 100) + 1
          : Math.floor(rng() * 1000) + 1;
    const end = Math.min(vals.length, start + len);
    // eslint-disable-next-line no-await-in-loop
    const r = await column.range(start, end);
    totalRangeLen += r.length;
  }
  const tRangeMs = performance.now() - tRangeStart;
  console.log(
    `Range x${formatNumber(RANGES)}: ${tRangeMs.toFixed(0)} ms — ops/s ${formatNumber(
      Math.floor(RANGES / (tRangeMs / 1000)),
    )} — avg len ${(totalRangeLen / RANGES).toFixed(1)}`,
  );

  // Scan timings (10k)
  const ordered = await column.range(0, vals.length);
  const tScanStart = performance.now();
  let totalScanLen = 0;
  for (let i = 0; i < RANGES; i++) {
    const aIdx = Math.floor(rng() * vals.length);
    const bIdx = Math.min(vals.length - 1, aIdx + Math.floor(rng() * 1000));
    const a = ordered[aIdx] as number;
    const b = ordered[bIdx] as number;
    const min = Math.min(a, b);
    const max = Math.max(a, b);
    // eslint-disable-next-line no-await-in-loop
    const r = await column.scan(min, max);
    totalScanLen += r.length;
  }
  const tScanMs = performance.now() - tScanStart;
  console.log(
    `Scan x${formatNumber(RANGES)}: ${tScanMs.toFixed(0)} ms — ops/s ${formatNumber(
      Math.floor(RANGES / (tScanMs / 1000)),
    )} — avg len ${(totalScanLen / RANGES).toFixed(1)}`,
  );

  // Segment distribution (from meta)
  await column.flush();
  const segSizes = collectSegmentSizesFromMeta(column);
  segSizes.sort((a, b) => a - b);
  const segTotal = segSizes.length;
  const segMin = segSizes[0] ?? 0;
  const segMax = segSizes[segSizes.length - 1] ?? 0;
  const p50 = percentile(segSizes, 50);
  const p90 = percentile(segSizes, 90);
  const p99 = percentile(segSizes, 99);
  console.log(
    `Segments: ${formatNumber(segTotal)} — min ${segMin}, p50 ${p50}, p90 ${p90}, p99 ${p99}, max ${segMax}`,
  );
  const { ranges, counts } = makeHistogram(
    segSizes,
    Math.min(12, Math.max(4, Math.ceil(Math.log2(segTotal + 1)))),
  );
  console.log("Histogram (segment sizes):");
  for (let i = 0; i < ranges.length; i++) {
    const [lo, hi] = ranges[i] as [number, number];
    const bar = "#".repeat(
      Math.max(
        1,
        Math.floor(((counts[i] ?? 0) as number) / Math.max(1, segTotal / 60)),
      ),
    );
    console.log(
      `  [${lo.toString().padStart(5)}, ${hi.toString().padStart(5)}]:`.padEnd(
        20,
      ),
      `${formatNumber((counts[i] ?? 0) as number)}`.padStart(8),
      bar,
    );
  }
}

async function benchIndexed(
    column: IndexedColumnInterface<number>,
    values: number[],
    mode: "append" | "random",
    store: MemoryStore,
): Promise<void> {
  console.log("\n=== IndexedColumn (FenwickColumn) ===");
  console.log(`values: ${formatNumber(values.length)} (mode=${mode})`);

  const rng = mulberry32(0xabad1dea);
  const t0 = performance.now();
  for (let i = 0; i < values.length; i++) {
    const idx = mode === "append" ? i : Math.floor(rng() * (i + 1));
    // eslint-disable-next-line no-await-in-loop
    await column.insertAt(idx, values[i] as number);
    if ((i + 1) % 100_000 === 0) {
      const dt = performance.now() - t0;
      const ips = (i + 1) / (dt / 1000);
      console.log(
        `inserted ${formatNumber(i + 1)} in ${dt.toFixed(0)} ms — inserts/s ${formatNumber(Math.floor(ips))}`,
      );
    }
  }
  const insertMs = performance.now() - t0;
  console.log(
    `Insert total: ${insertMs.toFixed(0)} ms — inserts/s ${formatNumber(
      Math.floor(values.length / (insertMs / 1000)),
    )}`,
  );

  // Range timings (10k)
  const RANGES = 10_000;
  const tRangeStart = performance.now();
  let totalRangeLen = 0;
  for (let i = 0; i < RANGES; i++) {
    const start = Math.floor(rng() * values.length);
    const lenRand = rng();
    const len =
      lenRand < 0.6
        ? Math.floor(rng() * 10) + 1
        : lenRand < 0.9
          ? Math.floor(rng() * 100) + 1
          : Math.floor(rng() * 1000) + 1;
    const end = Math.min(values.length, start + len);
    // eslint-disable-next-line no-await-in-loop
    const r = await column.range(start, end);
    totalRangeLen += r.length;
  }
  const tRangeMs = performance.now() - tRangeStart;
  console.log(
    `Range x${formatNumber(RANGES)}: ${tRangeMs.toFixed(0)} ms — ops/s ${formatNumber(
      Math.floor(RANGES / (tRangeMs / 1000)),
    )} — avg len ${(totalRangeLen / RANGES).toFixed(1)}`,
  );

  // Segment distribution (from meta)
  await column.flush();
  const segSizes = collectSegmentSizesFromMeta(column);
  segSizes.sort((a, b) => a - b);
  const segTotal = segSizes.length;
  const segMin = segSizes[0] ?? 0;
  const segMax = segSizes[segSizes.length - 1] ?? 0;
  const p50 = percentile(segSizes, 50);
  const p90 = percentile(segSizes, 90);
  const p99 = percentile(segSizes, 99);
  console.log(
    `Segments: ${formatNumber(segTotal)} — min ${segMin}, p50 ${p50}, p90 ${p90}, p99 ${p99}, max ${segMax}`,
  );
  const { ranges, counts } = makeHistogram(
    segSizes,
    Math.min(12, Math.max(4, Math.ceil(Math.log2(segTotal + 1)))),
  );
  console.log("Histogram (segment sizes):");
  for (let i = 0; i < ranges.length; i++) {
    const [lo, hi] = ranges[i] as [number, number];
    const bar = "#".repeat(
      Math.max(
        1,
        Math.floor(((counts[i] ?? 0) as number) / Math.max(1, segTotal / 60)),
      ),
    );
    console.log(
      `  [${lo.toString().padStart(5)}, ${hi.toString().padStart(5)}]:`.padEnd(
        20,
      ),
      `${formatNumber((counts[i] ?? 0) as number)}`.padStart(8),
      bar,
    );
  }
}

async function main(): Promise<void> {
  console.log("=== Columns Bench ===");
  console.log(
    `TOTAL=${formatNumber(TOTAL)}, maxPerSegment=${formatNumber(MAX_PER_SEGMENT)}, chunkSegments=${SEGMENTS_PER_CHUNK ?? "auto"}`,
  );

  // Ordered column (append and random)
  const orderedValues = buildValues(TOTAL, DUP_RATE);
  for (const mode of ["append", "random"] as const) {
    const store = new MemoryStore();
    const ordered = new FenwickOrderedColumn<number>(store, {
      segmentCount: MAX_PER_SEGMENT,
      chunkCount: SEGMENTS_PER_CHUNK as number,
    });
    await benchOrdered(ordered, orderedValues, mode, store);
  }

  // Indexed column (append and random)
  const indexedValues = new Array<number>(TOTAL);
  for (let i = 0; i < TOTAL; i++) indexedValues[i] = hash32(i);
  for (const mode of ["append", "random"] as const) {
    const store = new MemoryStore();
    const indexed = new FenwickColumn<number>(store, {
      segmentCount: MAX_PER_SEGMENT,
      chunkCount: SEGMENTS_PER_CHUNK as number,
    });
    await benchIndexed(indexed, indexedValues, mode, store);
  }
}

// eslint-disable-next-line unicorn/prefer-top-level-await
main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
