import { FenwickOrderedList } from "../src/FenwickOrderedList";
import { MemoryStore } from "../src/Store";

// Defaults (override with CLI: bun run bench/ordered-list.ts 1000000 8192)
const TOTAL = Number(process.argv[2] ?? 1_000_000);
const MAX_PER_SEGMENT = Number(process.argv[3] ?? 8192);
const SEGMENTS_PER_CHUNK = process.argv[4]
	? Number(process.argv[4])
	: undefined;
const DUP_RATE = 0.05; // 5% duplicates → ~95% unique

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

function buildValues(count: number, dupRate: number): number[] {
	const seed = 0x1ee7_c0de ^ count;
	const rng = mulberry32(seed);
	const arr = new Array<number>(count);
	for (let i = 0; i < count; i++) {
		if (i > 0 && rng() < dupRate) {
			const j = Math.floor(rng() * i);
			arr[i] = arr[j] as number;
		} else {
			// Generate a new unique-ish number (hash reduces clustering, mix i and seed)
			arr[i] = hash32(i ^ seed);
		}
	}
	return arr;
}

function makeHistogram(
	values: number[],
	numBins = 10,
): { ranges: [number, number][]; counts: number[] } {
	if (values.length === 0) return { ranges: [], counts: [] };
	let min = Number.POSITIVE_INFINITY;
	let max = Number.NEGATIVE_INFINITY;
	for (const v of values) {
		if (v < min) min = v;
		if (v > max) max = v;
	}
	if (min === max) return { ranges: [[min, max]], counts: [values.length] };
	const binSize = (max - min) / numBins;
	const counts = new Array<number>(numBins).fill(0);
	for (const v of values) {
		let bin = Math.floor((v - min) / binSize);
		if (bin >= numBins) bin = numBins - 1; // guard max edge
		counts[bin] += 1;
	}
	const ranges: [number, number][] = [];
	for (let i = 0; i < numBins; i++) {
		const lo = Math.floor(min + i * binSize);
		const hi = Math.floor(i === numBins - 1 ? max : min + (i + 1) * binSize);
		ranges.push([lo, hi]);
	}
	return { ranges, counts };
}

async function main(): Promise<void> {
	console.log("=== FenwickOrderedList Bench ===");
	console.log(
		`values: ${formatNumber(TOTAL)}  (≈${Math.round((1 - DUP_RATE) * 100)}% unique)`,
	);
	console.log(`maxValuesPerSegment: ${formatNumber(MAX_PER_SEGMENT)}`);

	const values = buildValues(TOTAL, DUP_RATE);
	const store = new MemoryStore();
	const list = new FenwickOrderedList<number>(
		store,
		MAX_PER_SEGMENT,
		SEGMENTS_PER_CHUNK as number,
		(a, b) => (a < b ? -1 : a > b ? 1 : 0),
	);

	// Insert and time
	const t0 = performance.now();
	for (let i = 0; i < values.length; i++) {
		// eslint-disable-next-line no-await-in-loop
		await list.insert(values[i] as number);
		if ((i + 1) % 100_000 === 0) {
			const dt = performance.now() - t0;
			const ips = (i + 1) / (dt / 1000);
			console.log(
				`inserted ${formatNumber(i + 1)} in ${dt.toFixed(0)} ms — inserts/s ${formatNumber(Math.floor(ips))}`,
			);
		}
	}
	const t1 = performance.now();
	const insertMs = t1 - t0;
	const insertsPerSec = values.length / (insertMs / 1000);
	console.log(
		`Insert time: ${insertMs.toFixed(0)} ms — inserts/s ${formatNumber(Math.floor(insertsPerSec))}`,
	);

	// Verify order
	const checkStart = performance.now();
	const ordered = await list.range(0, values.length);
	let ok = true;
	for (let i = 1; i < ordered.length; i++) {
		if ((ordered[i - 1] as number) > (ordered[i] as number)) {
			ok = false;
			break;
		}
	}
	const checkMs = performance.now() - checkStart;
	console.log(`Order check: ${ok ? "OK" : "FAIL"} — ${checkMs.toFixed(0)} ms`);

	// Segment distribution (persist segments and fetch sizes)
	const dirtyKeys = await list.flush();
	const segSizes: number[] = [];
	for (const key of dirtyKeys) {
		// eslint-disable-next-line no-await-in-loop
		const rec =
			(await store.get<{ segments?: Record<number, number[]> }>(key)) ?? {};
		if (rec.segments) {
			for (const arr of Object.values(rec.segments)) segSizes.push(arr.length);
		} else {
			const seg = (await store.get<number[]>(key)) ?? [];
			segSizes.push(seg.length);
		}
	}
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
				Math.floor((counts[i] as number) / Math.max(1, segTotal / 60)),
			),
		);
		console.log(
			`  [${lo.toString().padStart(5)}, ${hi.toString().padStart(5)}]:`.padEnd(
				20,
			),
			`${formatNumber(counts[i] as number)}`.padStart(8),
			bar,
		);
	}

	// Range timings (10k)
	const RANGES = 10_000;
	const rng = mulberry32(0xabad1dea);
	const tRangeStart = performance.now();
	let totalRangeLen = 0;
	for (let i = 0; i < RANGES; i++) {
		const start = Math.floor(rng() * TOTAL);
		const lenRand = rng();
		const len =
			lenRand < 0.6
				? Math.floor(rng() * 10) + 1
				: lenRand < 0.9
					? Math.floor(rng() * 100) + 1
					: Math.floor(rng() * 1000) + 1;
		const end = Math.min(TOTAL, start + len);
		// eslint-disable-next-line no-await-in-loop
		const r = await list.range(start, end);
		totalRangeLen += r.length;
	}
	const tRangeMs = performance.now() - tRangeStart;
	console.log(
		`Range x${formatNumber(RANGES)}: ${tRangeMs.toFixed(0)} ms — ops/s ${formatNumber(Math.floor(RANGES / (tRangeMs / 1000)))} — avg len ${(totalRangeLen / RANGES).toFixed(1)}`,
	);

	// Scan timings (10k)
	const tScanStart = performance.now();
	let totalScanLen = 0;
	for (let i = 0; i < RANGES; i++) {
		const aIdx = Math.floor(rng() * TOTAL);
		const bIdx = Math.min(TOTAL - 1, aIdx + Math.floor(rng() * 1000));
		const a = ordered[aIdx] as number;
		const b = ordered[bIdx] as number;
		const min = Math.min(a, b);
		const max = Math.max(a, b);
		// eslint-disable-next-line no-await-in-loop
		const r = await list.scan(min, max);
		totalScanLen += r.length;
	}
	const tScanMs = performance.now() - tScanStart;
	console.log(
		`Scan x${formatNumber(RANGES)}: ${tScanMs.toFixed(0)} ms — ops/s ${formatNumber(Math.floor(RANGES / (tScanMs / 1000)))} — avg len ${(totalScanLen / RANGES).toFixed(1)}`,
	);
}

// eslint-disable-next-line unicorn/prefer-top-level-await
main().catch((err) => {
	console.error(err);
	process.exitCode = 1;
});
