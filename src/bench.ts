import { BPlusTree } from "./BPlusTree";
import { MemoryStore } from "./Store";

type BenchConfig = { maxValues: number; maxChildren: number };

const TOTAL = 1_000_000;
const CONFIGS: BenchConfig[] = [];

const maxChildrens = [8, 64];
const maxValueses = [1_000, 10_000, 100_000];

for (const maxChildren of maxChildrens) {
	for (const maxValues of maxValueses) {
		CONFIGS.push({ maxValues, maxChildren });
	}
}

function formatNumber(n: number): string {
	return Intl.NumberFormat("en-US").format(n);
}

// Deterministic PRNGs
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
	// Simple 32-bit mix to generate unique-looking numbers deterministically
	let x = input;
	x = x ^ 61 ^ (x >>> 16);
	x = x + (x << 3);
	x = x ^ (x >>> 4);
	x = Math.imul(x, 0x27d4eb2d);
	x = x ^ (x >>> 15);
	return x >>> 0;
}

function bucketDistribution(
	dist: Map<number, number>,
	bucketTarget = 32,
): Array<{ start: number; end: number; count: number }> {
	if (dist.size === 0) return [];
	const sizes = Array.from(dist.keys()).sort((a, b) => a - b);
	const minSize = sizes[0] as number;
	const maxSize = sizes[sizes.length - 1] as number;
	const totalLeaves = Array.from(dist.values()).reduce((a, b) => a + b, 0);
	const bins = Math.max(
		8,
		Math.min(bucketTarget, Math.ceil(Math.sqrt(totalLeaves))),
	);
	const width = Math.max(1, Math.ceil((maxSize - minSize + 1) / bins));
	const buckets = new Map<number, number>();
	for (const [size, count] of dist.entries()) {
		const bucketStart = minSize + Math.floor((size - minSize) / width) * width;
		buckets.set(bucketStart, (buckets.get(bucketStart) ?? 0) + count);
	}
	const out: Array<{ start: number; end: number; count: number }> = [];
	const starts = Array.from(buckets.keys()).sort((a, b) => a - b);
	for (const start of starts) {
		out.push({ start, end: start + width - 1, count: buckets.get(start) ?? 0 });
	}
	return out;
}

async function runOnce(config: BenchConfig): Promise<void> {
	const store = new MemoryStore<number>();
	const tree = new BPlusTree<number>(
		store,
		config.maxValues,
		config.maxChildren,
	);

	// Build deterministic values with ~95% unique: 1 in 20 positions duplicates a prior value
	const seed =
		0x12345678 ^ (config.maxValues * 31) ^ (config.maxChildren * 131);
	const rng = mulberry32(seed);
	const values = new Array<number>(TOTAL);
	const counts = new Map<number, number>();
	let minVal = Number.POSITIVE_INFINITY;
	let maxVal = Number.NEGATIVE_INFINITY;
	for (let i = 0; i < TOTAL; i++) {
		let v: number;
		if (i > 0 && i % 20 === 0) {
			// duplicate from an earlier index
			const j = Math.floor(rng() * i);
			v = values[j] as number;
		} else {
			// deterministically unique-like number via mixing i and seed
			v = hash32(i ^ seed);
		}
		values[i] = v;
		if (v < minVal) minVal = v;
		if (v > maxVal) maxVal = v;
		counts.set(v, (counts.get(v) ?? 0) + 1);
	}

	const start = performance.now();
	for (let i = 0; i < TOTAL; i++) {
		await tree.insert(values[i] as number);
	}
	const end = performance.now();

	const ms = end - start;
	const secs = ms / 1000;
	const ips = TOTAL / secs;

	// Verify the inserted sort order by comparing the tree's in-order output to the multiset counts
	const rangeFullStart = performance.now();
	const sortedOut = await tree.range(minVal, maxVal);
	const rangeFullEnd = performance.now();
	const rangeFullMs = rangeFullEnd - rangeFullStart;
	const rangeFullPerSec = sortedOut.length / (rangeFullMs / 1000);

	let isNonDecreasing = true;
	let prev = sortedOut[0] as number;
	if (sortedOut.length !== TOTAL) isNonDecreasing = false;
	for (let i = 0; i < sortedOut.length; i++) {
		const cur = sortedOut[i] as number;
		if (i > 0 && cur < prev) {
			isNonDecreasing = false;
			break;
		}
		prev = cur;
		const c = counts.get(cur);
		if (c === undefined || c <= 0) {
			isNonDecreasing = false;
			break;
		}
		counts.set(cur, c - 1);
	}
	let allCountsZero = true;
	for (const val of counts.values()) {
		if (val !== 0) {
			allCountsZero = false;
			break;
		}
	}
	const verified = isNonDecreasing && allCountsZero;

	// Random range query performance
	const NUM_RANGES = 64;
	let totalRangeCount = 0;
	const rqStart = performance.now();
	for (let i = 0; i < NUM_RANGES; i++) {
		const a = values[Math.floor(rng() * values.length)] as number;
		const b = values[Math.floor(rng() * values.length)] as number;
		const lo = a <= b ? a : b;
		const hi = a <= b ? b : a;
		const out = await tree.range(lo, hi);
		totalRangeCount += out.length;
	}
	const rqEnd = performance.now();
	const rqMs = rqEnd - rqStart;
	const rqAvgMs = rqMs / NUM_RANGES;
	const rqItemsPerSec = totalRangeCount / (rqMs / 1000);

	const {
		totalNodes: total,
		internalNodes: internal,
		leafNodes: leaves,
		leafSizeDistribution: dist,
	} = tree.debugStats();
	const grouped = bucketDistribution(dist, 24);

	console.log("\n=== B+Tree Bench ===");
	console.log(
		`maxValues=${config.maxValues} maxChildren=${config.maxChildren}`,
	);
	console.log(`inserted: ${formatNumber(TOTAL)} values`);
	console.log(`insert total: ${ms.toFixed(2)} ms (${secs.toFixed(2)} s)`);
	console.log(`insert/s: ${formatNumber(Math.floor(ips))}`);
	console.log(`verified sorted order: ${verified}`);
	console.log(
		`range(full) ${rangeFullMs.toFixed(2)} ms, items/s: ${formatNumber(Math.floor(rangeFullPerSec))}`,
	);
	console.log(
		`ranges(${NUM_RANGES}) avg ${rqAvgMs.toFixed(2)} ms, total ${rqMs.toFixed(2)} ms, items/s: ${formatNumber(Math.floor(rqItemsPerSec))}`,
	);
	console.log(
		`nodes: total=${formatNumber(total)} internal=${formatNumber(internal)} leaves=${formatNumber(leaves)}`,
	);
	console.log("leaf size histogram (bucketed):");
	for (const b of grouped) {
		console.log(`  ${b.start}..${b.end} -> ${formatNumber(b.count)}`);
	}
}

async function main(): Promise<void> {
	console.log(
		`Running B+Tree benchmarks for ${CONFIGS.length} configuration(s) with ${formatNumber(TOTAL)} inserts each...`,
	);
	for (const cfg of CONFIGS) {
		await runOnce(cfg);
	}
}

// eslint-disable-next-line unicorn/prefer-top-level-await
main().catch((err) => {
	console.error(err);
	process.exitCode = 1;
});
