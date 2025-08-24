import { Table } from "./Table";
import { BasicIndexedColumn, BPlusTreeSortedColumn } from "./Column";
import { MemoryStore } from "./Store";

type Row = { id: number; a: number; b: number; c: number };

const TOTAL_ROWS = 1_000_000;
const BATCH_SIZE = 10_000;

// Deterministic PRNGs (copied from bench.ts for consistency)
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

async function main(): Promise<void> {
	const store = new MemoryStore<number>();
	const table = new Table<number>(
		store,
		{ key: "id", column: new BPlusTreeSortedColumn<number>(store, 64, 10_000) },
		{
			a: new BasicIndexedColumn<number>(),
			b: new BasicIndexedColumn<number>(),
			c: new BasicIndexedColumn<number>(),
		},
	);

	// Build deterministic rows: ~1 in 20 ids duplicates a prior id
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

	const start = performance.now();
	const batch: Row[] = [];
	for (let i = 0; i < TOTAL_ROWS; i++) {
		batch.push({
			id: ids[i] as number,
			a: hash32(i) & 0xffff,
			b: hash32(i + 1) & 0xffff,
			c: hash32(i + 2) & 0xffff,
		});
		if (batch.length === BATCH_SIZE) {
			console.log(`inserting batch ${i / BATCH_SIZE}`);
			await table.insert(batch);
			batch.length = 0;
		}
	}
	if (batch.length > 0) {
		await table.insert(batch);
	}
	const end = performance.now();

	const ms = end - start;
	const secs = ms / 1000;
	const rps = TOTAL_ROWS / secs;

	console.log("\n=== Table Bench ===");
	console.log(`rows: ${formatNumber(TOTAL_ROWS)}`);
	console.log(`insert total: ${ms.toFixed(2)} ms (${secs.toFixed(2)} s)`);
	console.log(`rows/s: ${formatNumber(Math.floor(rps))}`);
}

// eslint-disable-next-line unicorn/prefer-top-level-await
main().catch((err) => {
	console.error(err);
	process.exitCode = 1;
});
