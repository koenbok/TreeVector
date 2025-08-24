import { describe, expect, it } from "bun:test";
import { BPlusTree } from "./BPlusTree";
import { MemoryStore } from "./Store";

describe("BPlusTree", () => {
	it("inserts and gets by index in order", async () => {
		const store = new MemoryStore<number>();
		const tree = new BPlusTree<number>(store, 4, 8);
		const nums = [5, 1, 3, 2, 4, 6, 0, 7, 9, 8];
		for (const n of nums) {
			await tree.insert(n);
		}
		for (let i = 0; i < nums.length; i++) {
			expect(await tree.get(i)).toBe(i);
		}
	});

	it("scan query returns sorted values within bounds", async () => {
		const store = new MemoryStore<number>();
		const tree = new BPlusTree<number>(store, 3, 4);
		const values = [10, 2, 7, 5, 1, 3, 9, 6, 4, 8];
		for (const v of values) await tree.insert(v);
		const out = await tree.scan(3, 7);
		expect(out).toEqual([3, 4, 5, 6, 7]);
	});

	it("handles duplicates by stable insertion order", async () => {
		const store = new MemoryStore<number>();
		const tree = new BPlusTree<number>(store, 3, 4);
		const values = [2, 2, 2, 1, 1, 3];
		for (const v of values) await tree.insert(v);
		// expect order: 1,1,2,2,2,3
		const got = await Promise.all(
			Array.from({ length: values.length }, (_, i) => tree.get(i)),
		);
		expect(got).toEqual([1, 1, 2, 2, 2, 3]);
		const range = await tree.scan(2, 2);
		expect(range).toEqual([2, 2, 2]);
	});

	it("range(min,max) returns values by index slice [min,max)", async () => {
		const store = new MemoryStore<number>();
		const tree = new BPlusTree<number>(store, 4, 8);
		for (const v of [10, 20, 30, 40, 50]) await tree.insert(v);
		expect(await tree.range(1, 4)).toEqual([20, 30, 40]);
		expect(await tree.range(0, 2)).toEqual([10, 20]);
		expect(await tree.range(4, 5)).toEqual([50]);
		expect(await tree.range(5, 6)).toEqual([]);
		expect(await tree.range(3, 3)).toEqual([]);
	});

	it("getIndex returns lower_bound for values and duplicates", async () => {
		const store = new MemoryStore<number>();
		const tree = new BPlusTree<number>(store, 4, 8);
		const values = [5, 1, 3, 2, 4, 2, 5];
		for (const v of values) await tree.insert(v);
		// Sorted: [1,2,2,3,4,5,5]
		expect(await tree.getIndex(0)).toBe(0);
		expect(await tree.getIndex(1)).toBe(0);
		expect(await tree.getIndex(2)).toBe(1);
		expect(await tree.getIndex(3)).toBe(3);
		expect(await tree.getIndex(4)).toBe(4);
		expect(await tree.getIndex(5)).toBe(5);
		expect(await tree.getIndex(6)).toBe(7);
	});

	it("getIndex on empty tree and extremes", async () => {
		const store = new MemoryStore<number>();
		const tree = new BPlusTree<number>(store, 4, 8);
		expect(await tree.getIndex(10)).toBe(0);
		// After some inserts
		for (const v of [10, 20, 20, 30]) await tree.insert(v);
		expect(await tree.getIndex(5)).toBe(0);
		expect(await tree.getIndex(10)).toBe(0);
		expect(await tree.getIndex(15)).toBe(1);
		expect(await tree.getIndex(20)).toBe(1);
		expect(await tree.getIndex(25)).toBe(3);
		expect(await tree.getIndex(1000)).toBe(4);
	});

	it("getIndex scales across multiple leaves and internal nodes", async () => {
		const store = new MemoryStore<number>();
		// small maxValues to force many splits
		const tree = new BPlusTree<number>(store, 4, 4);
		const N = 1000;
		for (let i = 1; i <= N; i++) await tree.insert(i);
		expect(await tree.getIndex(1)).toBe(0);
		expect(await tree.getIndex(2)).toBe(1);
		expect(await tree.getIndex(50)).toBe(49);
		expect(await tree.getIndex(1000)).toBe(999);
		expect(await tree.getIndex(1001)).toBe(1000);
	});
});
