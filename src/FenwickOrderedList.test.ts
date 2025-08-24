import { describe, expect, it } from "bun:test";
import { FenwickOrderedList } from "./FenwickOrderedList";
import { MemoryStore } from "./Store";

describe("FenwickOrderedList", () => {
	it("inserts and gets by index in order", async () => {
		const store = new MemoryStore<number>();
		const list = new FenwickOrderedList<number>(store, 1024);
		const nums = [5, 1, 3, 2, 4, 6, 0, 7, 9, 8];
		for (const n of nums) {
			await list.insert(n);
		}
		for (let i = 0; i < nums.length; i++) {
			expect(await list.get(i)).toBe(i);
		}
	});

	it("scan(min,max) returns sorted values within bounds", async () => {
		const store = new MemoryStore<number>();
		const list = new FenwickOrderedList<number>(store, 512);
		const values = [10, 2, 7, 5, 1, 3, 9, 6, 4, 8];
		for (const v of values) await list.insert(v);
		const out = await list.scan(3, 7);
		expect(out).toEqual([3, 4, 5, 6, 7]);
	});

	it("handles duplicates and keeps them contiguous", async () => {
		const store = new MemoryStore<number>();
		const list = new FenwickOrderedList<number>(store, 256);
		const values = [2, 2, 2, 1, 1, 3];
		for (const v of values) await list.insert(v);
		const got = await Promise.all(
			Array.from({ length: values.length }, (_, i) => list.get(i)),
		);
		expect(got).toEqual([1, 1, 2, 2, 2, 3]);
		const dupRange = await list.scan(2, 2);
		expect(dupRange).toEqual([2, 2, 2]);
	});

	it("range(min,max) returns values by index slice [min,max)", async () => {
		const store = new MemoryStore<number>();
		const list = new FenwickOrderedList<number>(store, 128);
		for (const v of [10, 20, 30, 40, 50]) await list.insert(v);
		expect(await list.range(1, 4)).toEqual([20, 30, 40]);
		expect(await list.range(0, 2)).toEqual([10, 20]);
		expect(await list.range(4, 5)).toEqual([50]);
		expect(await list.range(5, 6)).toEqual([]);
		expect(await list.range(3, 3)).toEqual([]);
	});

	it("getIndex returns lower_bound for values and duplicates", async () => {
		const store = new MemoryStore<number>();
		const list = new FenwickOrderedList<number>(store, 256);
		const values = [5, 1, 3, 2, 4, 2, 5];
		for (const v of values) await list.insert(v);
		// Sorted: [1,2,2,3,4,5,5]
		expect(await list.getIndex(0)).toBe(0);
		expect(await list.getIndex(1)).toBe(0);
		expect(await list.getIndex(2)).toBe(1);
		expect(await list.getIndex(3)).toBe(3);
		expect(await list.getIndex(4)).toBe(4);
		expect(await list.getIndex(5)).toBe(5);
		expect(await list.getIndex(6)).toBe(7);
	});

	it("getIndex scales across multiple segments", async () => {
		const store = new MemoryStore<number>();
		// small maxValues to force many splits into segments
		const list = new FenwickOrderedList<number>(store, 8);
		const N = 1000;
		for (let i = 1; i <= N; i++) await list.insert(i);
		expect(await list.getIndex(1)).toBe(0);
		expect(await list.getIndex(2)).toBe(1);
		expect(await list.getIndex(50)).toBe(49);
		expect(await list.getIndex(1000)).toBe(999);
		expect(await list.getIndex(1001)).toBe(1000);
	});
});
