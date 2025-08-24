import { describe, expect, it } from "bun:test";
import { FenwickList } from "./FenwickList";
import { MemoryStore } from "./Store";

describe("FenwickList (indexed)", () => {
	it("inserts at index and get reflects shifted positions", async () => {
		const store = new MemoryStore<number>();
		const list = new FenwickList<number>(store, 64);
		await list.insertAt(0, 2);
		await list.insertAt(0, 1); // [1,2]
		await list.insertAt(2, 4); // [1,2,4]
		await list.insertAt(2, 3); // [1,2,3,4]
		expect(await list.get(0)).toBe(1);
		expect(await list.get(1)).toBe(2);
		expect(await list.get(2)).toBe(3);
		expect(await list.get(3)).toBe(4);
	});

	it("range(min,max) returns slice [min,max)", async () => {
		const store = new MemoryStore<number>();
		const list = new FenwickList<number>(store, 64);
		for (const v of [10, 20, 30, 40, 50]) await list.insertAt(999, v); // append
		expect(await list.range(1, 4)).toEqual([20, 30, 40]);
		expect(await list.range(0, 2)).toEqual([10, 20]);
		expect(await list.range(4, 5)).toEqual([50]);
	});

	it("handles gaps with out-of-range get and appending beyond length", async () => {
		const store = new MemoryStore<number>();
		const list = new FenwickList<number>(store, 64);
		expect(await list.get(5)).toBeUndefined();
		expect(await list.range(3, 7)).toEqual([]);

		await list.insertAt(0, 100); // [100]
		await list.insertAt(1, 200); // [100,200]
		await list.insertAt(1, 150); // [100,150,200]
		expect(await list.get(10)).toBeUndefined();

		await list.insertAt(100, 300); // append -> [100,150,200,300]
		expect(await list.get(3)).toBe(300);
		expect(await list.range(2, 999)).toEqual([200, 300]);
	});

	it("inserting in the middle shifts subsequent elements", async () => {
		const store = new MemoryStore<number>();
		const list = new FenwickList<number>(store, 64);
		for (const v of [1, 3, 4]) await list.insertAt(999, v); // [1,3,4]
		await list.insertAt(1, 2); // -> [1,2,3,4]
		expect(await list.get(0)).toBe(1);
		expect(await list.get(1)).toBe(2);
		expect(await list.get(2)).toBe(3);
		expect(await list.get(3)).toBe(4);
	});

	it("range with min==max and min>max returns empty slice", async () => {
		const store = new MemoryStore<number>();
		const list = new FenwickList<number>(store, 64);
		for (const v of [10, 20, 30]) await list.insertAt(999, v);
		expect(await list.range(1, 1)).toEqual([]);
		expect(await list.range(2, 1)).toEqual([]);
	});

	it("scales across multiple segments and preserves order", async () => {
		const store = new MemoryStore<number>();
		// small segment size to force many splits
		const list = new FenwickList<number>(store, 8);
		const N = 256;
		for (let i = 0; i < N; i++) await list.insertAt(i, i);
		for (let i = 0; i < N; i++) expect(await list.get(i)).toBe(i);
		// insert in the middle repeatedly
		await list.insertAt(128, -1);
		expect(await list.get(128)).toBe(-1);
		expect(await list.get(129)).toBe(128);
	});
});
