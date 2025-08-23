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

	it("range query returns sorted values within bounds", async () => {
		const store = new MemoryStore<number>();
		const tree = new BPlusTree<number>(store, 3, 4);
		const values = [10, 2, 7, 5, 1, 3, 9, 6, 4, 8];
		for (const v of values) await tree.insert(v);
		const out = await tree.range(3, 7);
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
		const range = await tree.range(2, 2);
		expect(range).toEqual([2, 2, 2]);
	});
});
