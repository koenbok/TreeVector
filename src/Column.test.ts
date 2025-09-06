import { describe, it, expect } from "bun:test";
import {
  type IndexedColumnInterface,
  type OrderedColumnInterface,
  FenwickOrderedColumn,
  FenwickColumn,
} from "./Column";
import { MemoryStore } from "./Store";

const SortedColumns: Record<string, () => OrderedColumnInterface<number>> = {
  FenwickOrderedColumn: () =>
    new FenwickOrderedColumn<number>(new MemoryStore(), { segmentCount: 10, chunkCount: 10 }),
};

const IndexedColumns: Record<string, () => IndexedColumnInterface<number>> = {
  FenwickColumn: () => new FenwickColumn<number>(new MemoryStore(), { segmentCount: 10, chunkCount: 10 }),
};

for (const [name, ctor] of Object.entries(SortedColumns)) {
  describe(`ISortedColumn (${name})`, () => {
    it("inserts return rank and get yields sorted order", async () => {
      const col = ctor();
      const seq = [5, 1, 3, 2, 4, 2, 5];
      const inserted: number[] = [];
      for (const v of seq) {
        const rank = inserted.filter((x) => x < v).length;
        const idx = await col.insert(v);
        expect(idx).toBe(rank);
        inserted.push(v);
      }
      const out = await Promise.all(
        Array.from({ length: inserted.length }, (_, i) => col.get(i)),
      );
      const sorted = inserted.slice().sort((a, b) => a - b);
      expect(out).toEqual(sorted);
    });

    it("scan(min,max) is [min, max) with binary search bounds", async () => {
      const col = ctor();
      for (const v of [1, 2, 2, 3, 4, 5]) await col.insert(v);
      // [min,max) semantics: max exclusive
      expect(await col.scan(2, 5)).toEqual([2, 2, 3, 4]);
      expect(await col.scan(1, 3)).toEqual([1, 2, 2]);
      expect(await col.scan(0, 1)).toEqual([]);
    });

    it("handles empty column for get/range", async () => {
      const col = ctor();
      expect(await col.get(0)).toBeUndefined();
      expect(await col.scan(0, 10)).toEqual([]);
    });

    it("duplicates insert at lower_bound and are contiguous", async () => {
      const col = ctor();
      const toInsert = [2, 2, 2, 1, 3, 2];
      const inserted: number[] = [];
      for (const v of toInsert) {
        const rank = inserted.filter((x) => x < v).length;
        const idx = await col.insert(v);
        expect(idx).toBe(rank);
        inserted.push(v);
      }
      const min = Math.min(...inserted);
      const max = Math.max(...inserted);
      const out = await col.scan(min, (max + 1) as unknown as number);
      const expected = inserted.slice().sort((a, b) => a - b);
      expect(out).toEqual(expected);
    });

    it("range outside bounds and degenerate ranges", async () => {
      const col = ctor();
      for (const v of [10, 20, 30]) await col.insert(v);
      expect(
        await col.scan(Number.NEGATIVE_INFINITY, Number.POSITIVE_INFINITY),
      ).toEqual([10, 20, 30]);
      expect(await col.scan(1000, 2000)).toEqual([]);
      expect(await col.scan(15, 15)).toEqual([]);
    });
  });
}

for (const [name, ctor] of Object.entries(IndexedColumns)) {
  describe(`IIndexedColumn (${name})`, () => {
    it("inserts at index and get reflects shifted positions", async () => {
      const col = ctor();
      await col.insertAt(0, 2);
      await col.insertAt(0, 1); // [1,2]
      await col.insertAt(2, 4); // [1,2,4]
      await col.insertAt(2, 3); // [1,2,3,4]
      expect(await col.get(0)).toBe(1);
      expect(await col.get(1)).toBe(2);
      expect(await col.get(2)).toBe(3);
      expect(await col.get(3)).toBe(4);
    });

    it("range(min,max) returns slice [min,max)", async () => {
      const col = ctor();
      for (const v of [10, 20, 30, 40, 50]) await col.insertAt(999, v); // always append
      expect(await col.range(1, 4)).toEqual([20, 30, 40]);
      expect(await col.range(0, 2)).toEqual([10, 20]);
      expect(await col.range(4, 5)).toEqual([50]);
    });

    it("handles gaps with out-of-range get and appending beyond length", async () => {
      const col = ctor();
      expect(await col.get(5)).toBeUndefined();
      expect(await col.range(3, 7)).toEqual([]);

      await col.insertAt(0, 100); // [100]
      await col.insertAt(1, 200); // [100,200]
      await col.insertAt(1, 150); // [100,150,200]
      expect(await col.get(10)).toBeUndefined();

      await col.insertAt(100, 300); // append -> [100,150,200,300]
      expect(await col.get(3)).toBe(300);
      expect(await col.range(2, 999)).toEqual([200, 300]);
    });

    it("inserting in the middle shifts subsequent elements", async () => {
      const col = ctor();
      for (const v of [1, 3, 4]) await col.insertAt(999, v); // [1,3,4]
      await col.insertAt(1, 2); // -> [1,2,3,4]
      expect(await col.get(0)).toBe(1);
      expect(await col.get(1)).toBe(2);
      expect(await col.get(2)).toBe(3);
      expect(await col.get(3)).toBe(4);
    });

    it("range with min==max and min>max returns empty slice", async () => {
      const col = ctor();
      for (const v of [10, 20, 30]) await col.insertAt(999, v);
      expect(await col.range(1, 1)).toEqual([]);
      expect(await col.range(2, 1)).toEqual([]);
    });
  });
}
