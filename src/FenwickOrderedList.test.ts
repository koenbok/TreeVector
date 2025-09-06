import { describe, expect, it } from "bun:test";
import { FenwickOrderedList } from "./FenwickOrderedList";
import { type IStore, MemoryStore } from "./Store";

class TracingStore<T> implements IStore {
  private inner = new MemoryStore();
  public activeGets = 0;
  public maxActiveGets = 0;
  public totalGets = 0;
  constructor(private readonly delayMs = 5) { }
  async get<K = unknown>(key: string): Promise<K | undefined> {
    this.totalGets += 1;
    this.activeGets += 1;
    this.maxActiveGets = Math.max(this.maxActiveGets, this.activeGets);
    await new Promise((r) => setTimeout(r, this.delayMs));
    const v = await this.inner.get<K>(key);
    this.activeGets -= 1;
    return v;
  }
  async set<K = unknown>(key: string, value: K): Promise<void> {
    await this.inner.set<K>(key, value);
  }
  reset(): void {
    this.activeGets = 0;
    this.maxActiveGets = 0;
    this.totalGets = 0;
  }
}

class TestFenwickOrderedList<T> extends FenwickOrderedList<T> {
  dropValues(): void {
    this.segmentCache.clear();
  }
}

// New helper to count full fenwick rebuilds
class CountingFenwickOrderedList<T> extends FenwickOrderedList<T> {
  public rebuildCalls = 0;
  protected override rebuildFenwick(): void {
    this.rebuildCalls += 1;
    super.rebuildFenwick();
  }
}

describe("FenwickOrderedList", () => {
  it("inserts and gets by index in order", async () => {
    const store = new MemoryStore();
    const list = new FenwickOrderedList<number>(store, { segmentCount: 1024, chunkCount: 128 });
    const nums = [5, 1, 3, 2, 4, 6, 0, 7, 9, 8];
    for (const n of nums) {
      await list.insert(n);
    }
    for (let i = 0; i < nums.length; i++) {
      expect(await list.get(i)).toBe(i);
    }
  });

  it("scan(min,max) returns sorted values within [min,max) bounds", async () => {
    const store = new MemoryStore();
    const list = new FenwickOrderedList<number>(store, { segmentCount: 512, chunkCount: 128 });
    const values = [10, 2, 7, 5, 1, 3, 9, 6, 4, 8];
    for (const v of values) await list.insert(v);
    const out = await list.scan(3, 7);
    expect(out).toEqual([3, 4, 5, 6]);
  });

  it("handles duplicates and keeps them contiguous", async () => {
    const store = new MemoryStore();
    const list = new FenwickOrderedList<number>(store, { segmentCount: 256, chunkCount: 128 });
    const values = [2, 2, 2, 1, 1, 3];
    for (const v of values) await list.insert(v);
    const got = await Promise.all(
      Array.from({ length: values.length }, (_, i) => list.get(i)),
    );
    expect(got).toEqual([1, 1, 2, 2, 2, 3]);
    const dupRange = await list.scan(2, 3);
    expect(dupRange).toEqual([2, 2, 2]);
  });

  it("scan excludes max and handles duplicates with [min,max) semantics", async () => {
    const store = new MemoryStore();
    const list = new FenwickOrderedList<number>(store, { segmentCount: 128, chunkCount: 128 });
    for (const v of [1, 2, 2, 3, 4, 5]) await list.insert(v);
    expect(await list.scan(2, 5)).toEqual([2, 2, 3, 4]);
    expect(await list.scan(5, 6)).toEqual([5]);
    expect(await list.scan(5, 5)).toEqual([]);
  });

  it("range(min,max) returns values by index slice [min,max)", async () => {
    const store = new MemoryStore();
    const list = new FenwickOrderedList<number>(store, { segmentCount: 128, chunkCount: 128 });
    for (const v of [10, 20, 30, 40, 50]) await list.insert(v);
    expect(await list.range(1, 4)).toEqual([20, 30, 40]);
    expect(await list.range(0, 2)).toEqual([10, 20]);
    expect(await list.range(4, 5)).toEqual([50]);
    expect(await list.range(5, 6)).toEqual([]);
    expect(await list.range(3, 3)).toEqual([]);
  });

  it("getIndex returns lower_bound for values and duplicates", async () => {
    const store = new MemoryStore();
    const list = new FenwickOrderedList<number>(store, { segmentCount: 256, chunkCount: 128 });
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
    const store = new MemoryStore();
    // small maxValues to force many splits into segments
    const list = new FenwickOrderedList<number>(store, { segmentCount: 8, chunkCount: 128 });
    const N = 1000;
    for (let i = 1; i <= N; i++) await list.insert(i);
    expect(await list.getIndex(1)).toBe(0);
    expect(await list.getIndex(2)).toBe(1);
    expect(await list.getIndex(50)).toBe(49);
    expect(await list.getIndex(1000)).toBe(999);
    expect(await list.getIndex(1001)).toBe(1000);
  });

  it("no waterfall: scan loads needed segments in parallel", async () => {
    const store = new TracingStore<number>(5);
    const list = new TestFenwickOrderedList<number>(store, { segmentCount: 4, chunkCount: 0 });
    // populate enough values to create multiple segments
    for (let i = 0; i < 64; i++) await list.insert(i);
    await list.flush();
    list.dropValues();
    store.reset();
    const out = await list.scan(0, 64);
    expect(out.length).toBe(64);
    // ensure parallelism happened
    expect(store.maxActiveGets).toBeGreaterThan(1);
  });

  it("no waterfall: range loads needed segments in parallel", async () => {
    const store = new TracingStore<number>(5);
    const list = new TestFenwickOrderedList<number>(store, { segmentCount: 4, chunkCount: 0 });
    for (let i = 0; i < 64; i++) await list.insert(i);
    await list.flush();
    list.dropValues();
    store.reset();
    const out = await list.range(0, 64);
    expect(out.length).toBe(64);
    expect(store.maxActiveGets).toBeGreaterThan(1);
  });

  it("no waterfall: insert touches at most one segment load", async () => {
    const store = new TracingStore<number>(5);
    const list = new TestFenwickOrderedList<number>(store, { segmentCount: 4, chunkCount: 0 });
    for (let i = 0; i < 16; i++) await list.insert(i);
    await list.flush();
    list.dropValues();
    store.reset();
    await list.insert(8.5);
    // one segment load is enough for ordered insert
    expect(store.totalGets).toBeLessThanOrEqual(1);
    // no need for parallelism during a single insert
    expect(store.maxActiveGets).toBeLessThanOrEqual(1);
  });

  it("incremental fenwick: avoids full rebuild on split (only initial rebuild)", async () => {
    const store = new MemoryStore();
    const list = new CountingFenwickOrderedList<number>(store, { segmentCount: 4, chunkCount: 0 });
    const N = 128;
    for (let i = 0; i < N; i++) {
      await list.insert(i);
    }
    expect(list.rebuildCalls).toBe(1);
  });
});
