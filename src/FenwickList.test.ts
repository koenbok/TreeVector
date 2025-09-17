import { describe, expect, it } from "bun:test";
import { FenwickList } from "./FenwickList";
import { type IStore, MemoryStore } from "./Store";
import { IndexedColumn } from "./Column";

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

class TestFenwickList<T> extends FenwickList<T> {
  dropValues(): void {
    this.clearCaches();
  }
}

// New helper to count full fenwick rebuilds
class CountingFenwickList<T> extends FenwickList<T> {
  public rebuildCalls = 0;
  protected override rebuildIndices(): void {
    this.rebuildCalls += 1;
    super.rebuildIndices();
  }
}

describe("FenwickList (indexed)", () => {
  it("inserts at index and get reflects shifted positions", async () => {
    const store = new MemoryStore();
    const list = new FenwickList<number>(store, {
      segmentCount: 64,
      chunkCount: 0,
    });
    await list.insertAt(0, 2);
    await list.insertAt(0, 1); // [1,2]
    await list.insertAt(2, 4); // [1,2,4]
    await list.insertAt(2, 3); // [1,2,3,4]
    expect(await list.get(0)).toBe(1);
    expect(await list.get(1)).toBe(2);
    expect(await list.get(2)).toBe(3);
    expect(await list.get(3)).toBe(4);
  });

  it("insertManyAt (unique ascending indexes) matches sequential insertAt", async () => {
    const store = new MemoryStore();
    const col = new IndexedColumn<number>(store, { segmentCount: 8, chunkCount: 0 });
    // Start with some values to ensure multi-segment behavior later if needed
    await col.insertAt(0, 10);
    await col.insertAt(1, 40); // [10, 40]

    const indexes = [0, 2, 3];
    const values = [5, 20, 30];
    // Compute expected by applying sequential insertAt (already ascending)
    const store2 = new MemoryStore();
    const seq = new IndexedColumn<number>(store2, { segmentCount: 8, chunkCount: 0 });
    await seq.insertAt(0, 10);
    await seq.insertAt(1, 40);
    for (let i = 0; i < indexes.length; i++) {
      await seq.insertAt(indexes[i]!, values[i]!);
    }
    const expected = await seq.range(0, 5);

    // @ts-expect-error access to extended API for test
    await (col as any).insertManyAt(indexes, values);

    expect(await col.range(0, 5)).toEqual(expected);
  });

  it("range(min,max) returns slice [min,max)", async () => {
    const store = new MemoryStore();
    const list = new FenwickList<number>(store, {
      segmentCount: 64,
      chunkCount: 0,
    });
    for (const v of [10, 20, 30, 40, 50]) await list.insertAt(999, v); // append
    expect(await list.range(1, 4)).toEqual([20, 30, 40]);
    expect(await list.range(0, 2)).toEqual([10, 20]);
    expect(await list.range(4, 5)).toEqual([50]);
  });

  it("handles gaps with out-of-range get and appending beyond length", async () => {
    const store = new MemoryStore();
    const list = new FenwickList<number>(store, {
      segmentCount: 64,
      chunkCount: 0,
    });
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
    const store = new MemoryStore();
    const list = new FenwickList<number>(store, {
      segmentCount: 64,
      chunkCount: 0,
    });
    for (const v of [1, 3, 4]) await list.insertAt(999, v); // [1,3,4]
    await list.insertAt(1, 2); // -> [1,2,3,4]
    expect(await list.get(0)).toBe(1);
    expect(await list.get(1)).toBe(2);
    expect(await list.get(2)).toBe(3);
    expect(await list.get(3)).toBe(4);
  });

  it("range with min==max and min>max returns empty slice", async () => {
    const store = new MemoryStore();
    const list = new FenwickList<number>(store, {
      segmentCount: 64,
      chunkCount: 0,
    });
    for (const v of [10, 20, 30]) await list.insertAt(999, v);
    expect(await list.range(1, 1)).toEqual([]);
    expect(await list.range(2, 1)).toEqual([]);
  });

  it("scales across multiple segments and preserves order", async () => {
    const store = new MemoryStore();
    // small segment size to force many splits
    const list = new FenwickList<number>(store, {
      segmentCount: 8,
      chunkCount: 0,
    });
    const N = 256;
    for (let i = 0; i < N; i++) await list.insertAt(i, i);
    for (let i = 0; i < N; i++) expect(await list.get(i)).toBe(i);
    // insert in the middle repeatedly
    await list.insertAt(128, -1);
    expect(await list.get(128)).toBe(-1);
    expect(await list.get(129)).toBe(128);
  });

  it("no waterfall: range loads segments in parallel", async () => {
    const store = new TracingStore<number>(5);
    const list = new TestFenwickList<number>(store, {
      segmentCount: 4,
      chunkCount: 1,
    });
    for (let i = 0; i < 64; i++) await list.insertAt(i, i);
    await list.flush();
    list.dropValues();
    store.reset();
    const out = await list.range(0, 64);
    expect(out.length).toBe(64);
    expect(store.maxActiveGets).toBeGreaterThan(1);
  });

  it("no waterfall: insertAt triggers at most one load", async () => {
    const store = new TracingStore<number>(5);
    const list = new TestFenwickList<number>(store, {
      segmentCount: 4,
      chunkCount: 1,
    });
    for (let i = 0; i < 16; i++) await list.insertAt(i, i);
    await list.flush();
    list.dropValues();
    store.reset();
    await list.insertAt(8, 99);
    expect(store.totalGets).toBeLessThanOrEqual(1);
    expect(store.maxActiveGets).toBeLessThanOrEqual(1);
  });

  it("incremental fenwick: avoids full rebuild on split (only initial rebuild)", async () => {
    const store = new MemoryStore();
    const list = new CountingFenwickList<number>(store, {
      segmentCount: 4,
      chunkCount: 1,
    });
    const N = 128;
    for (let i = 0; i < N; i++) {
      await list.insertAt(i, i);
    }
    expect(list.rebuildCalls).toBe(1);
  });

  it("batch insert prefetches required segments (parallel loads)", async () => {
    const store = new TracingStore<number>(5);
    const list = new TestFenwickList<number>(store, {
      segmentCount: 4,
      chunkCount: 1,
    });
    // Seed multiple segments
    for (let i = 0; i < 32; i++) await list.insertAt(i, i);
    await list.flush();
    list.dropValues();
    store.reset();

    // Prepare a batch that touches multiple segments
    const indexes = [2, 6, 10, 14, 18, 22, 26, 30];
    const values = indexes.map((i) => 1000 + i);

    // @ts-expect-error access to extended API for test
    await (list as any).insertManyAt(indexes, values);
    // Expect multiple segment loads and some parallelism
    expect(store.totalGets).toBeGreaterThan(1);
    expect(store.maxActiveGets).toBeGreaterThan(1);
  });
});
