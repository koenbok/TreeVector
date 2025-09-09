import { describe, it, expect } from "bun:test";
import { MemoryStore } from "./Store";
import { FenwickList } from "./FenwickList";

class TestList<T> extends FenwickList<T> {
  // Expose helpers for tests
  setValueByIndex(index: number, value: T): void {
    const { segIndex, localIndex } = (
      this as unknown as {
        findByIndex(i: number): { segIndex: number; localIndex: number };
      }
    ).findByIndex(index);
    const seg = this.getMeta().segments[segIndex] as unknown as {
      count: number;
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const arr = (this as any).getOrCreateArraySync(seg as any, true) as T[];
    arr[localIndex] = value;
    // mark dirty
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (this as any).dirty.add(seg as any);
  }

  pushToSegment(segIndex: number, value: T): void {
    const seg = this.getMeta().segments[segIndex] as unknown as {
      count: number;
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const arr = (this as any).getOrCreateArraySync(seg as any, true) as T[];
    arr.push(value);
    seg.count = arr.length;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (this as any).dirty.add(seg as any);
  }

  getSegmentIndexRef(seg: unknown): number | undefined {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return ((this as any).segmentIndexByRef as Map<unknown, number>).get(seg);
  }
}

describe("Persistence and CoW logic", () => {
  it("Incremental Flush and CoW: single-segment update writes new chunk and preserves others", async () => {
    const store = new MemoryStore();
    const list = new TestList<number>(store, {
      segmentCount: 4,
      chunkCount: 4,
    });
    // Create 4 segments in chunk 0
    for (let i = 0; i < 16; i++) await list.insertAt(i, i);
    await list.flush();
    const initialKey = list.getMeta().chunks[0];
    expect(initialKey).toBeDefined();

    // Modify only segment 1 (indices 4..7)
    list.setValueByIndex(4, -1);
    await list.flush();
    const newKey = list.getMeta().chunks[0];
    expect(newKey).toBeDefined();
    expect(newKey).not.toBe(initialKey);

    // Re-init from meta
    const meta = list.getMeta();
    const list2 = new FenwickList<number>(store, meta);
    // seg0 unchanged
    expect(await list2.range(0, 4)).toEqual([0, 1, 2, 3]);
    // seg1 first value updated
    expect(await list2.range(4, 8)).toEqual([-1, 5, 6, 7]);
    // seg2, seg3 unchanged
    expect(await list2.range(8, 12)).toEqual([8, 9, 10, 11]);
    expect(await list2.range(12, 16)).toEqual([12, 13, 14, 15]);
  });

  it("State reconstruction from metadata", async () => {
    const store = new MemoryStore();
    const listA = new FenwickList<number>(store, {
      segmentCount: 32,
      chunkCount: 8,
    });
    const N = 1000;
    for (let i = 0; i < N; i++) await listA.insertAt(i, i);
    await listA.flush();
    const meta = listA.getMeta();

    const listB = new FenwickList<number>(store, meta);
    const out = await listB.range(0, N);
    expect(out.length).toBe(N);
    for (let i = 0; i < N; i++) expect(out[i]).toBe(i);
  });

  it("Cache invalidation reloads persisted state (drops unflushed changes)", async () => {
    const store = new MemoryStore();
    const list = new TestList<number>(store, {
      segmentCount: 8,
      chunkCount: 4,
    });
    for (let i = 0; i < 32; i++) await list.insertAt(i, i);
    await list.flush();
    // k in segment 0 for simplicity
    const k = 2;
    const v1 = await list.get(k);
    list.setValueByIndex(k, 999); // do not flush
    // Clear caches to force reload from store
    list.clearCaches();
    const vReloaded = await list.get(k);
    expect(vReloaded).toBe(v1 as number);
  });

  it("Segment split after flush persists and reloads correctly", async () => {
    const store = new MemoryStore();
    const list = new FenwickList<number>(store, {
      segmentCount: 10,
      chunkCount: 4,
    });
    for (let i = 0; i < 10; i++) await list.insertAt(i, i);
    await list.flush();
    await list.insertAt(10, 10); // triggers split of seg0
    await list.flush();
    const meta = list.getMeta();
    const list2 = new FenwickList<number>(store, meta);
    const out = await list2.range(0, 11);
    expect(out).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });

  it("segmentIndexByRef is consistent after split", async () => {
    const store = new MemoryStore();
    const list = new TestList<number>(store, {
      segmentCount: 4,
      chunkCount: 4,
    });
    // Create 2 segments
    for (let i = 0; i < 8; i++) await list.insertAt(i, i);
    const segRef = list.getMeta().segments[0];
    // Force split of segment 0
    await list.insertAt(0, -1);
    // After split, segRef should still map to index 0, and a new segment at index 1 exists
    const idx0 = list.getSegmentIndexRef(segRef);
    expect(idx0).toBe(0);
    expect(list.getMeta().segments.length).toBeGreaterThan(2);
    const segNew = list.getMeta().segments[1];
    const idx1 = list.getSegmentIndexRef(segNew);
    expect(idx1).toBe(1);
  });

  it("Empty list operations are safe", async () => {
    const store = new MemoryStore();
    const list = new FenwickList<number>(store, {
      segmentCount: 8,
      chunkCount: 4,
    });
    const keys = await list.flush();
    expect(keys.length).toBe(0);
    expect(await list.get(0)).toBeUndefined();
    expect(await list.range(0, 10)).toEqual([]);
  });
});
