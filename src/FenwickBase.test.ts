import { describe, it, expect } from "bun:test";
import { MemoryStore } from "./Store";
import { FenwickList } from "./FenwickList";

class TestFenwickList<T> extends FenwickList<T> {
  // Expose helpers for tests via any-casts to protected internals

  setValueByIndex(index: number, value: T): void {
    const { segIndex, localIndex } = (this as any).findByIndex(index) as {
      segIndex: number;
      localIndex: number;
    };
    const seg = this.getMeta().segments[segIndex] as unknown as {
      count: number;
    };
    const arr = (this as any).getOrCreateArraySync(seg, true) as T[];
    arr[localIndex] = value;
    (this as any).dirty.add(seg);
  }

  pushToSegment(segIndex: number, value: T): void {
    const seg = this.getMeta().segments[segIndex] as unknown as {
      count: number;
    };
    const arr = (this as any).getOrCreateArraySync(seg, true) as T[];
    arr.push(value);
    seg.count = arr.length;
    (this as any).dirty.add(seg);
  }

  getSegmentIndexRef(seg: unknown): number | undefined {
    return ((this as any).segmentIndexByRef as Map<unknown, number>).get(seg);
  }

  prefixBeforeSegment(segIndex: number): number {
    return (this as any).prefixSum(segIndex) as number;
  }

  findIndex(index: number): { segIndex: number; localIndex: number } {
    return (this as any).findByIndex(index) as {
      segIndex: number;
      localIndex: number;
    };
  }
}

describe("FenwickBase invariants and persistence", () => {
  it("prefix sums and findByIndex are consistent with segment counts", async () => {
    const store = new MemoryStore();
    const list = new TestFenwickList<number>(store, {
      segmentCount: 4,
      chunkCount: 0,
    });

    // Build 10 elements with small segmentCount to force multiple segments
    for (let i = 0; i < 10; i++) {
      // eslint-disable-next-line no-await-in-loop
      await list.insertAt(i, i);
    }
    // Expect segments roughly: [2,2,2,4] (total 10) given the split strategy
    const counts = list.getMeta().segments.map((s) => s.count);
    expect(counts.reduce((a, b) => a + b, 0)).toBe(10);

    // prefixSum(k) equals number of elements before segment k
    const prefixes = [
      list.prefixBeforeSegment(0),
      list.prefixBeforeSegment(1),
      list.prefixBeforeSegment(2),
      list.prefixBeforeSegment(3),
      list.prefixBeforeSegment(4),
    ];
    expect(prefixes[0]).toBe(0);
    expect(prefixes[1]).toBe(counts[0] ?? 0);
    expect(prefixes[2]).toBe((counts[0] ?? 0) + (counts[1] ?? 0));
    expect(prefixes[3]).toBe(
      (counts[0] ?? 0) + (counts[1] ?? 0) + (counts[2] ?? 0),
    );
    expect(prefixes[4]).toBe(10);

    // findByIndex maps global index to (segment,local) correctly
    const checks: Array<[number, [number, number]]> = [
      [0, [0, 0]],
      [1, [0, 1]],
      [2, [1, 0]],
      [3, [1, 1]],
      [4, [2, 0]],
      [5, [2, 1]],
      [6, [3, 0]],
      [9, [3, (counts[3] ?? 1) - 1]],
    ];
    for (const [global, [sExp, lExp]] of checks) {
      const { segIndex, localIndex } = list.findIndex(global);
      expect([segIndex, localIndex]).toEqual([sExp, lExp]);
      // Also cross-check via get
      // eslint-disable-next-line no-await-in-loop
      expect(await list.get(global)).toBe(global);
    }
  });

  it("cache invalidation reloads persisted state (drops unflushed changes)", async () => {
    const store = new MemoryStore();
    const list = new TestFenwickList<number>(store, {
      segmentCount: 8,
      chunkCount: 4,
    });
    for (let i = 0; i < 32; i++) {
      // eslint-disable-next-line no-await-in-loop
      await list.insertAt(i, i);
    }
    await list.flush();
    const k = 2;
    const persisted = await list.get(k);
    list.setValueByIndex(k, 999); // do not flush
    // Clear caches to force reload from store
    list.clearCaches();
    const reloaded = await list.get(k);
    expect(reloaded).toBe(persisted as number);
  });

  it("state can be reconstructed from metadata", async () => {
    const store = new MemoryStore();
    const listA = new FenwickList<number>(store, {
      segmentCount: 32,
      chunkCount: 8,
    });
    const N = 500;
    for (let i = 0; i < N; i++) {
      // eslint-disable-next-line no-await-in-loop
      await listA.insertAt(i, i);
    }
    await listA.flush();
    const meta = listA.getMeta();

    const listB = new FenwickList<number>(store, meta);
    const out = await listB.range(0, N);
    expect(out.length).toBe(N);
    for (let i = 0; i < N; i++) {
      expect(out[i]).toBe(i);
    }
  });

  it("segmentIndexByRef remains consistent after split", async () => {
    const store = new MemoryStore();
    const list = new TestFenwickList<number>(store, {
      segmentCount: 4,
      chunkCount: 2,
    });
    for (let i = 0; i < 8; i++) {
      // eslint-disable-next-line no-await-in-loop
      await list.insertAt(i, i);
    }
    const segRef = list.getMeta().segments[0];
    // Force split of segment 0 by inserting at index 0
    await list.insertAt(0, -1);
    const idx0 = list.getSegmentIndexRef(segRef);
    expect(idx0).toBe(0);
    expect(list.getMeta().segments.length).toBeGreaterThan(2);
    const segNew = list.getMeta().segments[1];
    const idx1 = list.getSegmentIndexRef(segNew);
    expect(idx1).toBe(1);
  });
  it("Segment split after flush persists and reloads correctly", async () => {
    const store = new MemoryStore();
    const list = new FenwickList<number>(store, {
      segmentCount: 10,
      chunkCount: 4,
    });
    for (let i = 0; i < 10; i++) await list.insertAt(i, i);
    await list.flush();
    await list.insertAt(10, 10); // triggers split
    await list.flush();
    const meta = list.getMeta();
    const list2 = new FenwickList<number>(store, meta);
    const out = await list2.range(0, 11);
    expect(out).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
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
