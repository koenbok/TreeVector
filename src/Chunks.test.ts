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
}

describe("Chunks and Copy-on-Write persistence", () => {
  it("rotates only the modified chunk key; other chunks remain unchanged", async () => {
    const store = new MemoryStore();
    // segmentCount small to force multiple segments, chunkCount=2 => 2 segments per chunk
    const list = new TestFenwickList<number>(store, {
      segmentCount: 4,
      chunkCount: 2,
    });

    // Build 16 elements -> typically 4 segments of ~4 each => 2 chunks
    for (let i = 0; i < 16; i++) {
      // eslint-disable-next-line no-await-in-loop
      await list.insertAt(i, i);
    }
    await list.flush();
    const before = [...list.getMeta().chunks];

    // Modify an element in the first segment (chunk 0)
    list.setValueByIndex(0, -1);
    await list.flush();
    const after = [...list.getMeta().chunks];

    // Only chunk 0 should change
    expect(after[0]).toBeDefined();
    expect(after[0]).not.toBe(before[0]);
    expect(after[1]).toBe(before[1] as any);

    // Rehydrate and validate data integrity
    const meta = list.getMeta();
    const list2 = new FenwickList<number>(store, meta);
    expect(await list2.range(0, 4)).toEqual([-1, 1, 2, 3]); // first segment updated
    expect(await list2.range(4, 8)).toEqual([4, 5, 6, 7]);
    expect(await list2.range(8, 12)).toEqual([8, 9, 10, 11]);
    expect(await list2.range(12, 16)).toEqual([12, 13, 14, 15]);
  });

  it("CoW preserves earlier segment updates when writing a different segment in the same chunk", async () => {
    const store = new MemoryStore();
    const list = new TestFenwickList<number>(store, {
      segmentCount: 3,
      chunkCount: 2,
    });

    // Build multiple segments
    for (let i = 0; i < 8; i++) {
      // eslint-disable-next-line no-await-in-loop
      await list.insertAt(i, i);
    }
    expect(list.getMeta().segments.length).toBeGreaterThanOrEqual(4);

    // Modify segment A (index 2) which lives in chunk index 1
    list.pushToSegment(2, 100);
    await list.flush();
    let key = list.getMeta().chunks[1]!;
    let c1 = (await store.get<number[][]>(key)) ?? [];
    expect(c1[0]).toEqual([4, 5, 100]);

    // Modify segment B (index 3) without touching A
    list.pushToSegment(3, 200);
    await list.flush();
    key = list.getMeta().chunks[1]!;
    const c2 = (await store.get<number[][]>(key)) ?? [];
    // A's data preserved, B's data updated
    expect(c2[0]).toEqual([4, 5, 100]);
    expect(c2[1]).toEqual([6, 7, 200]);
  });
});
