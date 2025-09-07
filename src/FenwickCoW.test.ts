import { describe, it, expect } from "bun:test";
import { MemoryStore } from "./Store";
import { FenwickList } from "./FenwickList";

class CoWFenwickList<T> extends FenwickList<T> {
    pushToSegment(segIndex: number, v: T): void {
        const seg = this.getMeta().segments[segIndex] as unknown as { count: number };
        // Use protected sync accessor to mutate segment without going through insertAt
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const arr = this.getOrCreateArraySync(seg as any, true) as unknown as T[];
        arr.push(v);
        seg.count = arr.length;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        this.dirty.add(seg as any);
    }
}

describe("Fenwick CoW persistence", () => {
    it("preserves earlier segment updates when writing a different segment in the same chunk", async () => {
        const store = new MemoryStore();
        const list = new CoWFenwickList<number>(store, { segmentCount: 3, chunkCount: 2 });

        // Build 4 segments: [0,1], [2,3], [4,5], [6,7]
        for (let i = 0; i < 8; i++) {
            // eslint-disable-next-line no-await-in-loop
            await list.insertAt(i, i);
        }
        expect(list.getMeta().segments.length).toBe(4);

        // Modify segment A (index 2) which lives in chunk index 1
        list.pushToSegment(2, 100);
        await list.flush();
        let key = list.getMeta().chunks[1]!; // chunk for segments [2,3]
        let c1 = (await store.get<number[][]>(key)) ?? [];
        expect(c1[0]).toEqual([4, 5, 100]);

        // Modify segment B (index 3) without touching A
        list.pushToSegment(3, 200);
        await list.flush();
        key = list.getMeta().chunks[1]!; // new CoW key
        const c2 = (await store.get<number[][]>(key)) ?? [];
        // A's data preserved, B's data updated
        expect(c2[0]).toEqual([4, 5, 100]);
        expect(c2[1]).toEqual([6, 7, 200]);
    });
});


