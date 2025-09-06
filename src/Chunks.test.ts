import { describe, it, expect } from "bun:test";
import { flushSegmentsToChunks, loadSegmentFromChunks } from "./Chunks";
import { MemoryStore } from "./Store";

describe("flushSegmentsToChunks", () => {
    it("merges into existing chunk and does not drop previously flushed segments", async () => {
        const store = new MemoryStore();
        const segmentsPerChunk = 2; // seg_0 and seg_1 share chunk_0

        // First flush seg_0 into chunk_0
        const cache1 = new Map([["seg_0", [1]]]);
        await flushSegmentsToChunks<number>(
            store,
            [{ id: "seg_0" }],
            cache1,
            segmentsPerChunk,
            "chunk_",
        );

        // Then flush seg_1 into the same chunk_0
        const cache2 = new Map([["seg_1", [2]]]);
        await flushSegmentsToChunks<number>(
            store,
            [{ id: "seg_1" }],
            cache2,
            segmentsPerChunk,
            "chunk_",
        );

        // Both segments should be present after both flushes
        const v0 = await loadSegmentFromChunks<number>(
            store,
            "seg_0",
            segmentsPerChunk,
            "chunk_",
        );
        const v1 = await loadSegmentFromChunks<number>(
            store,
            "seg_1",
            segmentsPerChunk,
            "chunk_",
        );

        expect(v0).toEqual([1]);
        expect(v1).toEqual([2]);
    });
});


