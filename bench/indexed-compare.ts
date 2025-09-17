import { FenwickList } from "../src/FenwickList";
import { MemoryStore as TVMemoryStore } from "../src/Store";
// VectorPulse
// eslint-disable-next-line import/no-relative-packages
import { SegmentNumberVector } from "/Users/koen/VectorPulse/src/SegmentNumberVector";
// eslint-disable-next-line import/no-relative-packages
import { MemoryStore as VPStore } from "/Users/koen/VectorPulse/src/store/MemoryStore";

function randomNumbers(count: number, max = 1_000_000): number[] {
    const arr = new Array<number>(count);
    for (let i = 0; i < count; i++) arr[i] = Math.floor(Math.random() * max);
    return arr;
}

async function benchFenwickListAppend(N: number, seg = 1024, chunks = 256): Promise<number> {
    const store = new TVMemoryStore();
    const list = new FenwickList<number>(store, { segmentCount: seg, chunkCount: chunks });
    const values = randomNumbers(N);
    const start = performance.now();
    for (let i = 0; i < N; i++) {
        // append at end
        // eslint-disable-next-line no-await-in-loop
        await list.insertAt(i, values[i] as number);
    }
    await list.flush();
    return performance.now() - start;
}

async function benchFenwickListRandom(N: number, seg = 1024, chunks = 256): Promise<number> {
    const store = new TVMemoryStore();
    const list = new FenwickList<number>(store, { segmentCount: seg, chunkCount: chunks });
    const values = randomNumbers(N);
    const start = performance.now();
    for (let i = 0; i < N; i++) {
        const idx = Math.floor(Math.random() * (i + 1));
        // eslint-disable-next-line no-await-in-loop
        await list.insertAt(idx, values[i] as number);
    }
    await list.flush();
    return performance.now() - start;
}

async function benchSegmentNumberVectorAppend(N: number, bucket = 1024): Promise<number> {
    const store = new VPStore();
    const vec = new SegmentNumberVector(store, bucket, async () => ({ size: 0, buckets: [] }));
    // Using backward-compatible API wrapper in SegmentNumberVector: insertAtIndexes(metaOrValues, ...)
    // but we'll call the new stateless API via the internal since class adapts.
    // We need to keep and update meta locally.
    let meta = { size: 0, buckets: [] as Array<{ id: string; key: string; head?: number; tail?: number; size: number; offset: number }> };
    const values = randomNumbers(N);
    const start = performance.now();
    for (let i = 0; i < N; i++) {
        // eslint-disable-next-line no-await-in-loop
        meta = (await vec.insertAtIndexes(meta as any, [values[i] as number], [i])) as any;
    }
    await vec.flush();
    return performance.now() - start;
}

async function benchSegmentNumberVectorRandom(N: number, bucket = 1024): Promise<number> {
    const store = new VPStore();
    const vec = new SegmentNumberVector(store, bucket, async () => ({ size: 0, buckets: [] }));
    let meta = { size: 0, buckets: [] as Array<{ id: string; key: string; head?: number; tail?: number; size: number; offset: number }> };
    const values = randomNumbers(N);
    const start = performance.now();
    for (let i = 0; i < N; i++) {
        const idx = Math.floor(Math.random() * (meta.size + 1));
        // eslint-disable-next-line no-await-in-loop
        meta = (await vec.insertAtIndexes(meta as any, [values[i] as number], [idx])) as any;
    }
    await vec.flush();
    return performance.now() - start;
}

async function main() {
    const mode = (process.argv[2] || "append") as "append" | "random";
    const N = Number.parseInt(process.argv[3] || (mode === "append" ? "200000" : "50000"), 10);
    const seg = Number.parseInt(process.argv[4] || "1024", 10);
    const chunks = Number.parseInt(process.argv[5] || "256", 10);
    const bucket = Number.parseInt(process.argv[6] || String(seg), 10);

    console.log(`Mode=${mode}, N=${N}, TV(segmentCount=${seg}, chunkCount=${chunks}) vs VP(bucketSize=${bucket})`);

    const tTV = mode === "append"
        ? await benchFenwickListAppend(N, seg, chunks)
        : await benchFenwickListRandom(N, seg, chunks);

    const tVP = mode === "append"
        ? await benchSegmentNumberVectorAppend(N, bucket)
        : await benchSegmentNumberVectorRandom(N, bucket);

    console.log(`FenwickList (${mode}): ${(N / (tTV / 1000)).toFixed(0)} ops/sec (${tTV.toFixed(1)} ms)`);
    console.log(`SegmentNumberVector (${mode}): ${(N / (tVP / 1000)).toFixed(0)} ops/sec (${tVP.toFixed(1)} ms)`);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});


