import { FenwickOrderedList } from "../src/FenwickOrderedList";
import { MemoryStore as TVMemoryStore } from "../src/Store";

// VectorPulse imports (absolute path as requested)
// eslint-disable-next-line import/no-relative-packages
import { SegmentOrderedNumberVector } from "/Users/koen/VectorPulse/src/SegmentOrderedNumberVector";
// eslint-disable-next-line import/no-relative-packages
import { MemoryStore as VPStore } from "/Users/koen/VectorPulse/src/store/MemoryStore";

function randomNumbers(count: number, max = 1_000_000): number[] {
    const arr = new Array<number>(count);
    for (let i = 0; i < count; i++) arr[i] = Math.floor(Math.random() * max);
    return arr;
}

async function benchFenwickOrderedList(N: number, segmentCount = 1024, chunkCount = 256): Promise<number> {
    const store = new TVMemoryStore();
    const ordered = new FenwickOrderedList<number>(store, {
        segmentCount,
        chunkCount,
    });
    const values = randomNumbers(N);
    const start = performance.now();
    for (let i = 0; i < values.length; i++) {
        // eslint-disable-next-line no-await-in-loop
        await ordered.insert(values[i] as number);
    }
    // flush to materialize chunk writes
    // eslint-disable-next-line no-await-in-loop
    await ordered.flush();
    return performance.now() - start;
}

// Incremental random inserts (unsorted), one-by-one, updating meta each time
async function benchSegmentOrderedNumberVector(N: number, bucketSize = 1024): Promise<number> {
    const store = new VPStore();
    let meta = { size: 0, buckets: [] as Array<{ id: string; key: string; head?: number; tail?: number; size: number; offset: number }> };
    const vector = new SegmentOrderedNumberVector(store, bucketSize, async () => meta);
    const values = randomNumbers(N);

    const start = performance.now();
    for (let i = 0; i < values.length; i++) {
        // eslint-disable-next-line no-await-in-loop
        const res = await vector.insertOrdered(meta, [values[i] as number]);
        meta = res.meta;
    }
    await vector.flush();
    return performance.now() - start;
}

async function main() {
    const N = Number.parseInt(process.argv[2] || "100000", 10);
    const seg = Number.parseInt(process.argv[3] || "1024", 10);
    const chunks = Number.parseInt(process.argv[4] || "256", 10);
    const bucket = Number.parseInt(process.argv[5] || String(seg), 10);
    console.log(`Random incremental inserts, N=${N}, TV(segmentCount=${seg}, chunkCount=${chunks}), VP(bucketSize=${bucket})`);

    const t1 = await benchFenwickOrderedList(N, seg, chunks);
    console.log(`FenwickOrderedList: ${(N / (t1 / 1000)).toFixed(0)} rows/sec (${t1.toFixed(1)} ms)`);

    const t2 = await benchSegmentOrderedNumberVector(N, bucket);
    console.log(`SegmentOrderedNumberVector (incremental): ${(N / (t2 / 1000)).toFixed(0)} rows/sec (${t2.toFixed(1)} ms)`);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});


