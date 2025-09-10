import { describe, it, expect } from "bun:test";
import { Table, type TableMeta } from "./Table";
import type { IStore } from "./Store";
import { IndexedColumn, OrderedColumn, type IndexedColumnInterface } from "./Column";

class HookStore implements IStore {
    private map = new Map<string, unknown>();
    public failKeys = new Set<string>();
    public setCount: Record<string, number> = {};
    public onSet?: (key: string, value: unknown) => void;

    async get<T = unknown>(key: string): Promise<T | undefined> {
        const value = this.map.get(key);
        if (value === undefined) return undefined;
        return structuredClone(value) as T;
    }

    async set<T = unknown>(key: string, value: T): Promise<void> {
        if (this.failKeys.has(key)) throw new Error(`set failed for ${key}`);
        this.onSet?.(key, value);
        this.map.set(key, structuredClone(value));
        this.setCount[key] = (this.setCount[key] ?? 0) + 1;
    }
}

describe("Table ACID: meta persistence and atomicity", () => {
    it("empty flush persists minimal meta and can rehydrate", async () => {
        const store = new HookStore();
        const table = new Table<number>(
            store,
            {
                key: "id",
                column: new OrderedColumn<number>(store, { segmentCount: 4, chunkCount: 4 }),
            },
            undefined,
            { segmentCount: 4, chunkCount: 4 },
        );

        const key = "acid/empty.meta";
        await table.flush(key);
        const meta = (await store.get<TableMeta<number>>(key))!;
        expect(meta.defaults.segmentCount).toBe(4);
        expect(meta.defaults.chunkCount).toBe(4);
        const rehydrated = new Table<number>(store, meta);
        expect(await rehydrated.range(0, 10)).toEqual([]);
    });

    it("store meta matches in-memory meta after successful flush", async () => {
        const store = new HookStore();
        const table = new Table<number>(
            store,
            {
                key: "id",
                column: new OrderedColumn<number>(store, { segmentCount: 4, chunkCount: 4 }),
            },
            undefined,
            { segmentCount: 4, chunkCount: 4 },
        );
        await table.insert([
            { id: 2, name: "b" },
            { id: 1, name: "a" },
        ]);
        const key = "acid/store-sync.meta";
        await table.flush(key);
        const stored = (await store.get<TableMeta<number>>(key))!;
        expect(stored).toEqual(table.getMeta());
    });

    it("atomic commit: if a non-order column flush fails, meta remains unchanged", async () => {
        const store = new HookStore();
        const table = new Table<number>(
            store,
            {
                key: "id",
                column: new OrderedColumn<number>(store, { segmentCount: 4, chunkCount: 4 }),
            },
            { string: { name: new IndexedColumn<string>(store, { segmentCount: 4, chunkCount: 4 }) } } as unknown as { string?: Record<string, IndexedColumnInterface<string>>; number?: Record<string, IndexedColumnInterface<number>> },
            { segmentCount: 4, chunkCount: 4 },
        );
        await table.insert([
            { id: 1, name: "a" },
            { id: 2, name: "b" },
        ]);
        const key = "acid/atomic.meta";
        await table.flush(key);
        const v1 = (await store.get<TableMeta<number>>(key))!;

        // Inject failing column
        const failingCol: IndexedColumnInterface<unknown> = {
            insertAt: async () => { },
            range: async () => [],
            get: async () => undefined,
            flush: async () => {
                throw new Error("flush failed");
            },
            getMeta: () => ({ segmentCount: 1, chunkCount: 1, segments: [], chunks: [] }),
            setMeta: () => { },
            padEnd: async () => { },
        };
        (table as unknown as { columns: { string: Record<string, IndexedColumnInterface<unknown>>; number: Record<string, IndexedColumnInterface<unknown>> } }).columns.string["name"] = failingCol;

        await table.insert([{ id: 3, name: "c" }]);
        await expect(table.flush(key)).rejects.toBeTruthy();

        // Store and table meta remain at v1
        expect(await store.get<TableMeta<number>>(key)).toEqual(v1);
        expect(table.getMeta()).toEqual(v1);
    });

    it("atomic commit: if the order column flush fails, meta remains unchanged", async () => {
        const store = new HookStore();
        const table = new Table<number>(
            store,
            {
                key: "id",
                column: new OrderedColumn<number>(store, { segmentCount: 4, chunkCount: 4 }),
            },
            undefined,
            { segmentCount: 4, chunkCount: 4 },
        );
        await table.insert([
            { id: 1, name: "a" },
            { id: 2, name: "b" },
        ]);
        const key = "acid/order-fail.meta";
        await table.flush(key);
        const v1 = (await store.get<TableMeta<number>>(key))!;

        // Patch order column flush to fail
        const orderRef = (table as unknown as { order: { column: { flush: () => Promise<string[]> } } }).order.column;
        const originalFlush = orderRef.flush.bind(orderRef);
        (orderRef as { flush: () => Promise<string[]> }).flush = async () => {
            throw new Error("order flush failed");
        };
        await table.insert([{ id: 3, name: "c" }]);
        await expect(table.flush(key)).rejects.toBeTruthy();
        // Restore for safety (not strictly needed since table is local)
        (orderRef as { flush: () => Promise<string[]> }).flush = originalFlush;

        expect(await store.get<TableMeta<number>>(key)).toEqual(v1);
        expect(table.getMeta()).toEqual(v1);
    });

    it("in-memory meta updates after store.set has executed (ordering)", async () => {
        const store = new HookStore();
        const table = new Table<number>(
            store,
            {
                key: "id",
                column: new OrderedColumn<number>(store, { segmentCount: 4, chunkCount: 4 }),
            },
            undefined,
            { segmentCount: 4, chunkCount: 4 },
        );
        const key = "acid/order.meta";
        await table.insert([{ id: 1, name: "a" }]);
        await table.flush(key);
        const v1 = table.getMeta();

        let observedPreWasOld = false;
        store.onSet = (k, value) => {
            if (k !== key) return;
            // During set, table meta should still be the previous committed meta
            observedPreWasOld = observedPreWasOld || JSON.stringify(table.getMeta()) === JSON.stringify(v1);
        };

        await table.insert([{ id: 2, name: "b" }]);
        await table.flush(key);

        expect(observedPreWasOld).toBe(true);
        const stored = (await store.get<TableMeta<number>>(key))!;
        expect(stored).toEqual(table.getMeta());
    });

    it("idempotent flush with no changes still persists a snapshot", async () => {
        const store = new HookStore();
        const table = new Table<number>(
            store,
            {
                key: "id",
                column: new OrderedColumn<number>(store, { segmentCount: 4, chunkCount: 4 }),
            },
            undefined,
            { segmentCount: 4, chunkCount: 4 },
        );
        const key = "acid/idempotent.meta";
        await table.insert([{ id: 1, name: "a" }]);
        await table.flush(key);
        const setCount1 = store.setCount[key] ?? 0;
        await table.flush(key);
        const setCount2 = store.setCount[key] ?? 0;
        expect(setCount2).toBe(setCount1 + 1);
    });
});


