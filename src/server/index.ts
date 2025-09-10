import { Hono } from "hono";
import { Table, type TableMeta } from "../Table";
import { OrderedColumn } from "../Column";
import { MemoryStore } from "../Store";
import {
    DEFAULT_ORDER_KEY,
    DEFAULT_ORDER_KEY_TYPE,
    DEFAULT_SEGMENT_COUNT,
    DEFAULT_CHUNK_COUNT,
} from "./config";

type Row = Record<string, unknown>;

const store = new MemoryStore();

function getMetaKey(team: string, project: string, table: string): string {
    return `tables/${team}/${project}/${table}.meta`;
}

function requireOrderKey(rows: Row[], key: string): void {
    if (!Array.isArray(rows) || rows.length === 0) return;
    for (let i = 0; i < rows.length; i++) {
        if ((rows[i] as Row)?.[key] === undefined) {
            throw new Error(`Row ${i} is missing required order key "${key}"`);
        }
    }
}

function inferOrderType(value: unknown): "number" | "string" {
    const t = typeof value;
    if (t === "number" || t === "string") return t;
    throw new Error(`Unsupported order value type: ${t}`);
}

export const app = new Hono();

app.post("/:team/:project/:table", async (c) => {
    try {
        const { team, project, table } = c.req.param();
        const metaKey = getMetaKey(team, project, table);

        const rows = (await c.req.json().catch(() => [])) as Row[];
        if (!Array.isArray(rows) || rows.length === 0) {
            return c.json({ ok: false, error: "Body must be a non-empty array of rows" }, 400);
        }

        const orderKey = DEFAULT_ORDER_KEY;
        try {
            requireOrderKey(rows, orderKey);
        } catch (e) {
            const msg = e instanceof Error ? e.message : "Missing order key";
            return c.json({ ok: false, error: msg }, 400);
        }

        const existingMeta = await store.get<TableMeta<any>>(metaKey);

        let tbl: Table<any>;
        if (existingMeta) {
            tbl = new Table<any>(store, existingMeta);
        } else {
            const orderSample = rows[0]?.[orderKey];
            const orderType = inferOrderType(orderSample);
            if (orderType !== DEFAULT_ORDER_KEY_TYPE) {
                throw new Error(`Order key \"${orderKey}\" type mismatch: expected ${DEFAULT_ORDER_KEY_TYPE}, got ${orderType}`);
            }
            const defaults = {
                segmentCount: DEFAULT_SEGMENT_COUNT,
                chunkCount: DEFAULT_CHUNK_COUNT,
            };

            if (orderType === "number") {
                const orderCol = new OrderedColumn<number>(store, defaults);
                tbl = new Table<number>(store, { key: orderKey, column: orderCol }, undefined, defaults);
            } else {
                const orderCol = new OrderedColumn<string>(store, defaults);
                tbl = new Table<string>(store, { key: orderKey, column: orderCol }, undefined, defaults);
            }
        }

        await tbl.insert(rows);
        await tbl.flush(metaKey);

        return c.json({ ok: true, inserted: rows.length, meta: tbl.getMeta() });
    } catch (err) {
        const message = err instanceof Error ? err.message : "Unknown error";
        return c.json({ ok: false, error: message }, 400);
    }
});

app.get("/:team/:project/:table", async (c) => {
    try {
        const { team, project, table } = c.req.param();
        const metaKey = getMetaKey(team, project, table);
        const existingMeta = await store.get<TableMeta<any>>(metaKey);
        if (!existingMeta) {
            return c.json({ ok: false, error: "Table not found" }, 404);
        }
        const tbl = new Table<any>(store, existingMeta);
        const rows = await tbl.range(0);
        return c.json({ ok: true, rows });
    } catch (err) {
        const message = err instanceof Error ? err.message : "Unknown error";
        return c.json({ ok: false, error: message }, 400);
    }
});

if (import.meta.main) {
    Bun.serve({
        fetch: app.fetch,
    });
}


