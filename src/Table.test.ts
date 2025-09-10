import { describe, it, expect } from "bun:test";
import { Table } from "./Table";
import { MemoryStore } from "./Store";
import {
  IndexedColumn,
  OrderedColumn,
  type IndexedColumnInterface,
} from "./Column";
import type { TableMeta } from "./Table";

type Row = { id: number; name: string };

describe("Table", () => {
  it("inserts rows and get returns non-order columns by index", async () => {
    const store = new MemoryStore();
    const table = new Table<number>(
      store,
      {
        key: "id",
        column: new OrderedColumn<number>(store, {
          segmentCount: 4,
          chunkCount: 10,
        }),
      },
      {
        number: {
          name: new IndexedColumn<number>(store, {
            segmentCount: 4,
            chunkCount: 10,
          }),
        },
      } as unknown as {
        string?: Record<string, IndexedColumnInterface<string>>;
        number?: Record<string, IndexedColumnInterface<number>>;
      },
    );

    // Insert three rows
    await table.insert([
      { id: 2, name: 200 },
      { id: 1, name: 100 },
      { id: 3, name: 300 },
    ]);

    // Sorted by id -> order indices correspond to ids [1,2,3]
    const r0 = await table.get(0);
    const r1 = await table.get(1);
    const r2 = await table.get(2);

    // get() only returns non-order columns (here: name) as flat values
    expect(r0).toEqual({ name: 100 });
    expect(r1).toEqual({ name: 200 });
    expect(r2).toEqual({ name: 300 });
  });

  it("range(offset, limit) returns rows in index slice", async () => {
    const store = new MemoryStore();
    const table = new Table<number>(
      store,
      {
        key: "id",
        column: new OrderedColumn<number>(store, {
          segmentCount: 4,
          chunkCount: 10,
        }),
      },
      {
        number: {
          name: new IndexedColumn<number>(store, {
            segmentCount: 4,
            chunkCount: 10,
          }),
        },
      } as unknown as {
        string?: Record<string, IndexedColumnInterface<string>>;
        number?: Record<string, IndexedColumnInterface<number>>;
      },
    );

    await table.insert([
      { id: 10, name: 100 },
      { id: 30, name: 300 },
      { id: 20, name: 200 },
      { id: 40, name: 400 },
    ]);
    const rows = await table.range(1, 2); // indices 1..2 -> ids [20,30]
    expect(rows.map((r) => r.id)).toEqual([20, 30]);
    expect(rows.map((r) => r.name)).toEqual([200, 300]);
  });

  it("range with undefined limit returns to the end", async () => {
    const store = new MemoryStore();
    const table = new Table<number>(
      store,
      {
        key: "id",
        column: new OrderedColumn<number>(store, {
          segmentCount: 4,
          chunkCount: 10,
        }),
      },
      {
        number: {
          name: new IndexedColumn<number>(store, {
            segmentCount: 4,
            chunkCount: 10,
          }),
        },
      } as unknown as {
        string?: Record<string, IndexedColumnInterface<string>>;
        number?: Record<string, IndexedColumnInterface<number>>;
      },
    );

    await table.insert([
      { id: 10, name: 100 },
      { id: 30, name: 300 },
      { id: 20, name: 200 },
      { id: 40, name: 400 },
    ]);

    const rows = await table.range(2); // from index 2 to end -> ids [30,40]
    expect(rows.map((r) => r.id)).toEqual([30, 40]);
    expect(rows.map((r) => r.name)).toEqual([300, 400]);
  });

  it("throws if a row is missing the order key", async () => {
    const store = new MemoryStore();
    const table = new Table<number>(
      store,
      {
        key: "id",
        column: new OrderedColumn<number>(store, {
          segmentCount: 4,
          chunkCount: 10,
        }),
      },
      {
        name: new IndexedColumn<number>(store, {
          segmentCount: 4,
          chunkCount: 10,
        }),
      } as unknown as Record<string, IndexedColumnInterface<number>>,
    );

    await expect(
      table.insert([{ name: 123 } as unknown as Row]),
    ).rejects.toBeTruthy();
  });
});

describe("Table (mixed types heavy)", () => {
  it("handles mixed string/number/null/undefined in same column and preserves values", async () => {
    const store = new MemoryStore();
    const table = new Table<number>(
      store,
      {
        key: "id",
        column: new OrderedColumn<number>(store, { segmentCount: 8, chunkCount: 0 }),
      },
      undefined,
      { segmentCount: 8, chunkCount: 0 },
    );

    const N = 100;
    const input: Array<{ id: number; value?: string | number | null }> = [];
    for (let i = 1; i <= N; i++) {
      const id = N - i + 1; // insert in reverse to exercise ordering
      let v: string | number | null | undefined;
      if (i % 10 === 0) v = null; // becomes undefined
      else if (i % 10 === 1) v = undefined;
      else if (i % 2 === 0) v = i; // number
      else v = `s${i}`; // string
      input.push({ id, value: v as any });
    }

    await table.insert(input as any);

    const rows = await table.range(0);
    expect(rows.length).toBe(N);
    // rows are sorted by id ascending; compute expected per id
    for (let id = 1; id <= N; id++) {
      const i2 = N - id + 1;
      let expected: string | number | undefined;
      if (i2 % 10 === 0) expected = undefined;
      else if (i2 % 10 === 1) expected = undefined;
      else if (i2 % 2 === 0) expected = i2;
      else expected = `s${i2}`;
      expect(rows[id - 1]?.id).toBe(id);
      expect(rows[id - 1]?.value).toBe(expected);
    }

    // Meta should contain both typed columns for 'value'
    const meta = table.getMeta();
    expect(Object.keys(meta.columns.number)).toContain("value");
    expect(Object.keys(meta.columns.string)).toContain("value");
  });
});

describe("Table (dynamic columns)", () => {
  it("auto-creates number and string columns based on row keys", async () => {
    const store = new MemoryStore();
    const table = new Table<number>(
      store,
      {
        key: "id",
        column: new OrderedColumn<number>(store, {
          segmentCount: 8,
          chunkCount: 0,
        }),
      },
      /* columns */ undefined,
      { segmentCount: 8, chunkCount: 0 },
    );

    await table.insert([
      { id: 2, name: "bob", score: 7 },
      { id: 1, name: "alice", score: 10 },
      { id: 3, name: "cara", score: 5 },
    ]);

    // Sorted by id: [1,2,3]
    const r0 = await table.get(0);
    const r1 = await table.get(1);
    const r2 = await table.get(2);
    expect(r0).toEqual({ name: "alice", score: 10 });
    expect(r1).toEqual({ name: "bob", score: 7 });
    expect(r2).toEqual({ name: "cara", score: 5 });

    const rows = await table.range(0, 3);
    expect(rows.map((r) => r.id)).toEqual([1, 2, 3]);
    expect(rows.map((r) => r.name)).toEqual(["alice", "bob", "cara"]);
    expect(rows.map((r) => r.score)).toEqual([10, 7, 5]);
  });

  it("adds columns when first seen; earlier rows read as undefined for that column", async () => {
    const store = new MemoryStore();
    const table = new Table<number>(
      store,
      {
        key: "id",
        column: new OrderedColumn<number>(store, {
          segmentCount: 8,
          chunkCount: 0,
        }),
      },
      undefined,
      { segmentCount: 8, chunkCount: 0 },
    );

    await table.insert([{ id: 2, name: "bob" }]); // only name column created
    await table.insert([{ id: 1, score: 10 }]); // later introduces score column

    // Sorted by id -> [1,2]
    const r0 = await table.get(0);
    const r1 = await table.get(1);
    expect(r0).toEqual({ name: undefined, score: 10 });
    expect(r1).toEqual({ name: "bob", score: undefined });

    const rows = await table.range(0, 2);
    expect(rows.map((r) => r.id)).toEqual([1, 2]);
    expect(rows.map((r) => r.name)).toEqual([undefined, "bob"]);
    expect(rows.map((r) => r.score)).toEqual([10, undefined]);
  });

  it("throws on unsupported column value types", async () => {
    const store = new MemoryStore();
    const table = new Table<number>(
      store,
      {
        key: "id",
        column: new OrderedColumn<number>(store, {
          segmentCount: 8,
          chunkCount: 0,
        }),
      },
      undefined,
      { segmentCount: 8, chunkCount: 0 },
    );

    await expect(
      table.insert([{ id: 1, active: true } as unknown as { id: number }]),
    ).rejects.toBeTruthy();

    await expect(
      table.insert([
        { id: 2, meta: { a: 1 } as unknown } as unknown as { id: number },
      ]),
    ).rejects.toBeTruthy();
  });

  it("flush persists after dynamic creation", async () => {
    const store = new MemoryStore();
    const table = new Table<number>(
      store,
      {
        key: "id",
        column: new OrderedColumn<number>(store, {
          segmentCount: 8,
          chunkCount: 0,
        }),
      },
      undefined,
      { segmentCount: 4, chunkCount: 0 },
    );
    await table.insert([
      { id: 3, name: "c" },
      { id: 1, name: "a", score: 1 },
      { id: 2, name: "b", score: 2 },
    ]);
    await table.flush("table.meta");
    const rows = await table.range(0, 3);
    expect(rows.map((r) => r.id)).toEqual([1, 2, 3]);
    expect(rows.map((r) => r.name)).toEqual(["a", "b", "c"]);
    expect(rows.map((r) => r.score)).toEqual([1, 2, undefined]);
  });
});

describe("Table ACID meta semantics", () => {
  it("flush(metaKey) persists meta to store and allows reconstruction", async () => {
    const store = new MemoryStore();
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
      { id: 3, name: "c" },
    ]);

    const metaKey = "tables/users.meta";
    await table.flush(metaKey);

    const stored = (await store.get<TableMeta<number>>(metaKey))!;
    expect(stored.order.key).toBe("id");
    expect(Object.keys(stored.columns.string)).toContain("name");

    const rehydrated = new Table<number>(store, stored);
    const rows = await rehydrated.range(0, 3);
    expect(rows.map((r) => r.id)).toEqual([1, 2, 3]);
    expect(rows.map((r) => r.name)).toEqual(["a", "b", "c"]);
  });

  it("does not update stored meta if any column flush fails (atomic meta commit)", async () => {
    const store = new MemoryStore();
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

    const metaKey = "tables/acids.meta";
    await table.flush(metaKey);
    const v1 = (await store.get<TableMeta<number>>(metaKey))!;

    // Replace the name column with a failing implementation that throws on flush
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
    // Inject failing column into string bucket
    (table as unknown as { columns: { string: Record<string, IndexedColumnInterface<unknown>>; number: Record<string, IndexedColumnInterface<unknown>> } }).columns.string["name"] = failingCol;

    await table.insert([{ id: 3, name: "c" }]);

    await expect(table.flush(metaKey)).rejects.toBeTruthy();

    const vAfter = await store.get<TableMeta<number>>(metaKey);
    expect(vAfter).toEqual(v1); // meta not updated
  });

  it("setMeta(meta) reinitializes the table to the committed snapshot", async () => {
    const store = new MemoryStore();
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
    await table.flush("tables/setmeta.meta");
    const meta = (await store.get<TableMeta<number>>("tables/setmeta.meta"))!;

    // Create a different table, then set meta to reinitialize
    const other = new Table<number>(
      store,
      {
        key: "id",
        column: new OrderedColumn<number>(store, { segmentCount: 2, chunkCount: 2 }),
      },
      { string: { name: new IndexedColumn<string>(store, { segmentCount: 2, chunkCount: 2 }) } } as unknown as { string?: Record<string, IndexedColumnInterface<string>>; number?: Record<string, IndexedColumnInterface<number>> },
      { segmentCount: 2, chunkCount: 2 },
    );
    other.setMeta(meta);

    const rows = await other.range(0, 2);
    expect(rows.map((r) => r.id)).toEqual([1, 2]);
    expect(rows.map((r) => r.name)).toEqual(["a", "b"]);
  });
});

describe("Table (rows echo)", () => {
  it("rows out equal rows in (null becomes undefined)", async () => {
    const store = new MemoryStore();
    const table = new Table<number>(
      store,
      {
        key: "id",
        column: new OrderedColumn<number>(store, { segmentCount: 4, chunkCount: 4 }),
      },
      undefined,
      { segmentCount: 4, chunkCount: 4 },
    );

    const input = [
      { id: 2, name: "b", score: 2, note: null as any },
      { id: 1, name: "a", score: 1 },
      { id: 3, name: undefined as any, score: "3" },
    ];

    await table.insert(input);
    const out = await table.range(0);

    // Sorted by id: 1,2,3
    expect(out[0]).toEqual({ id: 1, name: "a", score: 1 });
    expect(out[1]).toEqual({ id: 2, name: "b", score: 2, note: undefined });
    expect(out[2]).toEqual({ id: 3, name: undefined, score: "3" });
  });

});
