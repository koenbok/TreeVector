import { describe, it, expect } from "bun:test";
import { Table } from "./Table";
import { MemoryStore } from "./Store";
import { OrderedColumn } from "./Column";

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
    await table.flush();
    const rows = await table.range(0, 3);
    expect(rows.map((r) => r.id)).toEqual([1, 2, 3]);
    expect(rows.map((r) => r.name)).toEqual(["a", "b", "c"]);
    expect(rows.map((r) => r.score)).toEqual([1, 2, undefined]);
  });
});
