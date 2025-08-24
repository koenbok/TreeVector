import { describe, it, expect } from "bun:test";
import { Table } from "./Table";
import { MemoryStore } from "./Store";
import { FenwickColumn, FenwickOrderedColumn } from "./Column";

type Row = { id: number; name: string };

describe("Table", () => {
	it("inserts rows and get returns non-order columns by index", async () => {
		const store = new MemoryStore();
		const table = new Table<number>(
			store,
			{ key: "id", column: new FenwickOrderedColumn<number>(store, 4, 10) },
			{
				name: new FenwickColumn<number>(store, 4, 10),
			} as unknown as Record<string, FenwickColumn<number>>,
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

		// get() only returns non-order columns (here: name)
		expect(r0).toEqual({ name: 100 });
		expect(r1).toEqual({ name: 200 });
		expect(r2).toEqual({ name: 300 });
	});

	it("range(limit, offset) returns rows in index slice", async () => {
		const store = new MemoryStore();
		const table = new Table<number>(
			store,
			{ key: "id", column: new FenwickOrderedColumn<number>(store, 4, 10) },
			{
				name: new FenwickColumn<number>(store, 4, 10),
			} as unknown as Record<string, FenwickColumn<number>>,
		);

		await table.insert([
			{ id: 10, name: 100 },
			{ id: 30, name: 300 },
			{ id: 20, name: 200 },
			{ id: 40, name: 400 },
		]);

		const rows = await table.range(2, 1); // indices 1..2 -> ids [20,30]
		expect(rows.map((r) => r.id)).toEqual([20, 30]);
		expect(rows.map((r) => r.name)).toEqual([200, 300]);
	});

	it("throws if a row is missing the order key", async () => {
		const store = new MemoryStore();
		const table = new Table<number>(
			store,
			{ key: "id", column: new FenwickOrderedColumn<number>(store, 4, 10) },
			{ name: new FenwickColumn<number>(store, 4, 10) } as unknown as Record<
				string,
				FenwickColumn<number>
			>,
		);

		await expect(
			table.insert([{ name: 123 } as unknown as Row]),
		).rejects.toBeTruthy();
	});
});
