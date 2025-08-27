import { FenwickColumn, type IndexedColumnInterface, type OrderedColumnInterface } from "./Column";
import type { IStore } from "./Store";

type Row = Record<string, unknown>;

export class Table<T> {
  private columns: Record<string, IndexedColumnInterface<unknown>>;
  private readonly defaultSegmentN: number;
  private readonly defaultChunkN: number;

  constructor(
    private store: IStore,
    private order: { key: string; column: OrderedColumnInterface<T> },
    columns?: Record<string, IndexedColumnInterface<unknown>>,
    opts?: { segmentN?: number; chunkN?: number },
  ) {
    this.columns = columns ?? {};
    this.defaultSegmentN = opts?.segmentN ?? 8192;
    this.defaultChunkN = opts?.chunkN ?? 0;
  }

  private ensureColumnFor(key: string, sample: unknown): IndexedColumnInterface<unknown> {
    const existing = this.columns[key];
    if (existing) return existing;
    const t = typeof sample;
    if (t === "number") {
      const col = new FenwickColumn<number>(
        this.store,
        this.defaultSegmentN,
        this.defaultChunkN,
      );
      this.columns[key] = col as unknown as IndexedColumnInterface<unknown>;
      return this.columns[key] as IndexedColumnInterface<unknown>;
    }
    if (t === "string") {
      const col = new FenwickColumn<string>(
        this.store,
        this.defaultSegmentN,
        this.defaultChunkN,
      );
      this.columns[key] = col as unknown as IndexedColumnInterface<unknown>;
      return this.columns[key] as IndexedColumnInterface<unknown>;
    }
    throw new Error(`Unsupported column type for key "${key}": ${t}`);
  }

  async insert(rows: Row[]): Promise<void> {
    for (const row of rows) {
      const value = row[this.order.key];
      if (value === undefined) {
        throw new Error(`Row is missing key ${this.order.key}`);
      }
      const index = await this.order.column.insert(value as T);

      // Insert non-order columns in parallel for this row.
      // Columns are created on-demand based on the value type (number|string).
      const tasks: Array<Promise<void>> = [];
      for (const rowKey in row) {
        if (rowKey === this.order.key) continue;
        const value = row[rowKey];
        const col = this.ensureColumnFor(rowKey, value);
        tasks.push(col.insert(index, value as unknown as T));
      }
      await Promise.all(tasks);
    }
  }

  async get(index: number): Promise<Row> {
    const columnEntries = Object.entries(this.columns);
    const values = await Promise.all(
      columnEntries.map(([, column]) => column.get(index)),
    );

    const row: Row = {};
    for (let i = 0; i < columnEntries.length; i++) {
      const key = columnEntries[i]?.[0]!;
      row[key] = values[i];
    }
    return row;
  }

  async range(offset?: number, limit?: number): Promise<Row[]> {
    const a = Math.max(0, offset ?? 0);
    const b = a + (limit ?? Number.POSITIVE_INFINITY);

    const orderValues = await this.order.column.range(a, b);
    const otherEntries: [string, T[]][] = await Promise.all(
      Object.entries(this.columns).map(async ([key, column]) => [
        key,
        (await column.range(a, b)) as T[],
      ]),
    );
    const columns: Record<string, T[]> = Object.fromEntries([
      [this.order.key, orderValues as T[]],
      ...otherEntries,
    ]);

    const orderArr = columns[this.order.key] ?? [];
    const len = orderArr.length;
    const rows: Row[] = [];

    for (let i = 0; i < len; i++) {
      const row: Row = {};
      row[this.order.key] = orderArr[i] as unknown as T;
      for (const key of Object.keys(columns)) {
        if (key === this.order.key) continue;
        const valuesForKey = columns[key] as T[];
        row[key] = valuesForKey[i] as unknown as T;
      }
      rows.push(row);
    }
    return rows;
  }

  async flush(): Promise<void> {
    // Flush order column and all other columns in parallel
    await Promise.all([
      this.order.column.flush(),
      ...Object.values(this.columns).map((column) => column.flush()),
    ]);
  }
}
