import {
  IndexedColumn,
  type IndexedColumnInterface,
  type OrderedColumnInterface,
} from "./Column";
import type { IStore } from "./Store";

type Row = Record<string, unknown>;

export class Table<T> {
  private columns: Record<string, IndexedColumnInterface<unknown>>;
  private readonly defaultsegmentCount: number;
  private readonly defaultchunkCount: number;

  constructor(
    private store: IStore,
    private order: { key: string; column: OrderedColumnInterface<T> },
    columns?: Record<string, IndexedColumnInterface<unknown>>,
    opts?: { segmentCount?: number; chunkCount?: number },
  ) {
    this.columns = columns ?? {};
    this.defaultsegmentCount = opts?.segmentCount ?? 8192;
    this.defaultchunkCount = opts?.chunkCount ?? 0;
  }

  private ensureColumnFor(
    key: string,
    sample: unknown,
  ): IndexedColumnInterface<unknown> {
    const existing = this.columns[key];
    if (existing) return existing;
    const t = typeof sample;
    if (t === "number") {
      const col = new IndexedColumn<number>(this.store, {
        segmentCount: this.defaultsegmentCount,
        chunkCount: this.defaultchunkCount,
      });
      this.columns[key] = col as unknown as IndexedColumnInterface<unknown>;
      return this.columns[key] as IndexedColumnInterface<unknown>;
    }
    if (t === "string") {
      const col = new IndexedColumn<string>(this.store, {
        segmentCount: this.defaultsegmentCount,
        chunkCount: this.defaultchunkCount,
      });
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
      // 1) Insert values for keys present in this row (creating columns as needed)
      // 2) Pad any pre-existing columns that are missing in this row with undefined
      const tasks: Array<Promise<void>> = [];
      const rowKeys = new Set(Object.keys(row));

      // 1) Handle keys present in this row (except the order key)
      for (const rowKey of rowKeys) {
        if (rowKey === this.order.key) continue;
        const v = row[rowKey];
        const col = this.ensureColumnFor(rowKey, v);
        tasks.push(col.insertAt(index, v as unknown as T));
      }

      // 2) Pad missing pre-existing columns
      for (const key in this.columns) {
        if (key === this.order.key) continue;
        if (!rowKeys.has(key)) {
          const col = this.columns[key] as IndexedColumnInterface<unknown>;
          tasks.push(col.insertAt(index, undefined as unknown as T));
        }
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
