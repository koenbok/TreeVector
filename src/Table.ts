import {
  IndexedColumn,
  OrderedColumn,
  type IndexedColumnInterface,
  type OrderedColumnInterface,
} from "./Column";
import type { IStore } from "./Store";
import type {
  FenwickBaseMeta,
  BaseSegment,
} from "./FenwickBase";

type Row = Record<string, unknown>;

type ValueType = "number" | "string";

export type TableMeta<T> = {
  defaults: { segmentCount: number; chunkCount: number };
  order: {
    key: string;
    valueType: ValueType;
    meta: FenwickBaseMeta<T, BaseSegment<T> & { min: T; max: T }>;
  };
  columns: Record<
    string,
    {
      valueType: ValueType;
      meta: FenwickBaseMeta<unknown, BaseSegment<unknown>>;
    }
  >;
};

export class Table<T> {
  private columns: Record<string, IndexedColumnInterface<unknown>>;
  private defaultsegmentCount: number;
  private defaultchunkCount: number;
  private meta?: TableMeta<T>; // committed snapshot, only updated after successful flush
  private store: IStore;
  private order!: { key: string; column: OrderedColumnInterface<T> };

  // Overloads: construct from meta OR from explicit columns
  constructor(store: IStore, meta: TableMeta<T>);
  constructor(
    store: IStore,
    order: { key: string; column: OrderedColumnInterface<T> },
    columns?: Record<string, IndexedColumnInterface<unknown>>,
    opts?: { segmentCount?: number; chunkCount?: number },
  );
  constructor(
    store: IStore,
    orderOrMeta:
      | TableMeta<T>
      | { key: string; column: OrderedColumnInterface<T> },
    columns?: Record<string, IndexedColumnInterface<unknown>>,
    opts?: { segmentCount?: number; chunkCount?: number },
  ) {
    this.store = store;
    if (Table.isTableMeta(orderOrMeta)) {
      const meta = orderOrMeta as TableMeta<T>;
      this.defaultsegmentCount = meta.defaults.segmentCount ?? 8192;
      this.defaultchunkCount = meta.defaults.chunkCount ?? 0;
      // Reconstruct order and columns from meta
      const orderColumn =
        meta.order.valueType === "number"
          ? (new OrderedColumn<number>(
            this.store,
            meta.order.meta as unknown as FenwickBaseMeta<
              number,
              BaseSegment<number> & { min: number; max: number }
            >,
          ) as unknown as OrderedColumnInterface<T>)
          : (new OrderedColumn<string>(
            this.store,
            meta.order.meta as unknown as FenwickBaseMeta<
              string,
              BaseSegment<string> & { min: string; max: string }
            >,
          ) as unknown as OrderedColumnInterface<T>);
      this.order = { key: meta.order.key, column: orderColumn };

      this.columns = {};
      for (const [key, info] of Object.entries(meta.columns)) {
        const col =
          info.valueType === "number"
            ? (new IndexedColumn<number>(
              this.store,
              info.meta as FenwickBaseMeta<number, BaseSegment<number>>,
            ) as unknown as IndexedColumnInterface<unknown>)
            : (new IndexedColumn<string>(
              this.store,
              info.meta as FenwickBaseMeta<string, BaseSegment<string>>,
            ) as unknown as IndexedColumnInterface<unknown>);
        this.columns[key] = col;
      }
      // Set committed meta
      this.meta = Table.cloneMeta(meta);
      return;
    }

    // Legacy signature
    this.order = orderOrMeta as { key: string; column: OrderedColumnInterface<T> };
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

  async flush(metaKey: string): Promise<void> {
    // Flush order column and all other columns in parallel
    await Promise.all([
      this.order.column.flush(),
      ...Object.values(this.columns).map((column) => column.flush()),
    ]);
    // Only after successful flush, commit a new meta snapshot (and persist if key provided)
    const snapshot = this.buildMetaSnapshot();
    await this.store.set<TableMeta<T>>(metaKey, snapshot);
    this.meta = Table.cloneMeta(snapshot);
  }

  getMeta(): TableMeta<T> {
    // If we have a committed snapshot, expose it
    return this.meta ?? this.buildMetaSnapshot();
  }

  setMeta(meta: TableMeta<T>): void {
    // Reinitialize from meta directly (no extra state)
    this.defaultsegmentCount = meta.defaults.segmentCount ?? 8192;
    this.defaultchunkCount = meta.defaults.chunkCount ?? 0;
    const orderColumn =
      meta.order.valueType === "number"
        ? (new OrderedColumn<number>(
          this.store,
          meta.order.meta as unknown as FenwickBaseMeta<
            number,
            BaseSegment<number> & { min: number; max: number }
          >,
        ) as unknown as OrderedColumnInterface<T>)
        : (new OrderedColumn<string>(
          this.store,
          meta.order.meta as unknown as FenwickBaseMeta<
            string,
            BaseSegment<string> & { min: string; max: string }
          >,
        ) as unknown as OrderedColumnInterface<T>);
    this.order = { key: meta.order.key, column: orderColumn };
    this.columns = {};
    for (const [key, info] of Object.entries(meta.columns)) {
      const col =
        info.valueType === "number"
          ? (new IndexedColumn<number>(
            this.store,
            info.meta as FenwickBaseMeta<number, BaseSegment<number>>,
          ) as unknown as IndexedColumnInterface<unknown>)
          : (new IndexedColumn<string>(
            this.store,
            info.meta as FenwickBaseMeta<string, BaseSegment<string>>,
          ) as unknown as IndexedColumnInterface<unknown>);
      this.columns[key] = col;
    }
    this.meta = Table.cloneMeta(meta);
  }

  private buildMetaSnapshot(): TableMeta<T> {
    const orderMeta = this.order.column.getMeta();
    const cols: TableMeta<T>["columns"] = {};
    for (const [key, column] of Object.entries(this.columns)) {
      // Infer value type from existing committed meta if available; default to number
      const vt = (this.meta?.columns?.[key]?.valueType as ValueType | undefined) ?? "number";
      cols[key] = {
        valueType: vt,
        meta: column.getMeta() as unknown as FenwickBaseMeta<
          unknown,
          BaseSegment<unknown>
        >,
      };
    }
    return {
      defaults: {
        segmentCount: this.defaultsegmentCount,
        chunkCount: this.defaultchunkCount,
      },
      order: {
        key: this.order.key,
        // Keep existing committed type if available; otherwise best-effort infer from first segment min
        valueType:
          (this.meta?.order.valueType as ValueType | undefined) ??
          (orderMeta.segments.length > 0
            ? (typeof (orderMeta.segments[0] as unknown as { min: unknown }).min === "string"
              ? "string"
              : "number")
            : "number"),
        meta: orderMeta as unknown as FenwickBaseMeta<
          T,
          BaseSegment<T> & { min: T; max: T }
        >,
      },
      columns: cols,
    } as TableMeta<T>;
  }

  private static isTableMeta<T>(arg: unknown): arg is TableMeta<T> {
    return (
      !!arg &&
      typeof arg === "object" &&
      "order" in (arg as Record<string, unknown>) &&
      "defaults" in (arg as Record<string, unknown>)
    );
  }

  private static cloneMeta<T>(meta: TableMeta<T>): TableMeta<T> {
    return structuredClone(meta) as TableMeta<T>;
  }
}
