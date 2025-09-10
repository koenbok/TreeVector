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
  columns: {
    string: Record<string, FenwickBaseMeta<string, BaseSegment<string>>>;
    number: Record<string, FenwickBaseMeta<number, BaseSegment<number>>>;
  };
};

export class Table<T> {
  private columns: {
    string: Record<string, IndexedColumnInterface<string>>;
    number: Record<string, IndexedColumnInterface<number>>;
  };
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
    columns?: {
      string?: Record<string, IndexedColumnInterface<string>>;
      number?: Record<string, IndexedColumnInterface<number>>;
    },
    opts?: { segmentCount?: number; chunkCount?: number },
  );
  constructor(
    store: IStore,
    orderOrMeta:
      | TableMeta<T>
      | { key: string; column: OrderedColumnInterface<T> },
    columns?: {
      string?: Record<string, IndexedColumnInterface<string>>;
      number?: Record<string, IndexedColumnInterface<number>>;
    },
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
      this.columns = { string: {}, number: {} };
      for (const [key, info] of Object.entries(meta.columns.number)) {
        const col = new IndexedColumn<number>(
          this.store,
          info as FenwickBaseMeta<number, BaseSegment<number>>,
        );
        this.columns.number[key] = col as IndexedColumnInterface<number>;
      }
      for (const [key, info] of Object.entries(meta.columns.string)) {
        const col = new IndexedColumn<string>(
          this.store,
          info as FenwickBaseMeta<string, BaseSegment<string>>,
        );
        this.columns.string[key] = col as IndexedColumnInterface<string>;
      }
      // Set committed meta
      this.meta = Table.cloneMeta(meta);
      return;
    }

    // Legacy signature
    this.order = orderOrMeta as { key: string; column: OrderedColumnInterface<T> };
    this.columns = {
      string: { ...(columns?.string ?? {}) },
      number: { ...(columns?.number ?? {}) },
    };
    this.defaultsegmentCount = opts?.segmentCount ?? 8192;
    this.defaultchunkCount = opts?.chunkCount ?? 0;
  }

  private async ensureTypedColumn(
    key: string,
    valueType: ValueType,
    prefillLength?: number,
  ): Promise<IndexedColumnInterface<string | number>> {
    const bucket = valueType === "string" ? this.columns.string : this.columns.number;
    const existing = bucket[key];
    if (existing) return existing as IndexedColumnInterface<string | number>;
    if (valueType === "number") {
      const col = new IndexedColumn<number>(this.store, {
        segmentCount: this.defaultsegmentCount,
        chunkCount: this.defaultchunkCount,
      });
      bucket[key] = col as unknown as IndexedColumnInterface<number>;
      // Backfill prior rows with undefined so indices align with order length
      const toPad = Math.max(0, prefillLength ?? 0);
      if (toPad > 0) {
        const promises: Promise<void>[] = [];
        for (let i = 0; i < toPad; i++) {
          promises.push((bucket[key] as unknown as IndexedColumnInterface<string | number>).insertAt(999999999, undefined as unknown as number));
        }
        if (promises.length > 0) {
          for (const p of promises) await p;
        }
      }
      return bucket[key] as unknown as IndexedColumnInterface<string | number>;
    }
    const col = new IndexedColumn<string>(this.store, {
      segmentCount: this.defaultsegmentCount,
      chunkCount: this.defaultchunkCount,
    });
    bucket[key] = col as unknown as IndexedColumnInterface<string>;
    // Backfill prior rows with undefined so indices align with order length
    const toPad = Math.max(0, prefillLength ?? 0);
    if (toPad > 0) {
      const promises: Promise<void>[] = [];
      for (let i = 0; i < toPad; i++) {
        promises.push((bucket[key] as unknown as IndexedColumnInterface<string | number>).insertAt(999999999, undefined as unknown as string));
      }
      if (promises.length > 0) {
        for (const p of promises) await p;
      }
    }
    return bucket[key] as unknown as IndexedColumnInterface<string | number>;
  }

  async insert(rows: Row[]): Promise<void> {
    for (const row of rows) {
      const value = row[this.order.key];
      if (value === undefined) {
        throw new Error(`Row is missing key ${this.order.key}`);
      }
      // capture current length before inserting into the order column
      const orderMetaBefore = this.order.column.getMeta();
      const preExistingRows = orderMetaBefore.segments.reduce((sum, s) => sum + (s.count ?? 0), 0);
      const index = await this.order.column.insert(value as T);

      // Insert non-order columns in parallel for this row.
      // 1) Insert values for keys present in this row (creating typed columns as needed)
      // 2) Pad any pre-existing typed columns that are missing in this row with undefined
      const tasks: Array<Promise<void>> = [];
      const rowKeys = new Set(Object.keys(row));
      const insertedTyped = new Set<string>(); // `${type}:${key}`

      // 1) Handle keys present in this row (except the order key)
      for (const rowKey of rowKeys) {
        if (rowKey === this.order.key) continue;
        const v = row[rowKey];
        if (v === null || v === undefined) {
          continue; // treat null as undefined (missing)
        }
        const t = typeof v;
        if (t !== "number" && t !== "string") {
          throw new Error(`Unsupported column type for key "${rowKey}": ${t}`);
        }
        const vt = t as ValueType;
        const col = await this.ensureTypedColumn(rowKey, vt, preExistingRows);
        insertedTyped.add(`${vt}:${rowKey}`);
        if (vt === "number") {
          tasks.push((col as unknown as IndexedColumnInterface<number>).insertAt(index, v as number));
        } else {
          tasks.push((col as unknown as IndexedColumnInterface<string>).insertAt(index, v as string));
        }
      }

      // 2) Pad missing pre-existing typed columns (only number/string buckets)
      const padBuckets: Array<[
        ValueType,
        Record<string, IndexedColumnInterface<string | number>>,
      ]> = [
          [
            "number",
            this.columns.number as unknown as Record<string, IndexedColumnInterface<string | number>>,
          ],
          [
            "string",
            this.columns.string as unknown as Record<string, IndexedColumnInterface<string | number>>,
          ],
        ];
      for (const [vt, bucket] of padBuckets) {
        for (const key in bucket) {
          if (key === this.order.key) continue;
          const sig = `${vt}:${key}`;
          if (!insertedTyped.has(sig)) {
            const col = bucket[key] as IndexedColumnInterface<string | number>;
            if (vt === "number") {
              tasks.push((col as unknown as IndexedColumnInterface<number>).insertAt(index, undefined as unknown as number));
            } else {
              tasks.push((col as unknown as IndexedColumnInterface<string>).insertAt(index, undefined as unknown as string));
            }
          }
        }
      }

      await Promise.all(tasks);
    }
  }

  async get(index: number): Promise<Row> {
    const typedEntries: Array<[
      ValueType,
      string,
      IndexedColumnInterface<string | number>,
    ]> = [];
    for (const [key, col] of Object.entries(this.columns.number)) {
      if (key === this.order.key) continue;
      typedEntries.push(["number", key, col as unknown as IndexedColumnInterface<string | number>]);
    }
    for (const [key, col] of Object.entries(this.columns.string)) {
      if (key === this.order.key) continue;
      typedEntries.push(["string", key, col as unknown as IndexedColumnInterface<string | number>]);
    }

    const values = await Promise.all(typedEntries.map(([, , column]) => column.get(index)));

    const row: Row = {};
    for (let i = 0; i < typedEntries.length; i++) {
      const [, key] = typedEntries[i]!;
      const v = values[i];
      if (v !== undefined) row[key] = v as unknown as T;
    }
    return row;
  }

  async range(offset?: number, limit?: number): Promise<Row[]> {
    const a = Math.max(0, offset ?? 0);
    const b = a + (limit ?? Number.POSITIVE_INFINITY);

    const orderValues = await this.order.column.range(a, b);

    const typedEntries: Array<[
      ValueType,
      string,
      IndexedColumnInterface<string | number>,
    ]> = [];
    for (const [key, col] of Object.entries(this.columns.number)) {
      if (key === this.order.key) continue;
      typedEntries.push(["number", key, col as unknown as IndexedColumnInterface<string | number>]);
    }
    for (const [key, col] of Object.entries(this.columns.string)) {
      if (key === this.order.key) continue;
      typedEntries.push(["string", key, col as unknown as IndexedColumnInterface<string | number>]);
    }

    const typedRanges: Array<[ValueType, string, (string | number)[]]> = await Promise.all(
      typedEntries.map(async ([vt, key, column]) => [
        vt,
        key,
        (await column.range(a, b)) as unknown as (string | number)[],
      ]),
    );

    const len = orderValues.length;
    const rows: Row[] = [];

    for (let i = 0; i < len; i++) {
      const row: Row = {};
      row[this.order.key] = orderValues[i] as unknown as T;
      for (const [, key, arr] of typedRanges) {
        const v = arr[i];
        if (v !== undefined) row[key] = v as unknown as T;
      }
      rows.push(row);
    }
    return rows;
  }

  async flush(metaKey: string): Promise<void> {
    // Flush order column and all other columns in parallel
    await Promise.all([
      this.order.column.flush(),
      ...Object.values(this.columns.number).map((column) => column.flush()),
      ...Object.values(this.columns.string).map((column) => column.flush()),
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
    this.columns = { string: {}, number: {} };
    for (const [key, info] of Object.entries(meta.columns.number)) {
      const col = new IndexedColumn<number>(
        this.store,
        info as FenwickBaseMeta<number, BaseSegment<number>>,
      );
      this.columns.number[key] = col as IndexedColumnInterface<number>;
    }
    for (const [key, info] of Object.entries(meta.columns.string)) {
      const col = new IndexedColumn<string>(
        this.store,
        info as FenwickBaseMeta<string, BaseSegment<string>>,
      );
      this.columns.string[key] = col as IndexedColumnInterface<string>;
    }
    this.meta = Table.cloneMeta(meta);
  }

  private buildMetaSnapshot(): TableMeta<T> {
    const orderMeta = this.order.column.getMeta();
    const cols: TableMeta<T>["columns"] = { string: {}, number: {} };
    for (const [key, column] of Object.entries(this.columns.number)) {
      (cols.number as Record<string, FenwickBaseMeta<number, BaseSegment<number>>>)[
        key
      ] = column.getMeta() as unknown as FenwickBaseMeta<number, BaseSegment<number>>;
    }
    for (const [key, column] of Object.entries(this.columns.string)) {
      (cols.string as Record<string, FenwickBaseMeta<string, BaseSegment<string>>>)[
        key
      ] = column.getMeta() as unknown as FenwickBaseMeta<string, BaseSegment<string>>;
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
            ? ((typeof (orderMeta.segments[0] as unknown as { min: unknown }).min ===
              "string")
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
