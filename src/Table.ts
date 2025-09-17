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

type ColumnBuckets = {
  string: Record<string, FenwickBaseMeta<string, BaseSegment<string>>>;
  number: Record<string, FenwickBaseMeta<number, BaseSegment<number>>>;
};

export type TableMeta<T> = {
  defaults: { segmentCount: number; chunkCount: number };
  order: {
    key: string;
    // Optional for backward compatibility; when present, accelerates rehydrate
    valueType?: ValueType;
    meta: FenwickBaseMeta<T, BaseSegment<T> & { min: T; max: T }>;
  };
  columns: ColumnBuckets;
};

export class Table<T> {
  private columns!: Record<
    string,
    | { type: "string"; col: IndexedColumnInterface<string> }
    | { type: "number"; col: IndexedColumnInterface<number> }
  >;
  private defaultsegmentCount!: number;
  private defaultchunkCount!: number;
  private meta?: TableMeta<T>; // committed snapshot, only updated after successful flush
  private store: IStore;
  private order!: { key: string; column: OrderedColumnInterface<T> };
  private orderValueType: ValueType = "number";

  // Overloads: construct from meta OR from explicit columns
  constructor(store: IStore, meta: TableMeta<T>);
  constructor(
    store: IStore,
    order: { key: string; column: OrderedColumnInterface<T> },
    columns?: Record<
      string,
      | { type: "string"; column: IndexedColumnInterface<string> }
      | { type: "number"; column: IndexedColumnInterface<number> }
    >,
    opts?: { segmentCount?: number; chunkCount?: number },
  );
  constructor(
    store: IStore,
    orderOrMeta:
      | TableMeta<T>
      | { key: string; column: OrderedColumnInterface<T> },
    columns?: Record<
      string,
      | { type: "string"; column: IndexedColumnInterface<string> }
      | { type: "number"; column: IndexedColumnInterface<number> }
    >,
    opts?: { segmentCount?: number; chunkCount?: number },
  ) {
    this.store = store;
    if (Table.isTableMeta(orderOrMeta)) {
      this.rehydrate(orderOrMeta as TableMeta<T>);
      return;
    }

    // Legacy signature
    this.order = orderOrMeta as { key: string; column: OrderedColumnInterface<T> };
    this.columns = {};
    if (columns) {
      for (const [key, spec] of Object.entries(columns)) {
        if (spec.type === "number")
          this.columns[key] = { type: "number", col: spec.column } as {
            type: "number";
            col: IndexedColumnInterface<number>;
          };
        else
          this.columns[key] = { type: "string", col: spec.column } as {
            type: "string";
            col: IndexedColumnInterface<string>;
          };
      }
    }
    this.defaultsegmentCount = opts?.segmentCount ?? 8192;
    this.defaultchunkCount = opts?.chunkCount ?? 0;
  }

  private async ensureTypedColumn(
    key: string,
    valueType: ValueType,
  ): Promise<IndexedColumnInterface<string | number>> {
    const existing = this.columns[key];
    if (existing) return (existing as unknown as { col: IndexedColumnInterface<string | number> }).col;
    if (valueType === "number") {
      const col = new IndexedColumn<number>(this.store, {
        segmentCount: this.defaultsegmentCount,
        chunkCount: this.defaultchunkCount,
      });
      this.columns[key] = { type: "number", col } as {
        type: "number";
        col: IndexedColumnInterface<number>;
      };
      return col as unknown as IndexedColumnInterface<string | number>;
    }
    const col = new IndexedColumn<string>(this.store, {
      segmentCount: this.defaultsegmentCount,
      chunkCount: this.defaultchunkCount,
    });
    this.columns[key] = { type: "string", col } as {
      type: "string";
      col: IndexedColumnInterface<string>;
    };
    return col as unknown as IndexedColumnInterface<string | number>;
  }

  async insert(rows: Row[]): Promise<void> {
    for (const row of rows) {
      const value = row[this.order.key];
      if (value === undefined) {
        throw new Error(`Row is missing key ${this.order.key}`);
      }
      const index = await this.order.column.insert(value as T);

      // Insert values for keys present in this row (creating typed columns as needed)
      const tasks: Array<Promise<void>> = [];

      // Handle keys present in this row (except the order key)
      for (const rowKey of Object.keys(row)) {
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
        const col = await this.ensureTypedColumn(rowKey, vt);
        if (vt === "number") {
          tasks.push((col as unknown as IndexedColumnInterface<number>).insertAt(index, v as number));
        } else {
          tasks.push((col as unknown as IndexedColumnInterface<string>).insertAt(index, v as string));
        }
      }

      // For pre-existing typed columns not present in this row, insert undefined at the index to maintain alignment
      for (const [key, spec] of Object.entries(this.columns)) {
        if (key === this.order.key) continue;
        if (key in row) continue;
        const col = (spec as unknown as { type: ValueType; col: IndexedColumnInterface<string | number> }).col;
        if ((spec as unknown as { type: ValueType }).type === "number")
          tasks.push((col as unknown as IndexedColumnInterface<number>).insertAt(index, undefined as unknown as number));
        else
          tasks.push((col as unknown as IndexedColumnInterface<string>).insertAt(index, undefined as unknown as string));
      }

      await Promise.all(tasks);
    }
  }

  async get(index: number): Promise<Row> {
    const typedEntries: Array<[
      string,
      IndexedColumnInterface<string | number>,
    ]> = Object.entries(this.columns)
      .filter(([key]) => key !== this.order.key)
      .map(([key, spec]) => [key, (spec as unknown as { col: IndexedColumnInterface<string | number> }).col]);

    const values = await Promise.all(
      typedEntries.map(([, column]) => column.get(index)),
    );

    const row: Row = {};
    for (let i = 0; i < typedEntries.length; i++) {
      const key = typedEntries[i]![0];
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
      string,
      IndexedColumnInterface<string | number>,
    ]> = Object.entries(this.columns)
      .filter(([key]) => key !== this.order.key)
      .map(([key, spec]) => [key, (spec as unknown as { col: IndexedColumnInterface<string | number> }).col]);

    const len = orderValues.length;
    const rows: Row[] = new Array<Row>(len);
    for (let i = 0; i < len; i++) {
      rows[i] = { [this.order.key]: orderValues[i] as unknown as T } as Row;
    }
    // Fetch column values aligned by absolute index a+i
    await Promise.all(
      typedEntries.map(async ([key, column]) => {
        const values = await Promise.all(
          Array.from({ length: len }, (_, i) => column.get(a + i)),
        );
        for (let i = 0; i < len; i++) {
          const v = values[i];
          if (v !== undefined) rows[i]![key] = v as unknown as T;
        }
      }),
    );
    return rows;
  }

  async flush(metaKey: string): Promise<void> {
    // Flush order column and all other columns in parallel
    await Promise.all([
      this.order.column.flush(),
      ...Object.values(this.columns).map((spec) => (spec as unknown as { col: IndexedColumnInterface<string | number> }).col.flush()),
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
    this.rehydrate(meta);
  }

  private buildMetaSnapshot(): TableMeta<T> {
    const orderMeta = this.order.column.getMeta();
    const cols: TableMeta<T>["columns"] = { string: {}, number: {} };
    for (const [key, spec] of Object.entries(this.columns)) {
      if ((key as string) === this.order.key) continue;
      const type = (spec as unknown as { type: ValueType }).type;
      if (type === "number") {
        (cols.number as Record<string, FenwickBaseMeta<number, BaseSegment<number>>>)[
          key
        ] = ((spec as unknown as { col: IndexedColumnInterface<number> }).col.getMeta() as unknown) as FenwickBaseMeta<
          number,
          BaseSegment<number>
        >;
      } else {
        (cols.string as Record<string, FenwickBaseMeta<string, BaseSegment<string>>>)[
          key
        ] = ((spec as unknown as { col: IndexedColumnInterface<string> }).col.getMeta() as unknown) as FenwickBaseMeta<
          string,
          BaseSegment<string>
        >;
      }
    }
    return {
      defaults: {
        segmentCount: this.defaultsegmentCount,
        chunkCount: this.defaultchunkCount,
      },
      order: {
        key: this.order.key,
        valueType: this.orderValueType,
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

  private rehydrate(meta: TableMeta<T> | (TableMeta<T> & { columns: Record<string, { type: ValueType; meta: unknown }> })): void {
    this.defaultsegmentCount = meta.defaults.segmentCount ?? 8192;
    this.defaultchunkCount = meta.defaults.chunkCount ?? 0;
    // Determine order value type; optional for backward compat
    const orderType = (meta.order as { valueType?: ValueType }).valueType ?? this.orderValueType;
    this.orderValueType = orderType ?? this.orderValueType;
    const orderColumn =
      (orderType ?? "number") === "number"
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
    const anyMeta = meta as unknown as {
      columns:
      | ColumnBuckets
      | Record<string, { type: ValueType; meta: unknown }>;
    };
    if (
      (anyMeta.columns as ColumnBuckets).string !== undefined ||
      (anyMeta.columns as ColumnBuckets).number !== undefined
    ) {
      // Old/desired shape: typed buckets
      const buckets = anyMeta.columns as ColumnBuckets;
      for (const [key, m] of Object.entries(
        buckets.number ?? ({} as Record<string, FenwickBaseMeta<number, BaseSegment<number>>>),
      )) {
        const col = new IndexedColumn<number>(
          this.store,
          (m as unknown) as FenwickBaseMeta<number, BaseSegment<number>>,
        );
        this.columns[key] = { type: "number", col } as {
          type: "number";
          col: IndexedColumnInterface<number>;
        };
      }
      for (const [key, m] of Object.entries(
        buckets.string ?? ({} as Record<string, FenwickBaseMeta<string, BaseSegment<string>>>),
      )) {
        const col = new IndexedColumn<string>(
          this.store,
          (m as unknown) as FenwickBaseMeta<string, BaseSegment<string>>,
        );
        this.columns[key] = { type: "string", col } as {
          type: "string";
          col: IndexedColumnInterface<string>;
        };
      }
    } else {
      // Newer flat shape (backward compat): map of { type, meta }
      for (const [key, entry] of Object.entries(
        anyMeta.columns as Record<string, { type: ValueType; meta: unknown }>,
      )) {
        if (entry.type === "number") {
          const col = new IndexedColumn<number>(
            this.store,
            (entry.meta as unknown) as FenwickBaseMeta<number, BaseSegment<number>>,
          );
          this.columns[key] = { type: "number", col } as {
            type: "number";
            col: IndexedColumnInterface<number>;
          };
        } else {
          const col = new IndexedColumn<string>(
            this.store,
            (entry.meta as unknown) as FenwickBaseMeta<string, BaseSegment<string>>,
          );
          this.columns[key] = { type: "string", col } as {
            type: "string";
            col: IndexedColumnInterface<string>;
          };
        }
      }
    }
    this.meta = Table.cloneMeta(meta as TableMeta<T>);
  }
}
