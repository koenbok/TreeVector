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
    if (!rows.length) return;

    // 1) Compute order indexes once in input order
    const indexes: number[] = new Array<number>(rows.length);
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i] as Row;
      const value = row[this.order.key];
      if (value === undefined) {
        throw new Error(`Row is missing key ${this.order.key}`);
      }
      indexes[i] = await this.order.column.insert(value as T);
    }

    // 2) Snapshot pre-existing typed columns (excluding order key)
    const preExistingEntries: Array<[
      string,
      { type: ValueType; col: IndexedColumnInterface<string | number> },
    ]> = Object.entries(this.columns)
      .filter(([key]) => key !== this.order.key)
      .map(([key, spec]) => [key, spec as unknown as { type: ValueType; col: IndexedColumnInterface<string | number> }]);

    // 3) Bulk insert per pre-existing column using computed indexes
    for (const [key, spec] of preExistingEntries) {
      const isNumber = (spec as unknown as { type: ValueType }).type === "number";
      const col = (spec as unknown as { col: IndexedColumnInterface<string | number> }).col;
      const vals: Array<string | number | undefined> = new Array(rows.length);
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i] as Row;
        const present = Object.prototype.hasOwnProperty.call(row, key);
        const raw = present ? row[key] : undefined;
        vals[i] = raw === null ? undefined : (raw as unknown as (string | number | undefined));
      }
      // Prefer bulk path if available
      if (typeof (col as any).insertManyAt === "function") {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        await (col as any).insertManyAt(indexes, vals);
      } else {
        for (let i = 0; i < rows.length; i++) {
          const v = vals[i];
          const idx = indexes[i] as number;
          if (isNumber)
            // eslint-disable-next-line @typescript-eslint/no-confusing-void-expression
            await (col as unknown as IndexedColumnInterface<number>).insertAt(idx, (v as unknown) as number);
          else
            // eslint-disable-next-line @typescript-eslint/no-confusing-void-expression
            await (col as unknown as IndexedColumnInterface<string>).insertAt(idx, (v as unknown) as string);
        }
      }
    }

    // 4) Handle columns first seen in this batch, in a single pass preserving row order
    const preExistingKeys = new Set(preExistingEntries.map(([k]) => k));
    const candidateNewKeys = new Set<string>();
    for (const row of rows) {
      for (const k of Object.keys(row)) {
        if (k === this.order.key) continue;
        if (preExistingKeys.has(k)) continue;
        candidateNewKeys.add(k);
      }
    }

    const createdNew = new Map<string, { type: ValueType; col: IndexedColumnInterface<string | number> }>();

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i] as Row;
      const idx = indexes[i] as number;

      // 4a) For already created new columns, insert row value or undefined to maintain alignment
      for (const [k, spec] of createdNew) {
        const isNum = spec.type === "number";
        const has = Object.prototype.hasOwnProperty.call(row, k);
        const raw = has ? row[k] : undefined;
        const v = raw === null ? undefined : (raw as unknown);
        if (typeof (spec.col as any).insertManyAt === "function") {
          // fallback to single insert path if bulk not used in this micro-step
          if (isNum)
            // eslint-disable-next-line @typescript-eslint/no-confusing-void-expression
            await (spec.col as unknown as IndexedColumnInterface<number>).insertAt(idx, (v as unknown) as number);
          else
            // eslint-disable-next-line @typescript-eslint/no-confusing-void-expression
            await (spec.col as unknown as IndexedColumnInterface<string>).insertAt(idx, (v as unknown) as string);
        } else {
          if (isNum)
            // eslint-disable-next-line @typescript-eslint/no-confusing-void-expression
            await (spec.col as unknown as IndexedColumnInterface<number>).insertAt(idx, (v as unknown) as number);
          else
            // eslint-disable-next-line @typescript-eslint/no-confusing-void-expression
            await (spec.col as unknown as IndexedColumnInterface<string>).insertAt(idx, (v as unknown) as string);
        }
      }

      // 4b) Create columns newly seen on this row and insert their values
      for (const k of Object.keys(row)) {
        if (k === this.order.key) continue;
        if (!candidateNewKeys.has(k)) continue;
        if (createdNew.has(k)) continue;
        const raw = row[k];
        if (raw === null || raw === undefined) continue; // defer creation until we see a concrete value
        const t = typeof raw;
        if (t !== "number" && t !== "string") {
          throw new Error(`Unsupported column type for key "${k}": ${t}`);
        }
        const vt = t as ValueType;
        const col = await this.ensureTypedColumn(k, vt);
        const entry = { type: vt, col } as unknown as { type: ValueType; col: IndexedColumnInterface<string | number> };
        createdNew.set(k, entry);
        if (vt === "number")
          // eslint-disable-next-line @typescript-eslint/no-confusing-void-expression
          await (col as unknown as IndexedColumnInterface<number>).insertAt(idx, (raw as unknown) as number);
        else
          // eslint-disable-next-line @typescript-eslint/no-confusing-void-expression
          await (col as unknown as IndexedColumnInterface<string>).insertAt(idx, (raw as unknown) as string);
      }
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
