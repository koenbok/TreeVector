import type { IndexedColumnInterface, OrderedColumnInterface } from "./Column";
import type { IStore } from "./Store";

type Row = Record<string, unknown>;

export class Table<T> {
  constructor(
    private store: IStore<T>,
    private order: { key: string; column: OrderedColumnInterface<T> },
    private columns: Record<string, IndexedColumnInterface<T>>,
  ) {}

  async insert(rows: Row[]): Promise<void> {
    for (const row of rows) {
      const value = row[this.order.key];
      if (value === undefined) {
        throw new Error(`Row is missing key ${this.order.key}`);
      }
      const index = await this.order.column.insert(value as T);

      for (const rowKey in row) {
        if (rowKey === this.order.key) {
          continue;
        }
        const otherColumn = this.columns[rowKey];
        if (otherColumn) {
          await otherColumn.insert(index, row[rowKey] as T);
        } else {
          throw new Error(`Column ${rowKey} not found`);
        }
      }
    }
  }

  async get(index: number): Promise<Row> {
    const row: Row = {};
    for (const [key, column] of Object.entries(this.columns)) {
      row[key] = await column.get(index);
    }
    return row;
  }

  async range(limit?: number, offset?: number): Promise<Row[]> {
    const a = offset ?? 0;
    const b = a + (limit ?? 0);

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
    await Promise.all(
      Object.values(this.columns).map((column) => column.flush()),
    );
  }
}
