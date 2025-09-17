import { FenwickOrderedList } from "./FenwickOrderedList";
import { FenwickList } from "./FenwickList";
import type { FenwickBaseMeta, BaseSegment } from "./FenwickBase";

export interface IndexedColumnInterface<T> {
  insertAt(index: number, value: T): Promise<void>;
  insertManyAt(indexes: number[], values: Array<T | undefined>): Promise<void>;
  range(min: number, max: number): Promise<T[]>;
  get(index: number): Promise<T | undefined>;
  length(): number;
  flush(): Promise<string[]>;
  getMeta(): FenwickBaseMeta<T, BaseSegment<T>>;
  setMeta(meta: FenwickBaseMeta<T, BaseSegment<T>>): void;
}

export interface OrderedColumnInterface<T> {
  insert(value: T): Promise<number>;
  range(min: number, max: number): Promise<T[]>;
  scan(min: T, max: T): Promise<T[]>;
  get(index: number): Promise<T | undefined>;
  getIndex(value: T): Promise<number>;
  length(): number;
  flush(): Promise<string[]>;
  getMeta(): FenwickBaseMeta<T, BaseSegment<T> & { min: T; max: T }>;
  setMeta(meta: FenwickBaseMeta<T, BaseSegment<T> & { min: T; max: T }>): void;
}

export const IndexedColumn = FenwickList;
export const OrderedColumn = FenwickOrderedList;
