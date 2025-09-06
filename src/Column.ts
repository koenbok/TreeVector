import type { IStore } from "./Store";
import { FenwickOrderedList } from "./FenwickOrderedList";
import { FenwickList } from "./FenwickList";
import type { FenwickBaseMeta, MakeOptional, BaseSegment } from "./FenwickBase";

export interface OrderedColumnInterface<T> {
  insert(value: T): Promise<number>;
  range(min: number, max: number): Promise<T[]>;
  scan(min: T, max: T): Promise<T[]>;
  get(index: number): Promise<T | undefined>;
  getIndex(value: T): Promise<number>;
  flush(): Promise<string[]>;
  getMeta(): Promise<Record<string, unknown>>;
  setMeta(meta: Record<string, unknown>): Promise<void>;
}

export interface IndexedColumnInterface<T> {
  insert(index: number, value: T): Promise<void>;
  range(min: number, max: number): Promise<T[]>;
  get(index: number): Promise<T | undefined>;
  flush(): Promise<string[]>;
  getMeta(): Promise<Record<string, unknown>>;
  setMeta(meta: Record<string, unknown>): Promise<void>;
}

export class FenwickColumn<T> implements IndexedColumnInterface<T> {
  private list: FenwickList<T>;
  constructor(store: IStore, meta: MakeOptional<FenwickBaseMeta<T, BaseSegment<T>>, "segments">) {
    this.list = new FenwickList<T>(store, meta);
  }
  async insert(index: number, value: T): Promise<void> {
    await this.list.insertAt(index, value);
  }
  async range(min: number, max: number): Promise<T[]> {
    return this.list.range(min, max);
  }
  async get(index: number): Promise<T | undefined> {
    return this.list.get(index);
  }
  async flush(): Promise<string[]> {
    return this.list.flush();
  }
  async getMeta(): Promise<Record<string, unknown>> {
    return this.list.getMeta();
  }
  async setMeta(meta: Record<string, unknown>): Promise<void> {
    await this.list.setMeta(meta as any);
  }
}

export class FenwickOrderedColumn<T> implements OrderedColumnInterface<T> {
  private list: FenwickOrderedList<T>;
  constructor(store: IStore, meta: MakeOptional<FenwickBaseMeta<T, BaseSegment<T> & { min: T; max: T }>, "segments">) {
    this.list = new FenwickOrderedList<T>(store, meta as any);
  }
  async insert(value: T): Promise<number> {
    return this.list.insert(value);
  }
  async range(min: number, max: number): Promise<T[]> {
    return this.list.range(min, max);
  }
  async scan(min: T, max: T): Promise<T[]> {
    // [min, max) semantics are natively implemented in FenwickOrderedList.scan
    return this.list.scan(min, max);
  }
  async get(index: number): Promise<T | undefined> {
    return this.list.get(index);
  }
  async getIndex(value: T): Promise<number> {
    return this.list.getIndex(value);
  }
  async flush(): Promise<string[]> {
    return this.list.flush();
  }

  async getMeta(): Promise<Record<string, unknown>> {
    return this.list.getMeta();
  }
  async setMeta(meta: Record<string, unknown>): Promise<void> {
    await this.list.setMeta(meta as any);
  }
}
