import type { IStore } from "./Store";
import { FenwickOrderedList } from "./FenwickOrderedList";
import { FenwickList } from "./FenwickList";

export interface OrderedColumnInterface<T> {
  insert(value: T): Promise<number>;
  range(min: number, max: number): Promise<T[]>;
  scan(min: T, max: T): Promise<T[]>;
  get(index: number): Promise<T | undefined>;
  getIndex(value: T): Promise<number>;
  flush(): Promise<string[]>;
}

export interface IndexedColumnInterface<T> {
  insert(index: number, value: T): Promise<void>;
  range(min: number, max: number): Promise<T[]>;
  get(index: number): Promise<T | undefined>;
  flush(): Promise<string[]>;
}

export class FenwickColumn<T> implements IndexedColumnInterface<T> {
  private list: FenwickList<T>;
  constructor(store: IStore, segmentN: number, chunkN: number) {
    this.list = new FenwickList<T>(store, segmentN, chunkN);
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
}

export class FenwickOrderedColumn<T> implements OrderedColumnInterface<T> {
  private list: FenwickOrderedList<T>;
  constructor(store: IStore, segmentN: number, chunkN: number) {
    this.list = new FenwickOrderedList<T>(store, segmentN, chunkN);
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
}
