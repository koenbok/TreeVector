import { BPlusTree } from "./BPlusTree";
import type { IStore } from "./Store";

export interface SortedColumn<T> {
	insert(value: T): Promise<number>;
	range(min: number, max: number): Promise<T[]>;
	scan(min: T, max: T): Promise<T[]>;
	get(index: number): Promise<T | undefined>;
	getIndex(value: T): Promise<number>;
	flush(): Promise<string[]>;
}

export interface IndexedColumn<T> {
	insert(index: number, value: T): Promise<void>;
	range(min: number, max: number): Promise<T[]>;
	get(index: number): Promise<T | undefined>;
	flush(): Promise<string[]>;
}

export class BasicSortedColumn<T> implements SortedColumn<T> {
	values: T[] = [];

	async insert(value: T): Promise<number> {
		const index = binarySearch(this.values, value);
		this.values.splice(index, 0, value);
		return index;
	}

	async range(min: number, max: number): Promise<T[]> {
		return this.values.slice(min, max);
	}

	async scan(min: T, max: T): Promise<T[]> {
		const a = binarySearch(this.values, min);
		const b = binarySearch(this.values, max);
		return this.values.slice(a, b);
	}

	async get(index: number): Promise<T | undefined> {
		return this.values[index];
	}

	async getIndex(value: T): Promise<number> {
		return binarySearch(this.values, value);
	}

	async flush(): Promise<string[]> {
		return [];
	}
}

export class BPlusTreeSortedColumn<T> implements SortedColumn<T> {
	tree: BPlusTree<T>;

	constructor(store: IStore<T>, maxChildren: number, maxKeys: number) {
		this.tree = new BPlusTree<T>(store, maxChildren, maxKeys);
	}

	async insert(value: T): Promise<number> {
		return this.tree.insert(value);
	}

	async range(min: number, max: number): Promise<T[]> {
		return this.tree.range(min, max);
	}

	async scan(min: T, max: T): Promise<T[]> {
		return this.tree.scan(min, max);
	}

	async get(index: number): Promise<T | undefined> {
		return this.tree.get(index);
	}

	async getIndex(value: T): Promise<number> {
		return this.tree.getIndex(value);
	}

	async flush(): Promise<string[]> {
		return this.tree.flush();
	}
}

export class BasicIndexedColumn<T> implements IndexedColumn<T> {
	values: T[] = [];

	async insert(index: number, value: T): Promise<void> {
		this.values.splice(index, 0, value);
	}

	async range(min: number, max: number): Promise<T[]> {
		return this.values.slice(min, max);
	}

	async get(index: number): Promise<T | undefined> {
		return this.values[index];
	}

	async flush(): Promise<string[]> {
		return [];
	}
}

function binarySearch<T>(values: T[], value: T): number {
	// lower_bound: first index i where values[i] >= value
	let low = 0;
	let high = values.length;
	while (low < high) {
		const mid = (low + high) >>> 1;
		const midVal = values[mid] as T;
		if (midVal < value) low = mid + 1;
		else high = mid;
	}
	return low;
}
