export interface IStore<T> {
	get(key: string): Promise<T[] | undefined>;
	set(key: string, values: T[]): Promise<void>;
}

export class MemoryStore<T> implements IStore<T> {
	private map = new Map<string, T[]>();

	async get(key: string): Promise<T[] | undefined> {
		return this.map.get(key);
	}

	async set(key: string, values: T[]): Promise<void> {
		// store a copy to avoid aliasing test arrays
		this.map.set(key, values.slice());
	}
}
