export interface IStore {
	get<T = unknown>(key: string): Promise<T | undefined>;
	set<T = unknown>(key: string, values: T): Promise<void>;
}

export class MemoryStore implements IStore {
	private map = new Map<string, unknown>();

	async get<T = unknown>(key: string): Promise<T | undefined> {
		const value = this.map.get(key);
		if (value === undefined) {
			return undefined;
		}
		return structuredClone(value) as T;
	}

	async set<T = unknown>(key: string, value: T): Promise<void> {
		this.map.set(key, structuredClone(value));
	}
}
