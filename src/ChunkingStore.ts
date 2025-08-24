import type { IStore } from "./Store";

type ChunkRecord = {
	segments: Record<number, unknown>;
};

export class ChunkingStore implements IStore {
	constructor(
		private readonly base: IStore,
		private readonly segmentsPerChunk: number,
		private readonly segmentPrefixes: readonly string[] = ["oseg_", "seg_"],
	) {}

	async get<T = unknown>(key: string): Promise<T | undefined> {
		const segId = this.parseSegmentId(key);
		if (segId === undefined) return this.base.get<T>(key);
		const chunkKey = this.chunkKeyFor(segId);
		const chunk = (await this.base.get<ChunkRecord>(chunkKey)) ?? { segments: {} };
		return chunk.segments[segId] as T | undefined;
	}

	async set<T = unknown>(key: string, values: T): Promise<void> {
		const segId = this.parseSegmentId(key);
		if (segId === undefined) {
			await this.base.set<T>(key, values);
			return;
		}
		const chunkKey = this.chunkKeyFor(segId);
		const chunk = (await this.base.get<ChunkRecord>(chunkKey)) ?? { segments: {} };
		chunk.segments[segId] = values as unknown;
		await this.base.set<ChunkRecord>(chunkKey, chunk);
	}

	private parseSegmentId(key: string): number | undefined {
		for (const prefix of this.segmentPrefixes) {
			if (key.startsWith(prefix)) {
				const n = Number.parseInt(key.slice(prefix.length), 10);
				if (Number.isFinite(n)) return n;
			}
		}
		return undefined;
	}

	private chunkKeyFor(segId: number): string {
		const chunkIndex = Math.floor(segId / this.segmentsPerChunk);
		return `chunk_${chunkIndex}`;
	}
}


