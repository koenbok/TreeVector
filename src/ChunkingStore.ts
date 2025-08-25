import type { IStore } from "./Store";

type ChunkRecord = {
  segments: Record<number, unknown>;
};

export class ChunkingStore implements IStore {
  constructor(
    private readonly base: IStore,
    private readonly segmentsPerChunk: number,
    private readonly segmentPrefixes: readonly string[] = ["oseg_", "seg_"],
  ) { }

  private readonly chunkCache = new Map<string, ChunkRecord>();

  async get<T = unknown>(key: string): Promise<T | undefined> {
    const parsed = this.parseSegmentId(key);
    if (!parsed) {
      // Chunk pass-through with caching
      if (this.isChunkKey(key)) {
        const cached = this.chunkCache.get(key);
        if (cached) return cached as unknown as T;
        const loaded = (await this.base.get<ChunkRecord>(key)) ?? { segments: {} };
        this.chunkCache.set(key, loaded);
        return loaded as unknown as T;
      }
      return this.base.get<T>(key);
    }
    const { segNum, segmentPrefix } = parsed;
    const ckey = this.chunkKeyFor(segNum, segmentPrefix);
    let chunk = this.chunkCache.get(ckey);
    if (!chunk) {
      chunk = (await this.base.get<ChunkRecord>(ckey)) ?? { segments: {} };
      this.chunkCache.set(ckey, chunk);
    }
    return chunk.segments[segNum] as T | undefined;
  }

  async set<T = unknown>(key: string, values: T): Promise<void> {
    const parsed = this.parseSegmentId(key);
    if (!parsed) {
      // Write-through for chunk keys and others
      if (this.isChunkKey(key)) {
        const record = (values as unknown as ChunkRecord) ?? { segments: {} };
        this.chunkCache.set(key, record);
        await this.base.set<ChunkRecord>(key, record);
        return;
      }
      await this.base.set<T>(key, values);
      return;
    }
    const { segNum, segmentPrefix } = parsed;
    const ckey = this.chunkKeyFor(segNum, segmentPrefix);
    let chunk = this.chunkCache.get(ckey);
    if (!chunk) {
      chunk = (await this.base.get<ChunkRecord>(ckey)) ?? { segments: {} };
      this.chunkCache.set(ckey, chunk);
    }
    chunk.segments[segNum] = values as unknown;
    await this.base.set<ChunkRecord>(ckey, chunk);
  }

  private parseSegmentId(key: string): { segNum: number; segmentPrefix: string } | undefined {
    for (const prefix of this.segmentPrefixes) {
      if (key.startsWith(prefix)) {
        const n = Number.parseInt(key.slice(prefix.length), 10);
        if (Number.isFinite(n)) return { segNum: n, segmentPrefix: prefix };
      }
    }
    return undefined;
  }

  private chunkKeyFor(segNum: number, segmentPrefix: string): string {
    const chunkIndex = Math.floor(segNum / this.segmentsPerChunk);
    const prefix = this.segmentToChunkPrefix(segmentPrefix);
    return `${prefix}${chunkIndex}`;
  }

  private isChunkKey(key: string): boolean {
    for (const segPrefix of this.segmentPrefixes) {
      const chPrefix = this.segmentToChunkPrefix(segPrefix);
      if (key.startsWith(chPrefix)) return true;
    }
    return false;
  }

  private segmentToChunkPrefix(segmentPrefix: string): string {
    if (segmentPrefix.endsWith("seg_")) return segmentPrefix.slice(0, -4) + "chunk_";
    return `${segmentPrefix}chunk_`;
  }
}
