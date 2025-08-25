import type { IStore } from "./Store";

export function segmentNumericId(id: string): number {
  const idx = id.lastIndexOf("_");
  if (idx >= 0) {
    const n = Number.parseInt(id.slice(idx + 1), 10);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

export function chunkKey(prefix: string, chunkIndex: number): string {
  return `${prefix}${chunkIndex}`;
}

export type ChunkCache<T> = {
  idx?: number;
  segs?: Record<number, T[]>;
};

export async function loadSegmentFromChunks<T>(
  store: IStore,
  segId: string,
  segmentsPerChunk?: number,
  chunkPrefix = "chunk_",
  cache?: ChunkCache<T>,
): Promise<T[]> {
  if (segmentsPerChunk && segmentsPerChunk > 0) {
    const segNum = segmentNumericId(segId);
    const cidx = Math.floor(segNum / segmentsPerChunk);
    let segments: Record<number, T[]> | undefined;
    if (cache && cache.idx === cidx && cache.segs) {
      segments = cache.segs;
    } else {
      const chunk =
        (await store.get<{ segments?: Record<number, T[]> }>(
          chunkKey(chunkPrefix, cidx),
        )) ?? {};
      segments = chunk.segments;
      if (cache) {
        cache.idx = cidx;
        cache.segs = segments ?? {};
      }
    }
    const arr = segments?.[segNum];
    if (arr) return arr.slice();
  }
  const flat = (await store.get<T[]>(segId)) ?? [];
  return flat.slice();
}

export async function flushSegmentsToChunks<T>(
  store: IStore,
  dirty: Array<{ id: string; values?: T[] }>,
  segmentsPerChunk?: number,
  chunkPrefix = "chunk_",
): Promise<string[]> {
  if (!segmentsPerChunk || segmentsPerChunk <= 0) {
    for (const seg of dirty)
      await store.set<T[]>(seg.id, (seg.values ?? []) as T[]);
    return dirty.map((s) => s.id);
  }
  const chunkMap = new Map<number, Record<number, T[]>>();
  for (const seg of dirty) {
    const segNum = segmentNumericId(seg.id);
    const cidx = Math.floor(segNum / segmentsPerChunk);
    let rec = chunkMap.get(cidx);
    if (!rec) {
      rec = {} as Record<number, T[]>;
      chunkMap.set(cidx, rec);
    }
    rec[segNum] = (seg.values ?? []) as T[];
  }
  const writtenKeys: string[] = [];
  for (const [cidx, segments] of chunkMap) {
    const key = chunkKey(chunkPrefix, cidx);
    await store.set(key, { segments } as { segments: Record<number, T[]> });
    writtenKeys.push(key);
  }
  return writtenKeys;
}
