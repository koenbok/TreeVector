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
    if (arr) return arr;
  }
  const flat = (await store.get<T[]>(segId)) ?? [];
  return flat;
}

export async function flushSegmentsToChunks<T>(
  store: IStore,
  dirty: Array<{ id: string }>,
  segmentCache: Map<string, T[]>,
  segmentsPerChunk?: number,
  chunkPrefix = "chunk_",
): Promise<string[]> {
  if (!segmentsPerChunk || segmentsPerChunk <= 0) {
    for (const seg of dirty)
      await store.set<T[]>(seg.id, segmentCache.get(seg.id) ?? []);
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
    rec[segNum] = segmentCache.get(seg.id) ?? [];
  }
  const entries = Array.from(chunkMap.entries()).map(([cidx, dirtySegments]) => ({
    cidx,
    key: chunkKey(chunkPrefix, cidx),
    dirtySegments,
  }));

  const existingChunks = await Promise.all(
    entries.map((e) => store.get<{ segments?: Record<number, T[]> }>(e.key)),
  );

  await Promise.all(
    entries.map(async (e, i) => {
      const existing = existingChunks[i] ?? {};
      const merged: { segments: Record<number, T[]> } = {
        segments: { ...(existing.segments ?? {}) },
      };
      Object.assign(merged.segments, e.dirtySegments);
      await store.set(e.key, merged as { segments: Record<number, T[]> });
    }),
  );

  return entries.map((e) => e.key);
}
