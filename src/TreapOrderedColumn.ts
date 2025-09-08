import type { OrderedColumnInterface } from "./Column";
import type { IStore } from "./Store";
import type { FenwickBaseMeta, BaseSegment } from "./FenwickBase";

/**
 * TreapOrderedColumn
 *
 * Pragmatic ordered column with:
 * - Leaves as x-MB arrays (bounded by segmentCount).
 * - Per-segment min/max for value routing and scans.
 * - Chunked persistence with copy-on-write (chunkCount segments per chunk).
 *
 * Simplicity-first baseline:
 * - Uses meta.segments for binary searching by segment max.
 * - Computes global indices via simple prefix-sum over segments (O(#segments)).
 * - Maintains in-memory arrays per segment; no fenwick tree.
 *
 * This is a good starting point before introducing a full treap or B+ internal index for faster global prefix sums.
 */

type Segment<T> = BaseSegment<T> & { min: T; max: T };
type OrderedMeta<T> = FenwickBaseMeta<T, Segment<T>>;

function getDefaults<T>(meta: Partial<OrderedMeta<T>>): OrderedMeta<T> {
  return {
    segmentCount: 1024,
    chunkCount: 128,
    segments: [],
    chunks: [],
    ...meta,
  } as OrderedMeta<T>;
}

function defaultCmp<T>(a: T, b: T): number {
  const av = a as unknown as number | string | bigint;
  const bv = b as unknown as number | string | bigint;
  return av < bv ? -1 : av > bv ? 1 : 0;
}

export class TreapOrderedColumn<T> implements OrderedColumnInterface<T> {
  private meta: OrderedMeta<T>;
  private totalCount = 0;

  // Persistence and caching
  private readonly store: IStore;
  private readonly segmentArrays = new Map<Segment<T>, T[]>();
  private readonly segmentIndexByRef = new Map<Segment<T>, number>();
  private readonly chunkCache = new Map<number, T[][]>();
  private readonly dirty = new Set<Segment<T>>();
  private structureChangedSinceFlush = false;

  private readonly cmp: (a: T, b: T) => number;

  constructor(
    store: IStore,
    meta: Partial<OrderedMeta<T>>,
    comparator?: (a: T, b: T) => number,
  ) {
    this.store = store;
    this.meta = getDefaults(meta);
    this.cmp = comparator ?? defaultCmp;
    this.rebuildFromMeta();
  }

  // ---- OrderedColumnInterface ----
  async insert(value: T): Promise<number> {
    if (this.meta.segments.length === 0) {
      // Create first segment
      const seg: Segment<T> = { count: 1, min: value, max: value };
      this.meta.segments.push(seg);
      this.segmentIndexByRef.set(seg, 0);
      const arr: T[] = [value];
      this.segmentArrays.set(seg, arr);
      this.totalCount = 1;
      this.dirty.add(seg);
      this.structureChangedSinceFlush = true;
      return 0;
    }

    // Locate segment: first with seg.max >= value; otherwise last
    const segIndex = this.findFirstSegmentByMaxLowerBound(value);
    const seg = this.meta.segments[segIndex] as Segment<T>;
    const arr = await this.getOrLoadArray(seg);
    // lower_bound inside this segment
    const local = this.lowerBoundInArray(arr, value);
    // insert into arr
    if (local >= arr.length) arr.push(value);
    else arr.splice(local, 0, value);
    seg.count = arr.length;
    // update per-segment min/max
    if (arr.length > 0) {
      seg.min = arr[0] as T;
      seg.max = arr[arr.length - 1] as T;
    } else {
      seg.min = value;
      seg.max = value;
    }
    this.dirty.add(seg);

    // global index = sum of counts of prior segments + local
    const before = this.sumCountsBefore(segIndex);
    const globalIndex = before + local;
    this.totalCount += 1;

    // Split if needed
    if (seg.count > this.meta.segmentCount) {
      await this.splitSegment(segIndex, arr);
    }

    return globalIndex;
  }

  async range(minIndex: number, maxIndex: number): Promise<T[]> {
    const out: T[] = [];
    if (this.totalCount === 0) return out;
    const a = Math.max(0, minIndex | 0);
    let b = maxIndex | 0;
    if (b <= a) return out;
    if (a >= this.totalCount) return out;
    if (b > this.totalCount) b = this.totalCount;

    let remain = b - a;
    // find starting segment and local offset
    let { segIndex, localIndex } = this.findByIndexLinear(a);
    while (remain > 0 && segIndex < this.meta.segments.length) {
      const seg = this.meta.segments[segIndex] as Segment<T>;
      const arr = await this.getOrLoadArray(seg);
      const take = Math.min(remain, Math.max(0, arr.length - localIndex));
      if (take > 0) out.push(...arr.slice(localIndex, localIndex + take));
      remain -= take;
      segIndex += 1;
      localIndex = 0;
    }
    return out;
  }

  async scan(min: T, max: T): Promise<T[]> {
    const out: T[] = [];
    if (this.meta.segments.length === 0) return out;

    // Identify candidate segment range [i, j)
    let i = this.findFirstSegmentByMaxLowerBound(min);
    let j = i;
    while (j < this.meta.segments.length) {
      const s = this.meta.segments[j] as Segment<T>;
      // stop once next segment's min >= max (because [min, max) semantics)
      if (this.cmp(s.min, max) >= 0) break;
      j += 1;
    }

    // Collect results
    while (i < j) {
      const s = this.meta.segments[i] as Segment<T>;
      const arr = await this.getOrLoadArray(s);
      const start = this.lowerBoundInArray(arr, min);
      const end = this.lowerBoundInArray(arr, max); // [min, max)
      if (start < end) out.push(...arr.slice(start, end));
      if (end < arr.length) return out; // ended inside this segment
      i += 1;
    }
    return out;
  }

  async get(index: number): Promise<T | undefined> {
    if (index < 0 || index >= this.totalCount) return undefined;
    const { segIndex, localIndex } = this.findByIndexLinear(index);
    const seg = this.meta.segments[segIndex] as Segment<T>;
    const arr = await this.getOrLoadArray(seg);
    return arr[localIndex];
  }

  async getIndex(value: T): Promise<number> {
    if (this.meta.segments.length === 0) return 0;
    const segIndex = this.findFirstSegmentByMaxLowerBound(value);
    const seg = this.meta.segments[segIndex] as Segment<T>;
    const arr = await this.getOrLoadArray(seg);
    const local = this.lowerBoundInArray(arr, value);
    const before = this.sumCountsBefore(segIndex);
    return before + local;
  }

  async flush(): Promise<string[]> {
    const written: string[] = [];
    const chunkSize = this.effectiveChunkSize();
    if (this.meta.segments.length === 0) return written;

    if (this.structureChangedSinceFlush) {
      // Rewrite all chunks to match current segment order
      const chunksNeeded = Math.ceil(this.meta.segments.length / chunkSize);
      for (let cidx = 0; cidx < chunksNeeded; cidx++) {
        const newChunk: T[][] = new Array<T[]>(chunkSize);
        for (let pos = 0; pos < chunkSize; pos++) {
          const segIdx = cidx * chunkSize + pos;
          if (segIdx >= this.meta.segments.length) {
            newChunk[pos] = [] as T[];
            continue;
          }
          const seg = this.meta.segments[segIdx] as Segment<T>;
          const arr = await this.getOrLoadArray(seg);
          newChunk[pos] = arr.slice() as T[];
        }
        const key = this.generateChunkKey(cidx);
        await this.store.set<T[][]>(key, newChunk);
        this.meta.chunks[cidx] = key;
        this.chunkCache.set(cidx, newChunk);
        written.push(key);
      }
      this.dirty.clear();
      this.structureChangedSinceFlush = false;
      return written;
    }

    if (this.dirty.size === 0) return written;

    // Group dirty segments by chunk
    const changedByChunk = new Map<number, Segment<T>[]>();
    for (const seg of this.dirty) {
      const idx = this.segmentIndexByRef.get(seg);
      if (idx === undefined || idx < 0) continue;
      const cidx = Math.floor(idx / chunkSize);
      const list = changedByChunk.get(cidx);
      if (list) list.push(seg);
      else changedByChunk.set(cidx, [seg]);
    }

    for (const [cidx, segs] of changedByChunk.entries()) {
      const chunk = await this.getOrLoadChunk(cidx);
      const newChunk: T[][] = new Array<T[]>(chunkSize);
      for (let i = 0; i < chunkSize; i++) newChunk[i] = (chunk[i] ?? []) as T[];
      for (const seg of segs) {
        const idx = this.segmentIndexByRef.get(seg);
        if (idx === undefined) continue;
        const pos = idx % chunkSize;
        const arr = await this.getOrLoadArray(seg);
        newChunk[pos] = arr.slice() as T[];
      }
      const key = this.generateChunkKey(cidx);
      await this.store.set<T[][]>(key, newChunk);
      this.meta.chunks[cidx] = key;
      this.chunkCache.set(cidx, newChunk);
      written.push(key);
    }

    this.dirty.clear();
    return written;
  }

  getMeta(): OrderedMeta<T> {
    return this.meta;
  }

  setMeta(meta: OrderedMeta<T>): void {
    this.meta = getDefaults(meta);
    this.rebuildFromMeta();
  }

  // ---- helpers ----

  private rebuildFromMeta(): void {
    this.segmentArrays.clear();
    this.segmentIndexByRef.clear();
    this.chunkCache.clear();
    this.dirty.clear();
    this.structureChangedSinceFlush = false;

    // Rebuild index and counts from provided segments
    for (let i = 0; i < this.meta.segments.length; i++) {
      const s = this.meta.segments[i] as Segment<T>;
      this.segmentIndexByRef.set(s, i);
    }
    this.totalCount = this.meta.segments.reduce(
      (sum, s) => sum + ((s?.count ?? 0) | 0),
      0,
    );
  }

  private effectiveChunkSize(): number {
    return this.meta.chunkCount > 0 ? this.meta.chunkCount : 1;
  }

  private async getOrLoadChunk(cidx: number): Promise<T[][]> {
    const cached = this.chunkCache.get(cidx);
    if (cached) return cached as T[][];
    const chunkSize = this.effectiveChunkSize();
    const key = this.meta.chunks[cidx];
    let chunk = (key ? await this.store.get<T[][]>(key) : undefined) ?? [];
    if (!Array.isArray(chunk)) chunk = [];
    if (chunk.length < chunkSize) {
      const augmented = new Array<T[]>(chunkSize);
      for (let i = 0; i < chunkSize; i++)
        augmented[i] = (chunk[i] ?? []) as T[];
      chunk = augmented;
    }
    this.chunkCache.set(cidx, chunk as T[][]);
    return chunk as T[][];
  }

  private generateChunkKey(cidx: number): string {
    const suffix = `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
    return `tord_chunk_${cidx}_${suffix}`;
  }

  private async getOrLoadArray(seg: Segment<T>): Promise<T[]> {
    const existing = this.segmentArrays.get(seg);
    if (existing) return existing as T[];
    const idx = this.segmentIndexByRef.get(seg);
    if (idx === undefined || idx < 0) {
      const arr: T[] = new Array<T>(seg.count | 0);
      this.segmentArrays.set(seg, arr);
      return arr;
    }
    const cidx = Math.floor(idx / this.effectiveChunkSize());
    const pos = idx % this.effectiveChunkSize();
    const chunk = await this.getOrLoadChunk(cidx);
    const arr = (chunk[pos] ?? []) as T[];
    const copy = arr.slice() as T[];
    this.segmentArrays.set(seg, copy);
    // Sync seg metadata from content
    seg.count = copy.length;
    if (copy.length > 0) {
      seg.min = copy[0] as T;
      seg.max = copy[copy.length - 1] as T;
    }
    return copy;
  }

  private lowerBoundInArray(arr: T[], value: T): number {
    let lo = 0;
    let hi = arr.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      const c = this.cmp(arr[mid] as T, value);
      if (c < 0) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  private findFirstSegmentByMaxLowerBound(value: T): number {
    let lo = 0;
    let hi = this.meta.segments.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      const s = this.meta.segments[mid] as Segment<T>;
      if (this.cmp(s.max, value) >= 0) hi = mid;
      else lo = mid + 1;
    }
    return Math.min(lo, this.meta.segments.length - 1);
  }

  private sumCountsBefore(segIndex: number): number {
    let sum = 0;
    for (let i = 0; i < segIndex; i++) sum += this.meta.segments[i]?.count ?? 0;
    return sum;
  }

  private findByIndexLinear(index: number): {
    segIndex: number;
    localIndex: number;
  } {
    let i = 0;
    let acc = 0;
    while (i < this.meta.segments.length) {
      const cnt = (this.meta.segments[i]?.count ?? 0) | 0;
      if (index < acc + cnt) return { segIndex: i, localIndex: index - acc };
      acc += cnt;
      i += 1;
    }
    // Fallback: clamp to last
    const last = Math.max(0, this.meta.segments.length - 1);
    const lastCount = (this.meta.segments[last]?.count ?? 0) | 0;
    return {
      segIndex: last,
      localIndex: Math.max(
        0,
        Math.min(index - (acc - lastCount), lastCount - 1),
      ),
    };
  }

  private async splitSegment(segIndex: number, arr: T[]): Promise<void> {
    const leftLen = arr.length >>> 1;
    const right = arr.slice(leftLen) as T[];
    const left = arr.slice(0, leftLen) as T[];
    if (left.length === 0 || right.length === 0) return;

    const seg = this.meta.segments[segIndex] as Segment<T>;
    // Update left in place
    this.segmentArrays.set(seg, left);
    seg.count = left.length;
    seg.min = left[0] as T;
    seg.max = left[left.length - 1] as T;
    this.dirty.add(seg);

    // Create right
    const newSeg: Segment<T> = {
      count: right.length,
      min: right[0] as T,
      max: right[right.length - 1] as T,
    };
    this.meta.segments.splice(segIndex + 1, 0, newSeg);
    this.segmentIndexByRef.clear();
    for (let i = 0; i < this.meta.segments.length; i++) {
      this.segmentIndexByRef.set(this.meta.segments[i] as Segment<T>, i);
    }
    this.segmentArrays.set(newSeg, right);
    this.dirty.add(newSeg);
    this.structureChangedSinceFlush = true;
  }
}

export default TreapOrderedColumn;
