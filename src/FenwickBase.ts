import type { IStore } from "./Store";

export type BaseSegment<T> = {
  count: number;
};

// Removed unused utility types to reduce clutter

export type FenwickBaseMeta<T, S extends BaseSegment<T>> = {
  // Maximum number of values per in-memory segment array
  segmentCount: number;
  // Ordered list of segment descriptors (no ids; indexed by position)
  segments: S[];
  // Number of segments per chunk (<=0 means 1 segment per chunk)
  chunkCount: number;
  // Copy-on-write chunk keys by chunk index
  chunks: string[];
};

export abstract class FenwickBase<T, S extends BaseSegment<T>> {
  protected meta!: FenwickBaseMeta<T, S>;
  protected fenwick: number[] = [];
  protected totalCount = 0;
  protected dirty = new Set<S>();
  // Single cache: chunks by chunk index → T[][] (for persisted loads)
  protected chunkCache = new Map<number, T[][]>();
  // In-memory authoritative arrays per segment (simple and fast)
  protected segmentArrays = new Map<S, T[]>();
  // O(1) lookup for segment index by object identity
  protected segmentIndexByRef = new Map<S, number>();
  // Back-compat shim for tests that call this.segmentCache.clear()
  public segmentCache: { clear: () => void } = {
    clear: () => {
      this.segmentArrays.clear();
      this.chunkCache.clear();
    },
  };

  protected constructor(
    protected readonly store: IStore,
    meta: FenwickBaseMeta<T, S>,
  ) {
    this.setMeta(meta);
  }

  setMeta(meta: FenwickBaseMeta<T, S>): void {
    this.meta = meta;
    if (!Array.isArray(this.meta.chunks)) this.meta.chunks = [];
    // Recompute derived state from provided segments
    this.totalCount = Array.isArray(this.meta.segments)
      ? this.meta.segments.reduce((sum, seg) => sum + (seg?.count ?? 0), 0)
      : 0;
    this.buildFenwick();
    this.rebuildSegmentIndexMap();
  }

  getMeta(): FenwickBaseMeta<T, S> {
    return this.meta;
  }

  // ---- public ops shared ----
  async get(index: number): Promise<T | undefined> {
    if (index < 0 || index >= this.totalCount) return undefined;
    const { segIndex, localIndex } = this.findByIndex(index);
    const seg = this.meta.segments[segIndex] as S;
    await this.ensureSegmentLoaded(seg);
    const arr = this.getOrCreateArraySync(seg, true);
    return arr[localIndex] as T;
  }

  async range(minIndex: number, maxIndex: number): Promise<T[]> {
    const out: T[] = [];
    if (this.totalCount === 0) return out;
    const a = Math.max(0, minIndex);
    let b = maxIndex;
    if (b <= a) return out;
    if (a >= this.totalCount) return out;
    if (b > this.totalCount) b = this.totalCount;
    let { segIndex, localIndex } = this.findByIndex(a);
    let remaining = b - a;
    // Load all required segments in parallel before slicing
    const { segIndex: endSegIndex } = this.findByIndex(b - 1);
    await Promise.all(
      this.meta.segments
        .slice(segIndex, endSegIndex + 1)
        .map((s) => this.ensureSegmentLoaded(s as S)),
    );
    while (remaining > 0 && segIndex < this.meta.segments.length) {
      const seg = this.meta.segments[segIndex] as S;
      const arr = this.getOrCreateArraySync(seg, true);
      const take = Math.min(remaining, Math.max(0, arr.length - localIndex));
      if (take > 0) out.push(...arr.slice(localIndex, localIndex + take));
      remaining -= take;
      segIndex += 1;
      localIndex = 0;
    }
    return out;
  }

  async flush(): Promise<string[]> {
    if (this.dirty.size === 0) return [];
    const changedByChunk = new Map<
      number,
      Array<{ seg: S; segIndex: number }>
    >();
    const chunkSize = this.effectiveChunkSize();
    for (const seg of this.dirty) {
      const idx = this.getSegmentIndex(seg);
      if (idx < 0) continue;
      const cidx = Math.floor(idx / chunkSize);
      let arr = changedByChunk.get(cidx);
      if (!arr) {
        arr = [];
        changedByChunk.set(cidx, arr);
      }
      arr.push({ seg, segIndex: idx });
    }

    const writtenKeys: string[] = [];
    await Promise.all(
      Array.from(changedByChunk.entries()).map(async ([cidx, items]) => {
        const chunk = await this.getOrLoadChunk(cidx);
        const newChunk: T[][] = new Array<T[]>(chunkSize);
        for (let i = 0; i < chunkSize; i++)
          newChunk[i] = (chunk[i] ?? []) as T[];
        // Override dirty segments with current in-memory arrays
        for (const { seg, segIndex } of items) {
          const pos = segIndex % chunkSize;
          const arr = this.getOrCreateArraySync(seg, true);
          newChunk[pos] = arr.slice() as unknown as T[];
        }
        const newKey = this.generateChunkKey(cidx);
        await this.store.set<T[][]>(newKey, newChunk);
        this.meta.chunks[cidx] = newKey;
        // Update cache to reflect the new persisted chunk
        this.chunkCache.set(cidx, newChunk);
        writtenKeys.push(newKey);
      }),
    );

    this.dirty.clear();
    return writtenKeys;
  }

  // ---- internals shared ----

  /**
   * Hook for subclasses to update metadata on an existing segment after modification.
   * Base implementation is a no-op.
   */
  protected updateSegmentMetadata(segment: S, data: T[]): void {
    // no-op by default
  }

  /**
   * Hook for subclasses to create a new segment object with specific metadata.
   * Base implementation only sets count.
   */
  protected createNewSegmentObject(count: number, data: T[]): S {
    return { count } as S;
  }
  protected async ensureSegmentLoaded(segment: S): Promise<void> {
    const idx = this.getSegmentIndex(segment);
    if (idx < 0) {
      segment.count = 0;
      return;
    }
    const arr = await this.getOrCreateArrayForSegment(segment, true);
    segment.count = arr.length;
  }

  protected async splitSegment(index: number): Promise<void> {
    const seg = this.meta.segments[index] as S;
    const arr = this.getOrCreateArraySync(seg, true);
    const mid = arr.length >>> 1;
    // Avoid double-copy: mutate original array to keep left half, splice to obtain right
    const right = arr.splice(mid);
    const left = arr; // arr now holds the left half
    if (left.length === 0 || right.length === 0) return;

    // 1) Update existing segment count and delegate metadata to hook
    seg.count = left.length;
    this.updateSegmentMetadata(seg, left);

    // 2) Create new segment via factory hook
    const newSeg = this.createNewSegmentObject(right.length, right);

    // 3) Place right half into the next segment slot and seed its array
    this.meta.segments.splice(index + 1, 0, newSeg);
    void this.getOrCreateArraySync(newSeg, true, right);

    // 4) Recompute fenwick and segment index map
    this.buildFenwick();
    this.rebuildSegmentIndexMap();

    // 5) Mark segments as dirty for persistence
    this.dirty.add(seg);
    this.dirty.add(newSeg);
  }

  protected findByIndex(index: number): {
    segIndex: number;
    localIndex: number;
  } {
    let idx = 0;
    let bit = 1;
    while (bit << 1 <= this.fenwick.length) bit <<= 1;
    let sum = 0;
    for (let step = bit; step > 0; step >>= 1) {
      const next = idx + step;
      if (
        next <= this.fenwick.length &&
        sum + (this.fenwick[next - 1] ?? 0) <= index
      ) {
        sum += this.fenwick[next - 1] as number;
        idx = next;
      }
    }
    const segIndex = Math.min(idx, this.meta.segments.length - 1);
    const local = index - sum;
    return { segIndex, localIndex: local };
  }

  protected prefixSum(endExclusive: number): number {
    if (endExclusive <= 0 || this.fenwick.length === 0) return 0;
    let sum = 0;
    let i = endExclusive;
    while (i > 0) {
      sum += this.fenwick[i - 1] as number;
      i -= i & -i;
    }
    return sum;
  }

  protected addFenwick(index: number, delta: number): void {
    if (index < 0 || this.fenwick.length === 0) return;
    let i = index + 1;
    while (i <= this.fenwick.length) {
      this.fenwick[i - 1] = (this.fenwick[i - 1] ?? 0) + delta;
      i += i & -i;
    }
  }

  protected rebuildFenwick(): void {
    this.buildFenwick();
    this.rebuildSegmentIndexMap();
  }

  // Centralized helper to create the very first segment with an initial value
  protected createInitialSegment(segment: S, initialValue: T): void {
    const arr = this.getOrCreateArraySync(segment, true);
    arr.push(initialValue);
    segment.count = 1;
    this.meta.segments.push(segment);
    this.rebuildFenwick();
    this.totalCount = 1;
    this.dirty.add(segment);
  }

  // ---- chunk helpers ----
  private effectiveChunkSize(): number {
    return this.meta.chunkCount > 0 ? this.meta.chunkCount : 1;
  }

  private getSegmentIndex(segment: S): number {
    const idx = this.segmentIndexByRef.get(segment);
    if (idx !== undefined) return idx as number;
    // Fallback (should be rare)
    const found = this.meta.segments.indexOf(segment);
    if (found >= 0) this.segmentIndexByRef.set(segment, found);
    return found;
  }

  protected getOrCreateArraySync(
    segment: S,
    create = false,
    preset?: T[],
  ): T[] {
    const existing = this.segmentArrays.get(segment);
    if (existing) return existing as T[];
    if (create) {
      const arr = (preset ?? []) as T[];
      this.segmentArrays.set(segment, arr);
      return arr;
    }
    // If not in memory, create empty array (caller may load asynchronously)
    const arr = (preset ?? []) as T[];
    this.segmentArrays.set(segment, arr);
    return arr;
  }

  private async getOrCreateArrayForSegment(
    segment: S,
    create = false,
  ): Promise<T[]> {
    const current = this.segmentArrays.get(segment);
    if (current) return current as T[];
    const idx = this.getSegmentIndex(segment);
    const chunkSize = this.effectiveChunkSize();
    const cidx = Math.floor(idx / chunkSize);
    const chunk = await this.getOrLoadChunk(cidx);
    const pos = idx % chunkSize;
    const arr = (chunk[pos] ?? []) as T[];
    // Store a working copy so mutations don't alias the chunk cache
    const copy = arr.slice() as unknown as T[];
    if (create || copy.length > 0) this.segmentArrays.set(segment, copy);
    return copy;
  }

  private async getOrLoadChunk(chunkIndex: number): Promise<T[][]> {
    const cached = this.chunkCache.get(chunkIndex);
    if (cached) return cached as T[][];
    const chunkSize = this.effectiveChunkSize();
    const key = this.meta.chunks[chunkIndex];
    let chunk = (key ? await this.store.get<T[][]>(key) : undefined) ?? [];
    if (!Array.isArray(chunk)) chunk = [];
    if (chunk.length < chunkSize) {
      const augmented = new Array<T[]>(chunkSize);
      for (let i = 0; i < chunkSize; i++)
        augmented[i] = (chunk[i] ?? []) as T[];
      chunk = augmented;
    }
    this.chunkCache.set(chunkIndex, chunk as T[][]);
    return chunk as T[][];
  }

  private generateChunkKey(chunkIndex: number): string {
    const suffix = `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
    return `chunk_${chunkIndex}_${suffix}`;
  }

  private rebuildSegmentIndexMap(): void {
    this.segmentIndexByRef.clear();
    for (let i = 0; i < this.meta.segments.length; i++) {
      this.segmentIndexByRef.set(this.meta.segments[i] as S, i);
    }
  }

  // Shared implementation for building the fenwick tree from current segments
  private buildFenwick(): void {
    const n = this.meta.segments.length;
    this.fenwick = new Array<number>(n).fill(0);
    for (let i = 0; i < n; i++)
      this.fenwick[i] = this.meta.segments[i]?.count ?? 0;
    for (let i = 0; i < n; i++) {
      const j = i + ((i + 1) & -(i + 1));
      if (j <= n - 1)
        this.fenwick[j] = (this.fenwick[j] ?? 0) + (this.fenwick[i] ?? 0);
    }
  }
}
