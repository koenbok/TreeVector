import type { IStore } from "./Store";
import { FenwickBase, type BaseSegment, type FenwickBaseMeta, type MakeOptional } from "./FenwickBase";

type Segment<T> = BaseSegment<T> & { min: T; max: T };

function defaultCmp<T>(a: T, b: T): number {
  const aVal = a as unknown as number | string | bigint;
  const bVal = b as unknown as number | string | bigint;
  return aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
}

type FenwickOrderedListMeta<T> = FenwickBaseMeta<T, Segment<T>>;

function getDefaults<T>(meta: Partial<FenwickOrderedListMeta<T>>): FenwickOrderedListMeta<T> {
  return {
    segmentCount: 1024,
    segmentPrefix: "segment_ordered_",
    chunkCount: 128,
    chunkPrefix: "chunk_",
    segments: [],
    ...meta,
  };
}

export class FenwickOrderedList<T> extends FenwickBase<T, Segment<T>> {
  private cmp: (a: T, b: T) => number;

  constructor(
    store: IStore,
    meta: Partial<FenwickOrderedListMeta<T>>,

    cmp?: (a: T, b: T) => number,
  ) {

    super(store, getDefaults<T>(meta));

    // Initialize cmp
    this.cmp = cmp || defaultCmp;
  }

  async insert(value: T): Promise<number> {
    if (this.meta.segments.length === 0) {
      const seg: Segment<T> = {
        id: this.newId(),
        count: 1,
        min: value,
        max: value,
      };
      this.segmentCache.set(seg.id, [value]);
      this.meta.segments.push(seg);
      this.rebuildFenwick();
      this.totalCount = 1;
      this.dirty.add(seg);
      return 0;
    }

    // locate segment: first with seg.max >= value
    const segIndex = this.findFirstSegmentByMaxLowerBound(value);
    const seg = this.meta.segments[segIndex] as Segment<T>;
    await this.ensureLoaded(seg);
    const arr = this.segmentCache.get(seg.id)!;

    // lower_bound inside segment
    const localIndex = this.lowerBoundInArray(arr, value);
    const insertPos = this.prefixSum(segIndex) + localIndex;

    if (localIndex === arr.length) arr.push(value);
    else arr.splice(localIndex, 0, value);
    seg.count += 1;
    this.totalCount += 1;
    // Conditionally update segment metadata only when necessary
    if (this.cmp(value, seg.min) < 0) seg.min = value;
    else if (this.cmp(value, seg.max) > 0) seg.max = value;
    this.addFenwick(segIndex, 1);
    this.dirty.add(seg);

    if (seg.count > this.meta.segmentCount) this.splitSegment(segIndex);
    return insertPos;
  }

  override async get(index: number): Promise<T | undefined> {
    return super.get(index);
  }

  override async range(minIndex: number, maxIndex: number): Promise<T[]> {
    return super.range(minIndex, maxIndex);
  }

  /**
   * Returns all values v such that min <= v < max, in sorted order.
   * Semantics are [min, max), i.e. max is exclusive. This matches common
   * library conventions (e.g. C++ lower_bound/upper_bound style ranges).
   */
  async scan(min: T, max: T): Promise<T[]> {
    const out: T[] = [];
    if (this.meta.segments.length === 0) return out;
    // find first segment that could contain min
    let i = this.findFirstSegmentByMaxLowerBound(min);
    // determine candidate range [i, j)
    let j = i;
    while (j < this.meta.segments.length) {
      const s = this.meta.segments[j] as Segment<T>;
      // [min, max) semantics: stop once the next segment's min >= max
      if (this.cmp(s.min, max) >= 0) break;
      j += 1;
    }
    // load candidates in parallel
    await Promise.all(
      this.meta.segments.slice(i, j).map((s) => this.ensureLoaded(s as Segment<T>)),
    );
    // now collect results sequentially
    while (i < j) {
      const s = this.meta.segments[i] as Segment<T>;
      const arr = this.segmentCache.get(s.id)!;
      // [min, max) semantics: lower_bound(min), lower_bound(max)
      const start = this.lowerBoundInArray(arr, min);
      const end = this.lowerBoundInArray(arr, max);
      if (start < end) out.push(...arr.slice(start, end));
      if (end < arr.length) return out; // ended inside this segment
      i += 1;
    }
    return out;
  }

  async getIndex(value: T): Promise<number> {
    if (this.meta.segments.length === 0) return 0;
    // first segment with max >= value
    const segIndex = this.findFirstSegmentByMaxLowerBound(value);
    const s = this.meta.segments[segIndex] as Segment<T>;
    await this.ensureLoaded(s);
    const arr = this.segmentCache.get(s.id)!;
    // lower_bound in arr
    const local = this.lowerBoundInArray(arr, value);
    const before = this.prefixSum(segIndex);
    return before + local;
  }

  protected override async ensureLoaded(segment: Segment<T>): Promise<void> {
    if (this.segmentCache.has(segment.id)) return;
    await super.ensureLoaded(segment);
    const arr = this.segmentCache.get(segment.id)!;
    if (arr.length > 0) {
      segment.min = arr[0] as T;
      segment.max = arr[arr.length - 1] as T;
    }
  }

  protected override splitSegment(index: number): void {
    const segment = this.meta.segments[index] as Segment<T>;
    const arr = this.segmentCache.get(segment.id)!;
    const mid = arr.length >>> 1;
    const right = arr.splice(mid);
    const left = arr; // reuse original array for left half
    if (left.length === 0 || right.length === 0) return;

    this.segmentCache.set(segment.id, left);
    segment.count = left.length;
    segment.min = left[0] as T;
    segment.max = left[left.length - 1] as T;
    const newSeg: Segment<T> = {
      id: this.newId(),
      count: right.length,
      min: right[0] as T,
      max: right[right.length - 1] as T,
    };
    this.segmentCache.set(newSeg.id, right);
    this.meta.segments.splice(index + 1, 0, newSeg);
    this.recomputeFenwick();
    this.dirty.add(segment);
    this.dirty.add(newSeg);
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

  private boundInArray(arr: T[], value: T, upper: boolean): number {
    let lo = 0;
    let hi = arr.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      const cmpRes = this.cmp(arr[mid] as T, value);
      if (upper ? cmpRes <= 0 : cmpRes < 0) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  private lowerBoundInArray(arr: T[], value: T): number {
    return this.boundInArray(arr, value, false);
  }

  private upperBoundInArray(arr: T[], value: T): number {
    return this.boundInArray(arr, value, true);
  }
}
