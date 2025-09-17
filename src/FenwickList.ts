import type { IStore } from "./Store";
import {
  FenwickBase,
  type BaseSegment,
  type FenwickBaseMeta,
} from "./FenwickBase";

type Segment<T> = BaseSegment<T>;

type FenwickListMeta<T> = FenwickBaseMeta<T, Segment<T>>;

function getDefaults<T>(meta: Partial<FenwickListMeta<T>>): FenwickListMeta<T> {
  return {
    segmentCount: 1024,
    chunkCount: 128,
    segments: [],
    chunks: [],
    ...meta,
  } as FenwickListMeta<T>;
}

// Helper: sort pairs by target index asc, then by original order asc, and annotate stable rank
type InsertPair<T> = { idx: number; val: T | undefined; order: number; rank?: number };
function sortAndRankInsertPairs<T>(pairs: Array<InsertPair<T>>): void {
  pairs.sort((a, b) => (a.idx - b.idx) || (a.order - b.order));
  for (let i = 0; i < pairs.length; i++) pairs[i]!.rank = i;
}

export class FenwickList<T> extends FenwickBase<T, Segment<T>> {
  constructor(store: IStore, meta: Partial<FenwickListMeta<T>>) {
    super(store, getDefaults<T>(meta));
  }

  async insertAt(index: number, value: T): Promise<void> {
    await this.insertManyAt([index], [value]);
  }

  private async singleInsertAt(index: number, value: T): Promise<void> {
    const clamped = Math.max(0, Math.min(index, this.totalCount));
    if (this.meta.segments.length === 0) {
      const seg: Segment<T> = { count: 1 } as Segment<T>;
      this.createInitialSegment(seg, value);
      return;
    }

    // Fast append path: place at end of last segment
    if (clamped === this.totalCount) {
      const segIndex = this.meta.segments.length - 1;
      const seg = this.meta.segments[segIndex] as Segment<T>;
      await this.ensureSegmentLoaded(seg);
      const arr = this.getOrCreateArraySync(seg, true);
      arr.push(value);
      seg.count += 1;
      this.totalCount += 1;
      this.dirty.add(seg);
      if (seg.count > this.meta.segmentCount) await this.splitSegment(segIndex);
      else this.addFenwick(segIndex, 1);
      return;
    }

    const { segIndex, localIndex } = this.findByIndex(clamped);
    const seg = this.meta.segments[segIndex] as Segment<T>;
    await this.ensureSegmentLoaded(seg);
    const arr = this.getOrCreateArraySync(seg, true);
    arr.splice(localIndex, 0, value);
    seg.count += 1;
    this.totalCount += 1;

    // If segment exceeds capacity, perform split which will recompute fenwick
    if (seg.count > this.meta.segmentCount) {
      await this.splitSegment(segIndex);
    } else {
      this.addFenwick(segIndex, 1);
    }

    this.dirty.add(seg);
  }

  async insertManyAt(indexes: number[], values: Array<T | undefined>): Promise<void> {
    if (indexes.length !== values.length) {
      throw new Error("indexes and values must have the same length");
    }
    if (indexes.length === 0) return;

    if (indexes.length === 1) {
      await this.singleInsertAt(indexes[0] as number, values[0] as T);
      return;
    }

    // Fast path: empty structure – build initial segment in one pass using insertAt semantics.
    if (this.meta.segments.length === 0) {
      const pairs: Array<InsertPair<T>> = new Array(indexes.length);
      for (let i = 0; i < indexes.length; i++) pairs[i] = { idx: indexes[i] as number, val: values[i] as T | undefined, order: i };
      // Apply insertAt semantics on an empty array: insert at given index, shifting to the right
      sortAndRankInsertPairs(pairs);
      const newArr: T[] = [];
      for (let i = 0; i < pairs.length; i++) {
        const p = pairs[i]!;
        const pos = Math.max(0, Math.min(p.idx, newArr.length));
        newArr.splice(pos, 0, p.val as T);
      }
      const seg: Segment<T> = { count: newArr.length } as Segment<T>;
      // Seed a new segment with the batch values
      this.meta.segments.push(seg);
      void this.getOrCreateArraySync(seg, true, newArr as unknown as T[]);
      this.totalCount = newArr.length;
      this.rebuildIndices();
      this.dirty.add(seg);
      // Split if oversized
      const segIndex = this.meta.segments.length - 1;
      while (seg.count > this.meta.segmentCount) {
        await this.splitSegment(segIndex);
      }
      return;
    }

    // Stable order by desired final index (idx asc, then order asc)
    const pairs: Array<InsertPair<T>> = new Array(indexes.length);
    for (let i = 0; i < indexes.length; i++) pairs[i] = { idx: indexes[i] as number, val: values[i] as T | undefined, order: i };
    sortAndRankInsertPairs(pairs);

    // Convert to original-array coordinates (before any insertions): oldIdx = idx - rank
    const mapped: Array<{ oldIdx: number; val: T | undefined; rank: number }> = new Array(pairs.length);
    for (let i = 0; i < pairs.length; i++) {
      const oldIdx = Math.max(0, Math.min(this.totalCount, (pairs[i]!.idx as number) - i));
      mapped[i] = { oldIdx, val: pairs[i]!.val, rank: pairs[i]!.rank as number };
    }

    // Group by target segment using old indices
    const perSeg = new Map<number, Array<{ local: number; val: T | undefined; rank: number }>>();
    for (let i = 0; i < mapped.length; i++) {
      const m = mapped[i]!;
      let segIndex: number;
      let localIndex: number;
      if (m.oldIdx === this.totalCount) {
        // Append to last segment
        segIndex = Math.max(0, this.meta.segments.length - 1);
        const seg = this.meta.segments[segIndex] as Segment<T>;
        await this.ensureSegmentLoaded(seg);
        const arr = this.getOrCreateArraySync(seg, true);
        localIndex = arr.length;
      } else {
        const found = this.findByIndex(m.oldIdx);
        segIndex = found.segIndex;
        localIndex = found.localIndex;
      }
      let list = perSeg.get(segIndex);
      if (!list) {
        list = [];
        perSeg.set(segIndex, list);
      }
      list.push({ local: localIndex, val: m.val, rank: m.rank });
    }

    // Prefetch all required segments' data (loads underlying chunks in parallel)
    await Promise.all(
      Array.from(perSeg.keys()).map(async (segIndex) =>
        this.getReadOnlyArrayForSegment(this.meta.segments[segIndex] as Segment<T>),
      ),
    );

    // Apply per-segment merges
    for (const [segIndex, inserts] of perSeg.entries()) {
      const seg = this.meta.segments[segIndex] as Segment<T>;
      await this.ensureSegmentLoaded(seg);
      const arr = this.getOrCreateArraySync(seg, true);

      // Sort by local position asc; for equal local, process earlier pairs first (rank asc)
      inserts.sort((a, b) => (a.local - b.local) || (a.rank - b.rank));

      const newArr: T[] = new Array<T>(arr.length + inserts.length);
      let src = 0;
      let dst = 0;
      let i = 0;
      while (i < inserts.length) {
        const currentLocal = inserts[i]!.local;
        // Copy everything before the insertion point
        const take = currentLocal - src;
        if (take > 0) {
          for (let k = 0; k < take; k++) newArr[dst + k] = arr[src + k] as T;
          src += take;
          dst += take;
        }
        // Insert all values for this local position in stable order
        let j = i;
        while (j < inserts.length && inserts[j]!.local === currentLocal) {
          newArr[dst++] = (inserts[j]!.val as unknown) as T;
          j++;
        }
        // Now copy the occupant at this position (if any), then advance src by 1
        if (src < arr.length && src === currentLocal) {
          newArr[dst++] = arr[src++] as T;
        }
        i = j;
      }
      // Copy remaining tail
      while (src < arr.length) newArr[dst++] = arr[src++] as T;

      // Replace in-memory array contents to keep reference stable
      arr.length = 0;
      Array.prototype.push.apply(arr, newArr as unknown as T[]);
      seg.count = arr.length;
      this.dirty.add(seg);

      // Split if necessary (may need multiple splits)
      while (seg.count > this.meta.segmentCount) {
        await this.splitSegment(segIndex);
      }
    }

    // Update totals and fenwick after all per-segment changes
    this.totalCount += indexes.length;
    this.rebuildIndices();
  }
}
