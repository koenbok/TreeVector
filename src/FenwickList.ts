import type { IStore } from "./Store";
import { FenwickBase, type BaseSegment, type FenwickBaseMeta, type MakeOptional } from "./FenwickBase";

type Segment<T> = BaseSegment<T>;

export class FenwickList<T> extends FenwickBase<T, Segment<T>> {
  constructor(store: IStore, meta: MakeOptional<FenwickBaseMeta<T, Segment<T>>, "segments">) {
    // Set defaults for missing meta properties
    const mutableMeta = { ...meta };
    if (!mutableMeta.chunkPrefix) mutableMeta.chunkPrefix = "chunk_";
    if (!mutableMeta.idPrefix) mutableMeta.idPrefix = "seg_";
    super(store, mutableMeta);
  }

  async insertAt(index: number, value: T): Promise<void> {
    const clamped = Math.max(0, Math.min(index, this.totalCount));
    if (this.meta.segments.length === 0) {
      const seg: Segment<T> = { id: this.newId(), count: 1 };
      this.segmentCache.set(seg.id, [value]);
      this.meta.segments.push(seg);
      this.rebuildFenwick();
      this.totalCount = 1;
      this.dirty.add(seg);
      return;
    }

    // Fast append path: place at end of last segment
    if (clamped === this.totalCount) {
      const segIndex = this.meta.segments.length - 1;
      const seg = this.meta.segments[segIndex] as Segment<T>;
      await this.ensureLoaded(seg);
      this.segmentCache.get(seg.id)!.push(value);
      seg.count += 1;
      this.totalCount += 1;
      this.addFenwick(segIndex, 1);
      this.dirty.add(seg);
      if (seg.count > this.meta.segmentN) this.splitSegment(segIndex);
      return;
    }

    const { segIndex, localIndex } = this.findByIndex(clamped);
    const seg = this.meta.segments[segIndex] as Segment<T>;
    await this.ensureLoaded(seg);
    const arr = this.segmentCache.get(seg.id)!;
    arr.splice(localIndex, 0, value);
    seg.count += 1;
    this.totalCount += 1;

    // If segment exceeds capacity, perform split which will recompute fenwick
    if (seg.count > this.meta.segmentN) {
      this.splitSegment(segIndex);
    } else {
      // Inline Fenwick tree point update: fenwick[idx] += 1 for idx in path
      for (let i = segIndex + 1; i <= this.fenwick.length; i += i & -i) {
        this.fenwick[i - 1]! += 1;
      }
    }

    this.dirty.add(seg);
  }
}
