import type { IStore } from "./Store";
import { FenwickBase, type BaseSegment } from "./FenwickBase";

type Segment<T> = BaseSegment<T>;

export class FenwickList<T> extends FenwickBase<T, Segment<T>> {
  constructor(store: IStore, segmentN: number, chunkN: number) {
    super(store, segmentN, chunkN, "chunk_", "seg_");
  }

  async insertAt(index: number, value: T): Promise<void> {
    const clamped = Math.max(0, Math.min(index, this.totalCount));
    if (this.segments.length === 0) {
      const seg: Segment<T> = { id: this.newId(), count: 1, values: [value] };
      this.segments.push(seg);
      this.rebuildFenwick();
      this.totalCount = 1;
      this.dirty.add(seg);
      return;
    }

    // Fast append path: place at end of last segment
    if (clamped === this.totalCount) {
      const segIndex = this.segments.length - 1;
      const seg = this.segments[segIndex] as Segment<T>;
      await this.ensureLoaded(seg);
      (seg.values as T[]).push(value);
      seg.count += 1;
      this.totalCount += 1;
      this.addFenwick(segIndex, 1);
      this.dirty.add(seg);
      if (seg.count > this.segmentN) this.splitSegment(segIndex);
      return;
    }

    const { segIndex, localIndex } = this.findByIndex(clamped);
    const seg = this.segments[segIndex] as Segment<T>;
    await this.ensureLoaded(seg);
    const arr = seg.values as T[];
    arr.splice(localIndex, 0, value);
    seg.count += 1;
    this.totalCount += 1;
    this.addFenwick(segIndex, 1);
    this.dirty.add(seg);

    if (seg.count > this.segmentN) {
      this.splitSegment(segIndex);
    }
  }
}
