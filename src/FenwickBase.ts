import type { IStore } from "./Store";
import {
	flushSegmentsToChunks,
	loadSegmentFromChunks,
	type ChunkCache,
} from "./Chunks";

export type BaseSegment<T> = {
	id: string;
	count: number;
	values?: T[];
};

export abstract class FenwickBase<T, S extends BaseSegment<T>> {
	protected segments: S[] = [];
	protected fenwick: number[] = [];
	protected totalCount = 0;
	protected nextId = 0;
	protected dirty = new Set<S>();
	protected chunkCache: ChunkCache<T> | undefined;

	protected constructor(
		protected readonly store: IStore,
		protected readonly segmentN: number,
		protected readonly chunkN: number,
		private readonly chunkPrefix: string,
		private readonly idPrefix: string,
	) {}

	// ---- public ops shared ----
	async get(index: number): Promise<T | undefined> {
		if (index < 0 || index >= this.totalCount) return undefined;
		const { segIndex, localIndex } = this.findByIndex(index);
		const seg = this.segments[segIndex] as S;
		await this.ensureLoaded(seg);
		return (seg.values as T[])[localIndex] as T;
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
			this.segments
				.slice(segIndex, endSegIndex + 1)
				.map((s) => this.ensureLoaded(s as S)),
		);
		while (remaining > 0 && segIndex < this.segments.length) {
			const seg = this.segments[segIndex] as S;
			const arr = seg.values as T[];
			const take = Math.min(remaining, Math.max(0, arr.length - localIndex));
			if (take > 0) out.push(...arr.slice(localIndex, localIndex + take));
			remaining -= take;
			segIndex += 1;
			localIndex = 0;
		}
		return out;
	}

	async flush(): Promise<string[]> {
		const keys = await flushSegmentsToChunks<T>(
			this.store,
			Array.from(this.dirty.values()),
			this.chunkN,
			this.chunkPrefix,
		);
		this.dirty.clear();
		return keys;
	}

	// ---- internals shared ----
	protected async ensureLoaded(seg: S): Promise<void> {
		if (seg.values !== undefined) return;
		const arr = await loadSegmentFromChunks<T>(
			this.store,
			seg.id,
			this.chunkN,
			this.chunkPrefix,
			this.chunkCache,
		);
		seg.values = arr.slice();
		seg.count = arr.length;
	}

	protected splitSegment(index: number): void {
		const seg = this.segments[index] as S;
		const arr = seg.values as T[];
		const mid = arr.length >>> 1;
		const left = arr.slice(0, mid);
		const right = arr.slice(mid);
		if (left.length === 0 || right.length === 0) return;

		seg.values = left;
		seg.count = left.length;
		const newSeg = {
			id: this.newId(),
			count: right.length,
			values: right,
		} as S;
		this.segments.splice(index + 1, 0, newSeg);
		this.rebuildFenwick();
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
		const segIndex = Math.min(idx, this.segments.length - 1);
		const local = index - sum;
		return { segIndex, localIndex: local };
	}

	protected prefixSum(endExclusive: number): number {
		let sum = 0;
		let i = endExclusive;
		while (i > 0) {
			sum += this.fenwick[i - 1] as number;
			i -= i & -i;
		}
		return sum;
	}

	protected addFenwick(index: number, delta: number): void {
		let i = index + 1;
		while (i <= this.fenwick.length) {
			this.fenwick[i - 1] = (this.fenwick[i - 1] ?? 0) + delta;
			i += i & -i;
		}
	}

	protected rebuildFenwick(): void {
		const n = this.segments.length;
		this.fenwick = new Array<number>(n).fill(0);
		for (let i = 0; i < n; i++) this.fenwick[i] = this.segments[i]?.count ?? 0;
		for (let i = 0; i < n; i++) {
			const j = i + ((i + 1) & -(i + 1));
			if (j <= n - 1)
				this.fenwick[j] = (this.fenwick[j] ?? 0) + (this.fenwick[i] ?? 0);
		}
	}

	protected newId(): string {
		const id = `${this.idPrefix}${this.nextId}`;
		this.nextId += 1;
		return id;
	}
}
