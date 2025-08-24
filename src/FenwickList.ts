import type { IStore } from "./Store";

type Segment<T> = {
	id: string;
	count: number;
	values?: T[];
};

export class FenwickList<T> {
	private segments: Segment<T>[] = [];
	private fenwick: number[] = [];
	private totalCount = 0;
	private nextId = 0;
	private dirty = new Set<Segment<T>>();

	constructor(
		private readonly store: IStore<T>,
		private readonly maxValuesPerSegment: number,
	) {}

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
			if (seg.count > this.maxValuesPerSegment) this.splitSegment(segIndex);
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

		if (seg.count > this.maxValuesPerSegment) {
			this.splitSegment(segIndex);
		}
	}

	async get(index: number): Promise<T | undefined> {
		if (index < 0 || index >= this.totalCount) return undefined;
		const { segIndex, localIndex } = this.findByIndex(index);
		const seg = this.segments[segIndex] as Segment<T>;
		await this.ensureLoaded(seg);
		return (seg.values as T[])[localIndex] as T;
	}

	async range(min: number, max: number): Promise<T[]> {
		const out: T[] = [];
		if (this.totalCount === 0) return out;
		const a = Math.max(0, min);
		let b = max;
		if (b <= a) return out;
		if (a >= this.totalCount) return out;
		if (b > this.totalCount) b = this.totalCount;

		let { segIndex, localIndex } = this.findByIndex(a);
		let remaining = b - a;
		while (remaining > 0 && segIndex < this.segments.length) {
			const seg = this.segments[segIndex] as Segment<T>;
			await this.ensureLoaded(seg);
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
		for (const seg of this.dirty) {
			const arr = seg.values ?? [];
			await this.store.set(seg.id, arr);
		}
		const keys = Array.from(this.dirty.values()).map((s) => s.id);
		this.dirty.clear();
		return keys;
	}

	// ---- internals ----
	private async ensureLoaded(seg: Segment<T>): Promise<void> {
		if (seg.values !== undefined) return;
		const flat = (await this.store.get(seg.id)) ?? [];
		seg.values = [...flat];
		seg.count = flat.length;
	}

	private splitSegment(index: number): void {
		const seg = this.segments[index] as Segment<T>;
		const arr = seg.values as T[];
		const mid = arr.length >>> 1;
		const left = arr.slice(0, mid);
		const right = arr.slice(mid);
		if (left.length === 0 || right.length === 0) return;

		seg.values = left;
		seg.count = left.length;
		const newSeg: Segment<T> = {
			id: this.newId(),
			count: right.length,
			values: right,
		};
		this.segments.splice(index + 1, 0, newSeg);
		this.rebuildFenwick();
		this.dirty.add(seg);
		this.dirty.add(newSeg);
	}

	private findByIndex(index: number): {
		segIndex: number;
		localIndex: number;
	} {
		// Find largest prefix <= index, return containing segment and local index
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

	private prefixSum(endExclusive: number): number {
		// Sum of counts for segments [0, endExclusive)
		let sum = 0;
		let i = endExclusive;
		while (i > 0) {
			sum += this.fenwick[i - 1] ?? 0;
			i -= i & -i;
		}
		return sum;
	}

	private addFenwick(index: number, delta: number): void {
		// 0-based fenwick, but internal uses 1-based logic mapped onto 0-based array
		let i = index + 1;
		while (i <= this.fenwick.length) {
			this.fenwick[i - 1] = (this.fenwick[i - 1] ?? 0) + delta;
			i += i & -i;
		}
	}

	private rebuildFenwick(): void {
		const n = this.segments.length;
		this.fenwick = new Array<number>(n).fill(0);
		for (let i = 0; i < n; i++) {
			this.fenwick[i] = this.segments[i]?.count ?? 0;
		}
		// Build in-place: transform into fenwick tree array by accumulating lower bits
		for (let i = 0; i < n; i++) {
			const j = i + ((i + 1) & -(i + 1));
			if (j <= n - 1)
				this.fenwick[j] = (this.fenwick[j] ?? 0) + (this.fenwick[i] ?? 0);
		}
	}

	private newId(): string {
		const id = `seg_${this.nextId}`;
		this.nextId += 1;
		return id;
	}
}
