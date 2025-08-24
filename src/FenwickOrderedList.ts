import type { IStore } from "./Store";

type Segment<T> = {
	id: string;
	count: number;
	min: T;
	max: T;
	values?: T[];
};

function defaultCmp<T>(a: T, b: T): number {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	return (a as any) < (b as any) ? -1 : (a as any) > (b as any) ? 1 : 0;
}

export class FenwickOrderedList<T> {
	private segments: Segment<T>[] = [];
	private fenwick: number[] = [];
	private totalCount = 0;
	private nextId = 0;
	private dirty = new Set<Segment<T>>();

	constructor(
		private readonly store: IStore<T>,
		private readonly maxValuesPerSegment: number,
		private readonly cmp: (a: T, b: T) => number = defaultCmp,
	) {}

	async insert(value: T): Promise<number> {
		if (this.segments.length === 0) {
			const seg: Segment<T> = {
				id: this.newId(),
				count: 1,
				min: value,
				max: value,
				values: [value],
			};
			this.segments.push(seg);
			this.rebuildFenwick();
			this.totalCount = 1;
			this.dirty.add(seg);
			return 0;
		}

		// locate segment: first with seg.max >= value
		let lo = 0;
		let hi = this.segments.length;
		while (lo < hi) {
			const mid = (lo + hi) >>> 1;
			const s = this.segments[mid] as Segment<T>;
			if (this.cmp(s.max, value) >= 0) hi = mid;
			else lo = mid + 1;
		}
		const segIndex = Math.min(lo, this.segments.length - 1);
		const seg = this.segments[segIndex] as Segment<T>;
		await this.ensureLoaded(seg);
		const arr = seg.values as T[];

		// lower_bound inside segment
		let l = 0;
		let r = arr.length;
		while (l < r) {
			const m = (l + r) >>> 1;
			if (this.cmp(arr[m] as T, value) < 0) l = m + 1;
			else r = m;
		}
		const localIndex = l;
		const insertPos = this.prefixSum(segIndex) + localIndex;

		if (localIndex === arr.length) arr.push(value);
		else arr.splice(localIndex, 0, value);
		seg.count += 1;
		this.totalCount += 1;
		if (this.cmp(value, seg.min) < 0) seg.min = value;
		if (this.cmp(value, seg.max) > 0) seg.max = value;
		this.addFenwick(segIndex, 1);
		this.dirty.add(seg);

		if (seg.count > this.maxValuesPerSegment) this.splitSegment(segIndex);
		return insertPos;
	}

	async get(index: number): Promise<T | undefined> {
		if (index < 0 || index >= this.totalCount) return undefined;
		const { segIndex, localIndex } = this.findByIndex(index);
		const seg = this.segments[segIndex] as Segment<T>;
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
		while (remaining > 0 && segIndex < this.segments.length) {
			const seg = this.segments[segIndex] as Segment<T>;
			await this.ensureLoaded(seg);
			const arr = seg.values as T[];
			const take = Math.min(remaining, Math.max(0, arr.length - localIndex));
			for (let i = 0; i < take; i++) out.push(arr[localIndex + i] as T);
			remaining -= take;
			segIndex += 1;
			localIndex = 0;
		}
		return out;
	}

	async scan(min: T, max: T): Promise<T[]> {
		const out: T[] = [];
		if (this.segments.length === 0) return out;
		// find first segment that could contain min
		let lo = 0;
		let hi = this.segments.length;
		while (lo < hi) {
			const mid = (lo + hi) >>> 1;
			const s = this.segments[mid] as Segment<T>;
			if (this.cmp(s.max, min) >= 0) hi = mid;
			else lo = mid + 1;
		}
		let i = Math.min(lo, this.segments.length - 1);
		while (i < this.segments.length) {
			const s = this.segments[i] as Segment<T>;
			if (this.cmp(s.min, max) > 0) break;
			await this.ensureLoaded(s);
			const arr = s.values as T[];
			// lower_bound for min
			let l = 0;
			let r = arr.length;
			while (l < r) {
				const m = (l + r) >>> 1;
				if (this.cmp(arr[m] as T, min) < 0) l = m + 1;
				else r = m;
			}
			for (let k = l; k < arr.length; k++) {
				const v = arr[k] as T;
				if (this.cmp(v, max) > 0) return out;
				out.push(v);
			}
			i += 1;
		}
		return out;
	}

	async getIndex(value: T): Promise<number> {
		if (this.segments.length === 0) return 0;
		// first segment with max >= value
		let lo = 0;
		let hi = this.segments.length;
		while (lo < hi) {
			const mid = (lo + hi) >>> 1;
			const s = this.segments[mid] as Segment<T>;
			if (this.cmp(s.max, value) >= 0) hi = mid;
			else lo = mid + 1;
		}
		const segIndex = Math.min(lo, this.segments.length - 1);
		const s = this.segments[segIndex] as Segment<T>;
		await this.ensureLoaded(s);
		const arr = s.values as T[];
		// lower_bound in arr
		let l = 0;
		let r = arr.length;
		while (l < r) {
			const m = (l + r) >>> 1;
			if (this.cmp(arr[m] as T, value) < 0) l = m + 1;
			else r = m;
		}
		const before = this.prefixSum(segIndex);
		return before + l;
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

	// internals
	private async ensureLoaded(seg: Segment<T>): Promise<void> {
		if (seg.values !== undefined) return;
		const flat = (await this.store.get(seg.id)) ?? [];
		seg.values = [...flat];
		seg.count = flat.length;
		if (flat.length > 0) {
			seg.min = flat[0] as T;
			seg.max = flat[flat.length - 1] as T;
		}
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
		seg.min = left[0] as T;
		seg.max = left[left.length - 1] as T;
		const newSeg: Segment<T> = {
			id: this.newId(),
			count: right.length,
			min: right[0] as T,
			max: right[right.length - 1] as T,
			values: right,
		};
		this.segments.splice(index + 1, 0, newSeg);
		this.rebuildFenwick();
		this.dirty.add(seg);
		this.dirty.add(newSeg);
	}

	private findByIndex(index: number): { segIndex: number; localIndex: number } {
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
		let sum = 0;
		let i = endExclusive;
		while (i > 0) {
			sum += this.fenwick[i - 1] as number;
			i -= i & -i;
		}
		return sum;
	}

	private addFenwick(index: number, delta: number): void {
		let i = index + 1;
		while (i <= this.fenwick.length) {
			this.fenwick[i - 1] += delta;
			i += i & -i;
		}
	}

	private rebuildFenwick(): void {
		const n = this.segments.length;
		this.fenwick = new Array<number>(n).fill(0);
		for (let i = 0; i < n; i++) this.fenwick[i] = this.segments[i]?.count ?? 0;
		for (let i = 0; i < n; i++) {
			const j = i + ((i + 1) & -(i + 1));
			if (j <= n - 1) this.fenwick[j] += this.fenwick[i] as number;
		}
	}

	private newId(): string {
		const id = `oseg_${this.nextId}`;
		this.nextId += 1;
		return id;
	}
}
