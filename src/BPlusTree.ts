import type { IStore } from "./Store";

type BPlusInternalNode<T> = {
	min: T;
	max: T;
	count: number;
	children: BPlusNode<T>[];
};

type BPlusLeafNode<T> = {
	id: string;
	min: T;
	max: T;
	count: number;
	chunks?: T[][];
	next?: BPlusLeafNode<T>;
	prev?: BPlusLeafNode<T>;
};

type BPlusNode<T> = BPlusInternalNode<T> | BPlusLeafNode<T>;

function isLeafNode<T>(node: BPlusNode<T>): node is BPlusLeafNode<T> {
	return "id" in (node as object);
}

function defaultCmp<T>(a: T, b: T): number {
	return a < b ? -1 : a > b ? 1 : 0;
}

export class BPlusTree<T> {
	root: BPlusNode<T> | undefined;
	dirty = new Set<BPlusLeafNode<T>>();
	private nextLeafId = 0;
	private readonly chunkSize: number;

	constructor(
		private store: IStore<T>,
		private maxValues: number,
		private maxChildren: number,
		private cmp: (a: T, b: T) => number = defaultCmp,
	) {
		this.chunkSize = Math.min(2048, Math.max(16, this.maxValues));
	}

	async insert(value: T): Promise<number> {
		if (!this.root) {
			const leaf: BPlusLeafNode<T> = {
				id: this.generateLeafId(),
				min: value,
				max: value,
				count: 1,
				chunks: [[value]],
			};
			// await this.store.set(leaf.id, [value]);
			this.dirty.add(leaf);
			this.root = leaf;
			return 0;
		}

		const result = await this.insertRecursive(this.root, value);
		if (result.split) {
			// Root split: create a new root internal node
			const [left, right] = result.split;
			const newRoot: BPlusInternalNode<T> = {
				min: left.min,
				max: right.max,
				count: left.count + right.count,
				children: [left, right],
			};
			this.root = newRoot;
		} else {
			this.root = result.node;
		}
		return result.insertedIndex;
	}

	async range(min: T, max: T): Promise<T[]> {
		const result: T[] = [];
		if (!this.root) return result;

		let leaf = await this.findLeafForValue(min);
		if (!leaf) return result;
		// ensure chunks present
		await this.ensureLeafChunks(leaf);

		while (leaf) {
			await this.ensureLeafChunks(leaf);
			const chunks = (leaf.chunks as T[][]) ?? [];
			for (let ci = 0; ci < chunks.length; ci++) {
				const chunk = chunks[ci] as T[];
				for (let i = 0; i < chunk.length; i++) {
					const v = chunk[i] as T;
					if (this.cmp(v, min) < 0) continue;
					if (this.cmp(v, max) > 0) return result;
					result.push(v);
				}
			}
			leaf = leaf.next;
			// next iteration will ensure chunks for the next leaf
		}
		return result;
	}

	async get(index: number): Promise<T | undefined> {
		if (!this.root) return undefined;
		if (index < 0 || index >= this.root.count) return undefined;

		let current: BPlusNode<T> = this.root;
		let currentIndex = index;
		while (!isLeafNode(current)) {
			let accumulated = 0;
			let selectedChild: BPlusNode<T> | undefined;
			for (let i = 0; i < current.children.length; i++) {
				const child = current.children[i];
				if (!child) continue;
				if (accumulated + child.count > currentIndex) {
					selectedChild = child;
					currentIndex -= accumulated;
					break;
				}
				accumulated += child.count;
			}
			if (!selectedChild) {
				const fallback = current.children[current.children.length - 1];
				if (!fallback) return undefined;
				selectedChild = fallback;
				currentIndex -= Math.max(0, accumulated - fallback.count);
			}
			current = selectedChild;
		}

		await this.ensureLeafChunks(current);
		const chunks = (current.chunks as T[][]) ?? [];
		let idx = currentIndex;
		for (let ci = 0; ci < chunks.length; ci++) {
			const chunk = chunks[ci] as T[];
			if (idx < chunk.length) return chunk[idx] as T;
			idx -= chunk.length;
		}
		return undefined;
	}

	private async ensureLeafChunks(node: BPlusLeafNode<T>): Promise<void> {
		if (node.chunks !== undefined) return;
		const flat = (await this.store.get(node.id)) ?? [];
		const chunks: T[][] = [];
		for (let i = 0; i < flat.length; i += this.chunkSize) {
			chunks.push(flat.slice(i, i + this.chunkSize));
		}
		node.chunks = chunks;
		node.count = flat.length;
		if (flat.length > 0) {
			node.min = flat[0] as T;
			node.max = flat[flat.length - 1] as T;
		}
	}

	private async insertRecursive(
		node: BPlusNode<T>,
		value: T,
	): Promise<{
		node: BPlusNode<T>;
		insertedIndex: number;
		split?: [BPlusNode<T>, BPlusNode<T>];
	}> {
		if (isLeafNode(node)) {
			await this.ensureLeafChunks(node);
			const { posInLeaf, chunkIndex, posInChunk } = this.findInsertPosition(
				node,
				value,
			);
			const chunks = node.chunks as T[][];
			const chunk = chunks[chunkIndex] as T[];
			if (posInChunk === chunk.length) chunk.push(value);
			else chunk.splice(posInChunk, 0, value);
			node.count += 1;
			if (this.cmp(value, node.min) < 0) node.min = value;
			if (this.cmp(value, node.max) > 0) node.max = value;
			if (chunk.length > this.chunkSize) {
				const mid = chunk.length >>> 1;
				const left = chunk.slice(0, mid);
				const right = chunk.slice(mid);
				chunks.splice(chunkIndex, 1, left, right);
			}
			this.dirty.add(node);

			if (node.count > this.maxValues) {
				const [left, right] = this.splitLeaf(node);
				return {
					node: left,
					insertedIndex: posInLeaf,
					split: [left, right],
				};
			}

			return { node, insertedIndex: posInLeaf };
		}

		// Internal node
		const childIndex = this.findChildIndexForValue(node, value);
		let offset = 0;
		for (let i = 0; i < childIndex; i++) {
			const c = node.children[i];
			if (c) offset += c.count;
		}
		const child = node.children[childIndex];
		if (!child) {
			throw new Error("Invariant violation: child not found during insert");
		}
		const res = await this.insertRecursive(child, value);

		if (res.split) {
			// Replace the child with two nodes
			node.children.splice(childIndex, 1, res.split[0], res.split[1]);
			// Fix leaf linkage if the split happened at leaf level.
			// splitLeaf already handled next/prev pointers.
		}

		// Recompute metadata
		if (node.children.length === 0) {
			throw new Error("Invariant violation: internal node has no children");
		}
		node.count = node.children.reduce((sum, c) => sum + (c ? c.count : 0), 0);
		const firstChild = node.children[0];
		const lastChild = node.children[node.children.length - 1];
		if (!firstChild || !lastChild) {
			throw new Error("Invariant violation: missing child after recompute");
		}
		node.min = firstChild.min;
		node.max = lastChild.max;

		// Split internal node if needed
		if (node.children.length > this.maxChildren) {
			const [left, right] = this.splitInternal(node);
			return {
				node: left,
				insertedIndex: offset + res.insertedIndex,
				split: [left, right],
			};
		}

		return { node, insertedIndex: offset + res.insertedIndex };
	}

	private splitLeaf(
		node: BPlusLeafNode<T>,
	): [BPlusLeafNode<T>, BPlusLeafNode<T>] {
		const chunks = (node.chunks as T[][]).slice();
		const total = node.count;
		const midCount = total >>> 1;
		let acc = 0;
		const leftChunks: T[][] = [];
		const rightChunks: T[][] = [];
		for (let i = 0; i < chunks.length; i++) {
			const c = chunks[i] as T[];
			if (acc + c.length <= midCount) {
				leftChunks.push(c);
				acc += c.length;
			} else if (acc < midCount) {
				const cut = midCount - acc;
				leftChunks.push(c.slice(0, cut));
				rightChunks.push(c.slice(cut));
				acc = midCount;
			} else {
				rightChunks.push(c);
			}
		}
		const leftCount = midCount;
		const rightCount = total - midCount;
		if (leftCount <= 0 || rightCount <= 0) {
			throw new Error("Invariant violation: split produced empty leaf");
		}

		const prevNeighbor = node.prev;
		const nextNeighbor = node.next;

		const leftFirstChunk = leftChunks[0] as T[];
		const leftMin = leftFirstChunk[0] as T;
		const leftLastChunk = leftChunks[leftChunks.length - 1] as T[];
		const leftMax = leftLastChunk[leftLastChunk.length - 1] as T;

		const rightFirstChunk = rightChunks[0] as T[];
		const rightMin = rightFirstChunk[0] as T;
		const rightLastChunk = rightChunks[rightChunks.length - 1] as T[];
		const rightMax = rightLastChunk[rightLastChunk.length - 1] as T;

		const left: BPlusLeafNode<T> = {
			id: node.id,
			min: leftMin,
			max: leftMax,
			count: leftCount,
			chunks: leftChunks,
			prev: prevNeighbor,
			next: undefined as BPlusLeafNode<T> | undefined,
		};

		const right: BPlusLeafNode<T> = {
			id: this.generateLeafId(),
			min: rightMin,
			max: rightMax,
			count: rightCount,
			chunks: rightChunks,
			prev: left,
			next: nextNeighbor,
		};

		left.next = right;
		if (prevNeighbor) prevNeighbor.next = left;
		if (nextNeighbor) nextNeighbor.prev = right;

		this.dirty.add(left);
		this.dirty.add(right);

		return [left, right];
	}

	private splitInternal(
		node: BPlusInternalNode<T>,
	): [BPlusInternalNode<T>, BPlusInternalNode<T>] {
		const mid = Math.floor(node.children.length / 2);
		const leftChildren = node.children.slice(0, mid);
		const rightChildren = node.children.slice(mid);
		if (leftChildren.length === 0 || rightChildren.length === 0) {
			throw new Error(
				"Invariant violation: split produced empty internal node",
			);
		}

		const left: BPlusInternalNode<T> = {
			min: (leftChildren[0] as BPlusNode<T>).min,
			max: (leftChildren[leftChildren.length - 1] as BPlusNode<T>).max,
			count: leftChildren.reduce((s, c) => s + (c ? c.count : 0), 0),
			children: leftChildren,
		};

		const right: BPlusInternalNode<T> = {
			min: (rightChildren[0] as BPlusNode<T>).min,
			max: (rightChildren[rightChildren.length - 1] as BPlusNode<T>).max,
			count: rightChildren.reduce((s, c) => s + (c ? c.count : 0), 0),
			children: rightChildren,
		};

		return [left, right];
	}

	private findLeafForValue = async (
		value: T,
	): Promise<BPlusLeafNode<T> | undefined> => {
		if (!this.root) return undefined;
		let node: BPlusNode<T> = this.root;
		while (!isLeafNode(node)) {
			const idx = this.findChildIndexForValue(node, value);
			const next = node.children[idx];
			if (!next)
				throw new Error("Invariant violation: child not found during search");
			node = next;
		}
		return node;
	};

	private findChildIndexForValue(node: BPlusInternalNode<T>, value: T): number {
		// Children are ordered by min/max; pick first child whose max >= value
		for (let i = 0; i < node.children.length; i++) {
			const child = node.children[i];
			if (!child) continue;
			if (this.cmp(value, child.max) <= 0) return i;
		}
		return node.children.length - 1;
	}

	private lowerBound(values: readonly T[], value: T): number {
		let low = 0;
		let high = values.length;
		while (low < high) {
			const mid = (low + high) >>> 1;
			const midVal = values[mid] as T;
			if (this.cmp(midVal, value) < 0) low = mid + 1;
			else high = mid;
		}
		return low;
	}

	private findInsertPosition(
		leaf: BPlusLeafNode<T>,
		value: T,
	): { posInLeaf: number; chunkIndex: number; posInChunk: number } {
		const chunks = (leaf.chunks as T[][]) ?? [];
		let pos = 0;
		for (let ci = 0; ci < chunks.length; ci++) {
			const chunk = chunks[ci] as T[];
			const last = chunk[chunk.length - 1] as T;
			if (this.cmp(value, last) <= 0) {
				const local = this.lowerBound(chunk, value);
				return { posInLeaf: pos + local, chunkIndex: ci, posInChunk: local };
			}
			pos += chunk.length;
		}
		if (chunks.length === 0) {
			leaf.chunks = [[value]];
			return { posInLeaf: 0, chunkIndex: 0, posInChunk: 0 };
		}
		return {
			posInLeaf: pos,
			chunkIndex: chunks.length - 1,
			posInChunk: (chunks[chunks.length - 1] as T[]).length,
		};
	}

	private generateLeafId(): string {
		const id = `leaf_${this.nextLeafId}`;
		this.nextLeafId += 1;
		return id;
	}

	// Debug/benchmark-only stats; not for general API usage
	public debugStats(): {
		totalNodes: number;
		internalNodes: number;
		leafNodes: number;
		leafSizeDistribution: Map<number, number>;
	} {
		let internalNodes = 0;
		let leafNodes = 0;
		if (!this.root) {
			return {
				totalNodes: 0,
				internalNodes: 0,
				leafNodes: 0,
				leafSizeDistribution: new Map(),
			};
		}

		const stack: BPlusNode<T>[] = [this.root];
		while (stack.length) {
			const node = stack.pop() as BPlusNode<T>;
			if (isLeafNode(node)) {
				leafNodes += 1;
			} else {
				internalNodes += 1;
				for (let i = 0; i < node.children.length; i++) {
					const c = node.children[i];
					if (c) stack.push(c);
				}
			}
		}

		// Leaf size histogram using a full DFS to avoid depending on next/prev correctness
		const dist = new Map<number, number>();
		const stack2: BPlusNode<T>[] = [this.root];
		while (stack2.length) {
			const n = stack2.pop() as BPlusNode<T>;
			if (isLeafNode(n)) {
				const size = n.count;
				dist.set(size, (dist.get(size) ?? 0) + 1);
			} else {
				for (let i = 0; i < n.children.length; i++) {
					const c = n.children[i];
					if (c) stack2.push(c);
				}
			}
		}

		return {
			totalNodes: internalNodes + leafNodes,
			internalNodes,
			leafNodes,
			leafSizeDistribution: dist,
		};
	}
}
