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
	values?: T[]; // fetch from store if not loaded
	next?: BPlusLeafNode<T>;
	prev?: BPlusLeafNode<T>;
};

type BPlusNode<T> = BPlusInternalNode<T> | BPlusLeafNode<T>;

function isLeafNode<T>(node: BPlusNode<T>): node is BPlusLeafNode<T> {
	return "values" in node;
}

function defaultCmp<T>(a: T, b: T): number {
	return a < b ? -1 : a > b ? 1 : 0;
}

export class BPlusTree<T> {
	root: BPlusNode<T> | undefined;
	dirty = new Set<BPlusLeafNode<T>>();
	private nextLeafId = 0;

	constructor(
		private store: IStore<T>,
		private maxValues: number,
		private maxChildren: number,
		private cmp: (a: T, b: T) => number = defaultCmp,
	) {}

	async insert(value: T): Promise<number> {
		if (!this.root) {
			const leaf: BPlusLeafNode<T> = {
				id: this.generateLeafId(),
				min: value,
				max: value,
				count: 1,
				values: [value],
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
		await this.ensureValuesLoaded([leaf]);

		while (leaf) {
			const values: T[] = (leaf.values ?? []) as T[];
			for (let i = 0; i < values.length; i++) {
				const v = values[i] as T;
				if (this.cmp(v, min) < 0) continue;
				if (this.cmp(v, max) > 0) return result;
				result.push(v);
			}
			leaf = leaf.next;
			if (leaf) {
				await this.ensureValuesLoaded([leaf]);
			}
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

		await this.ensureValuesLoaded([current]);
		const values: T[] = (current.values ?? []) as T[];
		return values[currentIndex];
	}

	private async ensureValuesLoaded(nodes: BPlusNode<T>[]): Promise<void> {
		await Promise.all(
			nodes.filter(isLeafNode).map(async (node) => {
				if (node.values === undefined) {
					node.values = (await this.store.get(node.id)) ?? [];
				}
			}),
		);
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
			await this.ensureValuesLoaded([node]);
			const values = node.values ?? [];
			const pos = this.lowerBound(values, value);
			values.splice(pos, 0, value);
			node.values = values;
			node.count = values.length;
			if (values.length === 0) {
				throw new Error(
					"Invariant violation: leaf must not be empty after insert",
				);
			}
			node.min = values[0] as T;
			node.max = values[values.length - 1] as T;
			// await this.store.set(node.id, values);
			this.dirty.add(node);

			if (values.length > this.maxValues) {
				const [left, right] = this.splitLeaf(node);
				return {
					node: left,
					insertedIndex: pos, // relative to subtree root; caller will adjust
					split: [left, right],
				};
			}

			return { node, insertedIndex: pos };
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
		const values = node.values ?? [];
		const mid = Math.floor(values.length / 2);
		const leftValues = values.slice(0, mid);
		const rightValues = values.slice(mid);
		if (leftValues.length === 0 || rightValues.length === 0) {
			throw new Error("Invariant violation: split produced empty leaf");
		}

		const prevNeighbor = node.prev;
		const nextNeighbor = node.next;

		const left: BPlusLeafNode<T> = {
			id: node.id,
			min: leftValues[0] as T,
			max: leftValues[leftValues.length - 1] as T,
			count: leftValues.length,
			values: leftValues,
			prev: prevNeighbor,
			next: undefined as BPlusLeafNode<T> | undefined,
		};

		const right: BPlusLeafNode<T> = {
			id: this.generateLeafId(),
			min: rightValues[0] as T,
			max: rightValues[rightValues.length - 1] as T,
			count: rightValues.length,
			values: rightValues,
			prev: left,
			next: nextNeighbor,
		};

		left.next = right;
		// Reconnect neighbors to new nodes
		if (prevNeighbor) prevNeighbor.next = left;
		if (nextNeighbor) nextNeighbor.prev = right;

		// Persist both halves
		// void this.store.set(left.id, left.values ?? []);
		// void this.store.set(right.id, right.values ?? []);
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
