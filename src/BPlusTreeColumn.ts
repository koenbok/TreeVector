import type { IndexedColumnInterface, OrderedColumnInterface } from "./Column";
import type { IStore } from "./Store";
import type { FenwickBaseMeta, BaseSegment } from "./FenwickBase";

/**
 * B+ tree column skeletons with leaf blocks and subtree sizes.
 *
 * Goals of this skeleton:
 * - Provide a clear structure that adheres to the project's column interfaces.
 * - Use leaf "blocks" (arrays) sized by `segmentCount`, persisted in `chunkCount`-sized chunks (T[][]).
 * - Keep code easy to extend into a full B+ tree (internal nodes with subtree sizes, value separator keys).
 *
 * Current state:
 * - Fully supports single-leaf operation (no splits). This is enough to validate API shape and persistence flow.
 * - Contains scaffolding for a real B+ tree (node types, helpers, and TODOs where splits/merges go).
 * - When capacity is exceeded, throws a descriptive error indicating where to implement splits.
 *
 * Extending to a full B+ implementation:
 * - On leaf overflow: split the leaf, create a right sibling, update inorder leaf links, insert child+key into parent.
 * - On internal overflow: split the internal node, promote middle key to parent, recurse up; create new root if needed.
 * - Maintain `sizes[]` (subtree counts) and `keys[]` (separator keys e.g., child's max) in internal nodes.
 * - Update `meta.segments` to mirror leaf order; map each leaf 1:1 to a segment for persistence.
 */
type OrderedSegment<T> = BaseSegment<T> & { min: T; max: T };

type BPlusMeta<T, S extends BaseSegment<T>> = FenwickBaseMeta<T, S>;

function getDefaults<T, S extends BaseSegment<T>>(
  meta: Partial<BPlusMeta<T, S>>,
): BPlusMeta<T, S> {
  return {
    segmentCount: 1024, // max values per leaf block
    chunkCount: 128, // segments per persisted chunk
    segments: [],
    chunks: [],
    ...meta,
  } as BPlusMeta<T, S>;
}

type LeafNode<T, S extends BaseSegment<T>> = {
  type: "leaf";
  seg: S;
  values: T[];
  count: number;
  next: LeafNode<T, S> | null; // inorder forward pointer (for scan)
  prev: LeafNode<T, S> | null; // inorder backward pointer
  // For ordered column fast paths
  min?: T;
  max?: T;
};

type InternalNode<T, S extends BaseSegment<T>> = {
  type: "internal";
  // Children in order; for a true B+ tree, keys.length === children.length
  children: Node<T, S>[];
  // Subtree counts per child for rank/select by index
  sizes: number[];
  // For ordered column: max key per child (separator), typically child's max
  keys?: T[];
};

type Node<T, S extends BaseSegment<T>> = LeafNode<T, S> | InternalNode<T, S>;

/**
 * Minimal base for B+ implementations. Provides:
 * - Meta management and persistence to IStore (chunked).
 * - Single-leaf operation (no splits) as a working baseline.
 * - Node/leaf scaffolding for future full B+ tree.
 */
abstract class BPlusTreeBase<T, S extends BaseSegment<T>> {
  protected meta: BPlusMeta<T, S>;
  protected root: Node<T, S> | null = null;
  protected totalCount = 0;

  // Inorder head leaf for sequential scans. With a single leaf, head === root.
  protected head: LeafNode<T, S> | null = null;

  // Persistence helpers (same model as other columns)
  protected chunkCache = new Map<number, T[][]>();
  protected dirty = new Set<S>();
  protected structureChangedSinceFlush = false;

  // Segment index lookup in meta.segments (kept in-order with leaves)
  protected segmentIndexByRef = new Map<S, number>();

  // Tree fanout/leaf capacity controls
  protected readonly order: number;

  constructor(
    protected readonly store: IStore,
    meta: Partial<BPlusMeta<T, S>>,
    order = 16,
  ) {
    this.meta = getDefaults<T, S>(meta);
    this.order = Math.max(3, order | 0);
    this.rebuildFromMeta();
  }

  // ---- abstract hooks for subclasses ----

  // For ordered variant: comparator, else undefined (not used by indexed).
  protected abstract cmp(a: T, b: T): number | 0;

  // On leaf mutation, subclasses may update min/max; base no-op by default.
  protected onLeafMutated(_leaf: LeafNode<T, S>): void {
    // no-op in base
  }

  // ---- meta management ----

  getMeta(): BPlusMeta<T, S> {
    return this.meta;
  }

  setMeta(meta: BPlusMeta<T, S>): void {
    this.meta = getDefaults<T, S>(meta);
    this.rebuildFromMeta();
  }

  protected rebuildFromMeta(): void {
    this.chunkCache.clear();
    this.dirty.clear();
    this.segmentIndexByRef.clear();
    this.root = null;
    this.head = null;
    this.totalCount = 0;

    // Build a single-level tree (leaves only) from meta.segments
    if (!Array.isArray(this.meta.segments) || this.meta.segments.length === 0) {
      return;
    }
    let prevLeaf: LeafNode<T, S> | null = null;
    for (let i = 0; i < this.meta.segments.length; i++) {
      const seg = this.meta.segments[i] as S;
      const leaf: LeafNode<T, S> = {
        type: "leaf",
        seg,
        values: new Array<T>(seg.count | 0), // unknown content until loaded; size placeholder
        count: seg.count | 0,
        next: null,
        prev: prevLeaf,
      };
      if (prevLeaf) prevLeaf.next = leaf;
      else this.head = leaf;
      prevLeaf = leaf;
      this.totalCount += leaf.count;
      this.segmentIndexByRef.set(seg, i);
    }

    // If there is exactly one leaf, make it root; else, for skeleton, keep root = head (still a leaf)
    this.root = this.head;
  }

  // ---- helpers: persistence ----

  protected effectiveChunkSize(): number {
    return this.meta.chunkCount > 0 ? this.meta.chunkCount : 1;
  }

  protected async getOrLoadChunk(index: number): Promise<T[][]> {
    const cached = this.chunkCache.get(index);
    if (cached) return cached as T[][];
    const chunkSize = this.effectiveChunkSize();
    const key = this.meta.chunks[index];
    let chunk = (key ? await this.store.get<T[][]>(key) : undefined) ?? [];
    if (!Array.isArray(chunk)) chunk = [];
    if (chunk.length < chunkSize) {
      const augmented = new Array<T[]>(chunkSize);
      for (let i = 0; i < chunkSize; i++)
        augmented[i] = (chunk[i] ?? []) as T[];
      chunk = augmented;
    }
    this.chunkCache.set(index, chunk as T[][]);
    return chunk as T[][];
  }

  protected generateChunkKey(index: number): string {
    const suffix = `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
    return `bpt_chunk_${index}_${suffix}`;
  }

  protected rebuildSegmentIndexMap(): void {
    this.segmentIndexByRef.clear();
    for (let i = 0; i < this.meta.segments.length; i++) {
      this.segmentIndexByRef.set(this.meta.segments[i] as S, i);
    }
  }

  // Ensure a leaf has a concrete array with loaded values from storage
  protected async ensureLeafLoaded(leaf: LeafNode<T, S>): Promise<void> {
    if (
      leaf.values &&
      leaf.values.length === leaf.count &&
      leaf.values[0] !== undefined
    ) {
      // Heuristic: values already loaded; leave as-is
      return;
    }
    const idx = this.segmentIndexByRef.get(leaf.seg);
    if (idx === undefined || idx < 0) {
      // Not persisted yet; ensure an empty array of proper length (may be 0)
      leaf.values =
        leaf.values && leaf.values.length === leaf.count
          ? leaf.values
          : new Array<T>(leaf.count);
      return;
    }
    const chunkSize = this.effectiveChunkSize();
    const cidx = Math.floor(idx / chunkSize);
    const pos = idx % chunkSize;
    const chunk = await this.getOrLoadChunk(cidx);
    const arr = (chunk[pos] ?? []) as T[];
    leaf.values = arr.slice() as unknown as T[];
    leaf.count = leaf.values.length;
    this.onLeafMutated(leaf);
  }

  // ---- skeleton B+ operations (single-leaf baseline) ----

  protected isSingleLeaf(): boolean {
    return (
      !!this.root &&
      this.root.type === "leaf" &&
      this.root === this.head &&
      this.head?.next === null
    );
  }

  // Insert at index within a single-leaf tree
  protected async singleLeafInsertAt(index: number, value: T): Promise<number> {
    if (!this.isSingleLeaf()) {
      throw new Error(
        "Single-leaf fast path not applicable; implement full B+ insertion.",
      );
    }
    let leaf = this.root as LeafNode<T, S> | null;
    if (!leaf) {
      // No leaf yet: create one and a segment
      const seg = { count: 1 } as S;
      leaf = {
        type: "leaf",
        seg,
        values: [value],
        count: 1,
        next: null,
        prev: null,
      };
      this.root = leaf;
      this.head = leaf;
      this.meta.segments = [seg];
      this.rebuildSegmentIndexMap();
      this.totalCount = 1;
      this.dirty.add(seg);
      this.structureChangedSinceFlush = true;
      this.onLeafMutated(leaf);
      return 0;
    }

    await this.ensureLeafLoaded(leaf);
    const i = Math.max(0, Math.min(index, leaf.count));
    leaf.values.splice(i, 0, value);
    leaf.count += 1;
    leaf.seg.count = leaf.count;
    this.totalCount += 1;
    this.dirty.add(leaf.seg);
    this.onLeafMutated(leaf);

    if (leaf.count > this.meta.segmentCount) {
      // TODO: Split leaf and create internal root; update meta.segments order accordingly.
      // - Right leaf gets a new segment inserted after current leaf's segment in meta.segments.
      // - Update inorder links (leaf.next/prev).
      // - Create an internal root with two children and sizes [leaf.count, right.count].
      throw new Error(
        "Leaf overflow: implement B+ leaf split and parent update.",
      );
    }
    return i;
  }

  // Get value at index in a single-leaf tree
  protected async singleLeafGet(index: number): Promise<T | undefined> {
    if (!this.head) return undefined;
    if (index < 0 || index >= this.totalCount) return undefined;
    const leaf = this.head;
    await this.ensureLeafLoaded(leaf);
    return leaf.values[index] as T;
  }

  // Range within a single-leaf tree
  protected async singleLeafRange(min: number, max: number): Promise<T[]> {
    const out: T[] = [];
    const leaf = this.head;
    if (!leaf) return out;
    const a = Math.max(0, min | 0);
    let b = max | 0;
    if (b <= a) return out;
    if (b > this.totalCount) b = this.totalCount;
    await this.ensureLeafLoaded(leaf);
    out.push(...leaf.values.slice(a, b));
    return out;
  }

  // Flush dirty leaves; if structure changed (leaf order/number), rewrite all chunks to reflect new order.
  protected async flushLeaves(): Promise<string[]> {
    const written: string[] = [];
    const chunkSize = this.effectiveChunkSize();

    if (!this.head) return written;

    if (this.structureChangedSinceFlush) {
      // Rewrite entire leaf order into chunks
      const leaves = this.collectLeavesInOrder();
      // Ensure meta.segments mirrors leaf order 1:1
      this.meta.segments = leaves.map((l) => l.seg);
      this.rebuildSegmentIndexMap();

      const chunksNeeded = Math.ceil(leaves.length / chunkSize);
      for (let cidx = 0; cidx < chunksNeeded; cidx++) {
        const newChunk: T[][] = new Array<T[]>(chunkSize);
        for (let pos = 0; pos < chunkSize; pos++) {
          const li = cidx * chunkSize + pos;
          if (li >= leaves.length) {
            newChunk[pos] = [] as T[];
            continue;
          }
          const leaf = leaves[li];
          if (!leaf) {
            newChunk[pos] = [] as T[];
            continue;
          }
          await this.ensureLeafLoaded(leaf);
          newChunk[pos] = leaf.values.slice() as unknown as T[];
        }
        const key = this.generateChunkKey(cidx);
        await this.store.set<T[][]>(key, newChunk);
        this.meta.chunks[cidx] = key;
        this.chunkCache.set(cidx, newChunk);
        written.push(key);
      }
      this.dirty.clear();
      this.structureChangedSinceFlush = false;
      return written;
    }

    if (this.dirty.size === 0) return written;

    // Otherwise, write only dirty leaves in their current chunks
    const changedByChunk = new Map<number, S[]>();
    for (const seg of this.dirty) {
      const idx = this.segmentIndexByRef.get(seg);
      if (idx === undefined || idx < 0) continue;
      const cidx = Math.floor(idx / chunkSize);
      const arr = changedByChunk.get(cidx);
      if (arr) arr.push(seg);
      else changedByChunk.set(cidx, [seg]);
    }

    for (const [cidx, segs] of changedByChunk.entries()) {
      const chunk = await this.getOrLoadChunk(cidx);
      const newChunk: T[][] = new Array<T[]>(chunkSize);
      for (let i = 0; i < chunkSize; i++) newChunk[i] = (chunk[i] ?? []) as T[];
      for (const seg of segs) {
        const idx = this.segmentIndexByRef.get(seg);
        if (idx === undefined) continue;
        const pos = idx % chunkSize;
        const leaf = this.findLeafBySegment(seg);
        if (!leaf) continue;
        await this.ensureLeafLoaded(leaf);
        newChunk[pos] = leaf.values.slice() as unknown as T[];
      }
      const key = this.generateChunkKey(cidx);
      await this.store.set<T[][]>(key, newChunk);
      this.meta.chunks[cidx] = key;
      this.chunkCache.set(cidx, newChunk);
      written.push(key);
    }

    this.dirty.clear();
    return written;
  }

  protected collectLeavesInOrder(): LeafNode<T, S>[] {
    const out: LeafNode<T, S>[] = [];
    let cur = this.head;
    while (cur) {
      out.push(cur);
      cur = cur.next;
    }
    return out;
  }

  protected findLeafBySegment(seg: S): LeafNode<T, S> | null {
    let cur = this.head;
    while (cur) {
      if (cur.seg === seg) return cur;
      cur = cur.next;
    }
    return null;
  }
}

/**
 * Indexed column via B+ tree skeleton.
 * - insertAt(index, value): single-leaf baseline; throws if capacity exceeded.
 * - get(index), range(min,max): single-leaf baseline.
 * - flush(): persists changed leaf blocks in x-MB (chunkCount) sized chunks.
 */
export class BPlusTreeIndexedColumn<T>
  extends BPlusTreeBase<T, BaseSegment<T>>
  implements IndexedColumnInterface<T>
{
  protected cmp(_a: T, _b: T): number | 0 {
    return 0;
  }

  // Keep leaf metadata up-to-date (no-op for indexed)
  protected override onLeafMutated(_leaf: LeafNode<T, BaseSegment<T>>): void {
    // no-op
  }

  async insertAt(index: number, value: T): Promise<void> {
    const clamp = (i: number, n: number) => Math.max(0, Math.min(i, n));

    // Helper: compute subtree size
    const subtreeCount = (cn: Node<T, BaseSegment<T>>): number =>
      cn.type === "leaf"
        ? cn.count | 0
        : (cn.sizes ?? []).reduce((s, v) => s + (v ?? 0), 0);

    // Helper: build internal node from children
    const buildInternal = (
      kids: Node<T, BaseSegment<T>>[],
    ): InternalNode<T, BaseSegment<T>> => {
      const sizes = kids.map((k) => subtreeCount(k));
      return { type: "internal", children: kids, sizes, keys: undefined };
    };

    // Helper: split overflowing internal node into two, return [left,right]
    const splitInternal = (
      n: InternalNode<T, BaseSegment<T>>,
    ): [InternalNode<T, BaseSegment<T>>, InternalNode<T, BaseSegment<T>>] => {
      const totalKids = n.children.length;
      const midIdx = totalKids >>> 1;
      const leftKids = n.children.slice(0, midIdx);
      const rightKids = n.children.slice(midIdx);
      const L = buildInternal(leftKids);
      const R = buildInternal(rightKids);
      return [L, R];
    };

    // Insert into empty tree
    if (!this.root) {
      const seg = { count: 1 } as BaseSegment<T>;
      const leaf: LeafNode<T, BaseSegment<T>> = {
        type: "leaf",
        seg,
        values: [value],
        count: 1,
        next: null,
        prev: null,
      };
      this.root = leaf;
      this.head = leaf;
      this.meta.segments = [seg];
      this.rebuildSegmentIndexMap();
      this.totalCount = 1;
      this.dirty.add(seg);
      this.structureChangedSinceFlush = true;
      return;
    }

    // Root is a leaf
    if (this.root.type === "leaf") {
      const leaf = this.root as LeafNode<T, BaseSegment<T>>;
      await this.ensureLeafLoaded(leaf);
      const i = clamp(index, leaf.count);
      leaf.values.splice(i, 0, value);
      leaf.count += 1;
      leaf.seg.count = leaf.count;
      this.totalCount += 1;
      this.dirty.add(leaf.seg);

      if (leaf.count <= this.meta.segmentCount) return;

      // Split leaf and create internal root
      const mid = leaf.values.length >>> 1;
      const leftVals = leaf.values.slice(0, mid) as T[];
      const rightVals = leaf.values.slice(mid) as T[];

      leaf.values = leftVals;
      leaf.count = leftVals.length;
      leaf.seg.count = leftVals.length;

      const rSeg = { count: rightVals.length } as BaseSegment<T>;
      const rightLeaf: LeafNode<T, BaseSegment<T>> = {
        type: "leaf",
        seg: rSeg,
        values: rightVals,
        count: rightVals.length,
        next: leaf.next,
        prev: leaf,
      };
      if (leaf.next) leaf.next.prev = rightLeaf;
      leaf.next = rightLeaf;

      const root: InternalNode<T, BaseSegment<T>> = buildInternal([
        leaf,
        rightLeaf,
      ]);
      this.root = root as Node<T, BaseSegment<T>>;
      this.head = leaf;
      this.dirty.add(rSeg);
      this.structureChangedSinceFlush = true;
      return;
    }

    // General case: internal root
    const clampIndex = clamp(index, this.totalCount);
    // Descend by sizes to find leaf and local offset, track path
    const path: Array<[InternalNode<T, BaseSegment<T>>, number]> = [];
    let cur: Node<T, BaseSegment<T>> = this.root;
    let i = clampIndex;
    while (cur.type === "internal") {
      let chosen = 0;
      for (let c = 0; c < cur.children.length; c++) {
        const sz = cur.sizes[c] ?? 0;
        if (i < sz) {
          chosen = c;
          break;
        }
        i -= sz;
      }
      path.push([cur, chosen]);
      cur = cur.children[chosen] as Node<T, BaseSegment<T>>;
    }
    const leaf = cur as LeafNode<T, BaseSegment<T>>;
    await this.ensureLeafLoaded(leaf);
    const local = i;
    leaf.values.splice(local, 0, value);
    leaf.count += 1;
    leaf.seg.count = leaf.count;
    this.dirty.add(leaf.seg);

    // Update sizes along the path (+1 in the chosen child)
    for (const [n, idx] of path) {
      n.sizes[idx] = (n.sizes[idx] ?? 0) + 1;
    }
    this.totalCount += 1;

    if (leaf.count <= this.meta.segmentCount) return;

    // Leaf split
    const mid = leaf.values.length >>> 1;
    const leftVals = leaf.values.slice(0, mid) as T[];
    const rightVals = leaf.values.slice(mid) as T[];

    leaf.values = leftVals;
    leaf.count = leftVals.length;
    leaf.seg.count = leftVals.length;

    const rSeg = { count: rightVals.length } as BaseSegment<T>;
    const rightLeaf: LeafNode<T, BaseSegment<T>> = {
      type: "leaf",
      seg: rSeg,
      values: rightVals,
      count: rightVals.length,
      next: leaf.next,
      prev: leaf,
    };
    if (leaf.next) leaf.next.prev = rightLeaf;
    leaf.next = rightLeaf;
    this.dirty.add(rSeg);
    this.structureChangedSinceFlush = true;

    // Insert rightLeaf into parent
    const [parent, childIdx] = path[path.length - 1]!;
    parent.children.splice(childIdx + 1, 0, rightLeaf);
    parent.sizes.splice(childIdx, 1, leaf.count);
    parent.sizes.splice(childIdx + 1, 0, rightLeaf.count);

    // Propagate internal splits if needed
    let curNode: InternalNode<T, BaseSegment<T>> = parent;
    let level = path.length - 1;
    while (curNode.children.length > this.order) {
      const [L, R] = splitInternal(curNode);
      if (level - 1 >= 0) {
        const [up, upIdx] = path[level - 1]!;
        // Replace overflowing child with L and insert R next
        up.children.splice(upIdx, 1, L);
        up.children.splice(upIdx + 1, 0, R);
        up.sizes.splice(upIdx, 1, subtreeCount(L));
        up.sizes.splice(upIdx + 1, 0, subtreeCount(R));
        curNode = up;
        level -= 1;
      } else {
        // Split root: create a new root
        const newRoot: InternalNode<T, BaseSegment<T>> = buildInternal([L, R]);
        this.root = newRoot as Node<T, BaseSegment<T>>;
        break;
      }
    }
  }

  async range(min: number, max?: number): Promise<T[]> {
    const out: T[] = [];
    if (!this.root || this.totalCount === 0) return out;
    const a = Math.max(0, Math.floor(min ?? 0));
    let b =
      max === undefined || !Number.isFinite(max)
        ? this.totalCount
        : Math.floor(max);
    if (b <= a) return out;
    if (b > this.totalCount) b = this.totalCount;

    // Descend by sizes to find starting leaf/local
    let cur: Node<T, BaseSegment<T>> = this.root;
    let i = a;
    while (cur.type === "internal") {
      let chosen = 0;
      for (let c = 0; c < cur.children.length; c++) {
        const sz = cur.sizes[c] ?? 0;
        if (i < sz) {
          chosen = c;
          break;
        }
        i -= sz;
      }
      cur = cur.children[chosen] as Node<T, BaseSegment<T>>;
    }
    let leaf = cur as LeafNode<T, BaseSegment<T>>;
    await this.ensureLeafLoaded(leaf);
    let need = b - a;
    let local = i;

    while (leaf && need > 0) {
      await this.ensureLeafLoaded(leaf);
      const take = Math.min(need, Math.max(0, leaf.values.length - local));
      if (take > 0) out.push(...leaf.values.slice(local, local + take));
      need -= take;
      leaf = leaf.next;
      local = 0;
    }

    return out;
  }

  async get(index: number): Promise<T | undefined> {
    return this.singleLeafGet(index);
  }

  async flush(): Promise<string[]> {
    return this.flushLeaves();
  }
}

/**
 * Ordered column via B+ tree skeleton.
 * - insert(value): lower_bound insert in a single leaf; throws on capacity overflow.
 * - getIndex(value): lower_bound index in a single leaf.
 * - scan(min,max): returns values v such that min <= v < max from a single leaf.
 * - range/get/flush/getMeta/setMeta: as expected.
 */
export class BPlusTreeOrderedColumn<T>
  extends BPlusTreeBase<T, OrderedSegment<T>>
  implements OrderedColumnInterface<T>
{
  constructor(
    store: IStore,
    meta: Partial<BPlusMeta<T, OrderedSegment<T>>>,
    private readonly comparator?: (a: T, b: T) => number,
  ) {
    super(store, meta);
  }

  protected cmp(a: T, b: T): number {
    if (this.comparator) return this.comparator(a, b);
    // Default comparator for number|string|bigint-like
    const av = a as unknown as number | string | bigint;
    const bv = b as unknown as number | string | bigint;
    return av < bv ? -1 : av > bv ? 1 : 0;
  }

  protected override onLeafMutated(leaf: LeafNode<T, OrderedSegment<T>>): void {
    if (leaf.count > 0) {
      const first = leaf.values[0] as T;
      const last = leaf.values[leaf.values.length - 1] as T;
      leaf.min = first;
      leaf.max = last;
      // persist into segment metadata as well
      (leaf.seg as OrderedSegment<T>).min = first;
      (leaf.seg as OrderedSegment<T>).max = last;
    } else {
      leaf.min = undefined;
      leaf.max = undefined;
    }
  }

  private lowerBoundInArray(arr: T[], value: T): number {
    let lo = 0;
    let hi = arr.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      const c = this.cmp(arr[mid] as T, value);
      if (c < 0) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  async insert(value: T): Promise<number> {
    // Helper: compute subtree max key
    const nodeMax = (n: Node<T, OrderedSegment<T>>): T => {
      if (n.type === "leaf") {
        const v = n.values[n.values.length - 1] as T;
        return v;
      }
      const last = n.children[n.children.length - 1] as Node<
        T,
        OrderedSegment<T>
      >;
      return nodeMax(last);
    };

    // Helper: choose child index in internal by separator keys (child max)
    const chooseChildIndex = (
      n: InternalNode<T, OrderedSegment<T>>,
      v: T,
    ): number => {
      const ks = n.keys ?? [];
      for (let i = 0; i < n.children.length; i++) {
        const k = ks[i];
        if (k === undefined) return Math.min(i, n.children.length - 1);
        if (this.cmp(k as T, v) >= 0) return i;
      }
      return n.children.length - 1;
    };

    // Helper: descend by value; returns path of [node, childIdx] and leaf
    const descend = (
      root: Node<T, OrderedSegment<T>>,
      v: T,
    ): {
      path: Array<[InternalNode<T, OrderedSegment<T>>, number]>;
      leaf: LeafNode<T, OrderedSegment<T>>;
    } => {
      const path: Array<[InternalNode<T, OrderedSegment<T>>, number]> = [];
      let cur = root;
      while (cur.type === "internal") {
        const idx = chooseChildIndex(cur, v);
        path.push([cur, idx]);
        cur = cur.children[idx] as Node<T, OrderedSegment<T>>;
      }
      return { path, leaf: cur };
    };

    // Helper: recompute keys for a given internal node around indexes
    const refreshKeysAround = (
      n: InternalNode<T, OrderedSegment<T>>,
      i: number,
    ): void => {
      if (!n.keys) n.keys = new Array<T>(n.children.length);
      for (
        let d = Math.max(0, i - 1);
        d <= Math.min(n.children.length - 1, i + 1);
        d++
      ) {
        n.keys[d] = nodeMax(n.children[d] as Node<T, OrderedSegment<T>>);
      }
    };

    if (!this.head) {
      // Create initial leaf/segment in empty tree
      const seg = { count: 1, min: value, max: value } as OrderedSegment<T>;
      const leaf: LeafNode<T, OrderedSegment<T>> = {
        type: "leaf",
        seg,
        values: [value],
        count: 1,
        next: null,
        prev: null,
        min: value,
        max: value,
      };
      this.root = leaf;
      this.head = leaf;
      this.meta.segments = [seg];
      this.rebuildSegmentIndexMap();
      this.totalCount = 1;
      this.dirty.add(seg);
      this.structureChangedSinceFlush = true;
      return 0;
    }

    // If root is leaf, either insert in-place or split and create a root internal
    if (this.root?.type === "leaf") {
      const leaf = this.root as LeafNode<T, OrderedSegment<T>>;
      await this.ensureLeafLoaded(leaf);
      const pos = this.lowerBoundInArray(leaf.values, value);
      leaf.values.splice(pos, 0, value);
      leaf.count += 1;
      leaf.seg.count = leaf.count;
      this.totalCount += 1;
      this.dirty.add(leaf.seg);
      this.onLeafMutated(leaf);

      if (leaf.count <= this.meta.segmentCount) {
        return pos;
      }

      // Split leaf and create internal root
      const mid = leaf.values.length >>> 1;
      const leftVals = leaf.values.slice(0, mid) as T[];
      const rightVals = leaf.values.slice(mid) as T[];

      // Left stays in place
      leaf.values = leftVals;
      leaf.count = leftVals.length;
      leaf.seg.count = leftVals.length;
      this.onLeafMutated(leaf);

      // Right is new leaf
      const rSeg = {
        count: rightVals.length,
        min: rightVals[0] as T,
        max: rightVals[rightVals.length - 1] as T,
      } as OrderedSegment<T>;
      const rightLeaf: LeafNode<T, OrderedSegment<T>> = {
        type: "leaf",
        seg: rSeg,
        values: rightVals,
        count: rightVals.length,
        next: leaf.next,
        prev: leaf,
        min: rightVals[0] as T,
        max: rightVals[rightVals.length - 1] as T,
      };
      if (leaf.next) leaf.next.prev = rightLeaf;
      leaf.next = rightLeaf;

      const root: InternalNode<T, OrderedSegment<T>> = {
        type: "internal",
        children: [leaf, rightLeaf],
        sizes: [leaf.count, rightLeaf.count],
        keys: [nodeMax(leaf), nodeMax(rightLeaf)],
      };
      this.root = root as Node<T, OrderedSegment<T>>;
      this.head = leaf;
      this.dirty.add(rSeg);
      this.structureChangedSinceFlush = true;
      // Return index within concatenation
      return pos;
    }

    // General case: internal root exists (height >= 2)
    const root = this.root as Node<T, OrderedSegment<T>>;
    const { path, leaf } = descend(root, value);
    await this.ensureLeafLoaded(leaf);
    const posLocal = this.lowerBoundInArray(leaf.values, value);
    leaf.values.splice(posLocal, 0, value);
    leaf.count += 1;
    leaf.seg.count = leaf.count;
    this.dirty.add(leaf.seg);
    this.onLeafMutated(leaf);

    // Update sizes along the path (+1 in the chosen child)
    for (const [n, idx] of path) {
      n.sizes[idx] = (n.sizes[idx] ?? 0) + 1;
      refreshKeysAround(n, idx);
    }
    this.totalCount += 1;

    if (leaf.count <= this.meta.segmentCount) {
      // Global index = sum over path of sizes of children before chosen child + local
      let globalBefore = 0;
      for (const [n, idx] of path) {
        for (let i = 0; i < idx; i++) globalBefore += n.sizes[i] ?? 0;
      }
      return globalBefore + posLocal;
    }

    // Split leaf and insert new right leaf into parent
    const mid = leaf.values.length >>> 1;
    const leftVals = leaf.values.slice(0, mid) as T[];
    const rightVals = leaf.values.slice(mid) as T[];

    // Left leaf updates
    leaf.values = leftVals;
    leaf.count = leftVals.length;
    leaf.seg.count = leftVals.length;
    this.onLeafMutated(leaf);

    // Create right leaf
    const rSeg = {
      count: rightVals.length,
      min: rightVals[0] as T,
      max: rightVals[rightVals.length - 1] as T,
    } as OrderedSegment<T>;
    const rightLeaf: LeafNode<T, OrderedSegment<T>> = {
      type: "leaf",
      seg: rSeg,
      values: rightVals,
      count: rightVals.length,
      next: leaf.next,
      prev: leaf,
      min: rightVals[0] as T,
      max: rightVals[rightVals.length - 1] as T,
    };
    if (leaf.next) leaf.next.prev = rightLeaf;
    leaf.next = rightLeaf;
    this.dirty.add(rSeg);
    this.structureChangedSinceFlush = true;

    // Insert into parent internal
    const [parent, childIdx] = path[path.length - 1]!;
    parent.children.splice(childIdx + 1, 0, rightLeaf);
    parent.sizes.splice(childIdx, 1, leaf.count);
    parent.sizes.splice(childIdx + 1, 0, rightLeaf.count);
    if (!parent.keys) parent.keys = new Array<T>(parent.children.length);
    refreshKeysAround(parent, childIdx);
    refreshKeysAround(parent, childIdx + 1);

    // If parent exceeds order, split and propagate upward (create new root if needed)
    if (parent.children.length > this.order) {
      const subtreeCount = (cn: Node<T, OrderedSegment<T>>): number =>
        cn.type === "leaf"
          ? cn.count | 0
          : (cn.sizes ?? []).reduce((s, v) => s + (v ?? 0), 0);

      const buildInternal = (
        kids: Node<T, OrderedSegment<T>>[],
      ): InternalNode<T, OrderedSegment<T>> => {
        const sizes = kids.map((k) => subtreeCount(k));
        const keys = kids.map((k) => nodeMax(k));
        return { type: "internal", children: kids, sizes, keys };
      };

      // current overflow node to split and index in its parent (path includes this parent at the end)
      let curNode = parent;
      let atLevel = path.length - 1;

      while (curNode.children.length > this.order) {
        const totalKids = curNode.children.length;
        const midIdx = totalKids >>> 1;
        const leftKids = curNode.children.slice(0, midIdx);
        const rightKids = curNode.children.slice(midIdx);
        const leftNode = buildInternal(leftKids);
        const rightNode = buildInternal(rightKids);

        if (atLevel - 1 >= 0) {
          // insert split result into the upper parent
          const [up, upIdx] = path[atLevel - 1]!;
          // replace the overflowing child with leftNode and insert rightNode after it
          up.children.splice(upIdx, 1, leftNode);
          up.children.splice(upIdx + 1, 0, rightNode);
          // recompute sizes for the affected positions
          up.sizes.splice(upIdx, 1, subtreeCount(leftNode));
          up.sizes.splice(upIdx + 1, 0, subtreeCount(rightNode));
          if (!up.keys) up.keys = new Array<T>(up.children.length);
          refreshKeysAround(up, upIdx);
          refreshKeysAround(up, upIdx + 1);

          // move up to continue checking for overflow
          curNode = up;
          atLevel -= 1;
        } else {
          // split root: create a new root with the two nodes
          const newRoot: InternalNode<T, OrderedSegment<T>> = {
            type: "internal",
            children: [leftNode, rightNode],
            sizes: [subtreeCount(leftNode), subtreeCount(rightNode)],
            keys: [nodeMax(leftNode), nodeMax(rightNode)],
          };
          this.root = newRoot as Node<T, OrderedSegment<T>>;
          break;
        }
      }
    }

    // Compute global index for return
    let globalBefore = 0;
    for (let p = 0; p < path.length - 1; p++) {
      const [n, idx] = path[p]!;
      for (let i = 0; i < idx; i++) globalBefore += n.sizes[i] ?? 0;
    }
    // For the parent level, decide which leaf now contains the inserted value
    const inRight = posLocal >= leftVals.length;
    const localInFinal = inRight ? posLocal - leftVals.length : posLocal;
    const idxAtParent = inRight ? childIdx + 1 : childIdx;
    for (let i = 0; i < idxAtParent; i++) globalBefore += parent.sizes[i] ?? 0;

    return globalBefore + localInFinal;
  }

  async range(minIndex: number, maxIndex?: number): Promise<T[]> {
    const out: T[] = [];
    const a = Math.max(0, Math.floor(minIndex ?? 0));
    let b = maxIndex === undefined ? this.totalCount : Math.floor(maxIndex);
    if (!this.root || a >= b) return out;
    if (b > this.totalCount) b = this.totalCount;

    // Helper: descend by index using sizes to find leaf and local offset
    const findByIndex = (
      root: Node<T, OrderedSegment<T>>,
      idx: number,
    ): { leaf: LeafNode<T, OrderedSegment<T>>; local: number } => {
      let cur = root;
      let i = idx;
      while (cur.type === "internal") {
        let chosen = 0;
        for (let c = 0; c < cur.children.length; c++) {
          const sz = cur.sizes[c] ?? 0;
          if (i < sz) {
            chosen = c;
            break;
          }
          i -= sz;
        }
        cur = cur.children[chosen] as Node<T, OrderedSegment<T>>;
      }
      return { leaf: cur, local: i };
    };

    const { leaf: startLeaf, local: startLocal } = findByIndex(this.root, a);
    await this.ensureLeafLoaded(startLeaf);
    let need = b - a;
    let curLeaf: LeafNode<T, OrderedSegment<T>> | null = startLeaf;
    let local = startLocal;

    while (curLeaf && need > 0) {
      await this.ensureLeafLoaded(curLeaf);
      const take = Math.min(need, Math.max(0, curLeaf.values.length - local));
      if (take > 0) out.push(...curLeaf.values.slice(local, local + take));
      need -= take;
      curLeaf = curLeaf.next;
      local = 0;
    }

    return out;
  }

  async scan(min: T, max: T): Promise<T[]> {
    const out: T[] = [];
    if (!this.root) return out;

    // helper: choose child by keys
    const chooseChildIndex = (
      n: InternalNode<T, OrderedSegment<T>>,
      v: T,
    ): number => {
      const ks = n.keys ?? [];
      for (let i = 0; i < n.children.length; i++) {
        const k = ks[i];
        if (k === undefined) return Math.min(i, n.children.length - 1);
        if (this.cmp(k as T, v) >= 0) return i;
      }
      return n.children.length - 1;
    };

    // descend to first leaf that could contain min
    let cur: Node<T, OrderedSegment<T>> = this.root;
    while (cur.type === "internal") {
      const idx = chooseChildIndex(cur, min);
      cur = cur.children[idx] as Node<T, OrderedSegment<T>>;
    }
    let leaf = cur as LeafNode<T, OrderedSegment<T>>;

    // iterate leaves while min of leaf is < max (respect [min,max) semantics)
    while (leaf) {
      await this.ensureLeafLoaded(leaf);
      if (leaf.min !== undefined && this.cmp(leaf.min as T, max) >= 0) break;

      const start = this.lowerBoundInArray(leaf.values, min);
      const end = this.lowerBoundInArray(leaf.values, max); // [min, max)
      if (start < end) out.push(...leaf.values.slice(start, end));
      // if we ended inside this leaf (end < length), we can stop
      if (end < leaf.values.length) break;

      leaf = leaf.next as LeafNode<T, OrderedSegment<T>> | null;
    }

    return out;
  }

  async get(index: number): Promise<T | undefined> {
    return this.singleLeafGet(index);
  }

  async getIndex(value: T): Promise<number> {
    if (!this.root) return 0;

    // Helper: choose child index
    const chooseChildIndex = (
      n: InternalNode<T, OrderedSegment<T>>,
      v: T,
    ): number => {
      const ks = n.keys ?? [];
      for (let i = 0; i < n.children.length; i++) {
        const k = ks[i];
        if (k === undefined) return Math.min(i, n.children.length - 1);
        if (this.cmp(k as T, v) >= 0) return i;
      }
      return n.children.length - 1;
    };

    let cur: Node<T, OrderedSegment<T>> = this.root;
    let before = 0;
    while (cur.type === "internal") {
      const idx = chooseChildIndex(cur, value);
      for (let i = 0; i < idx; i++) before += cur.sizes[i] ?? 0;
      cur = cur.children[idx] as Node<T, OrderedSegment<T>>;
    }

    const leaf = cur as LeafNode<T, OrderedSegment<T>>;
    await this.ensureLeafLoaded(leaf);
    const local = this.lowerBoundInArray(leaf.values, value);
    return before + local;
  }

  async flush(): Promise<string[]> {
    return this.flushLeaves();
  }
}

export default {
  BPlusTreeIndexedColumn,
  BPlusTreeOrderedColumn,
};
