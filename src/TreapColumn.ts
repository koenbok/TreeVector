import type { IndexedColumnInterface } from "./Column";
import type { IStore } from "./Store";
import type { FenwickBaseMeta, BaseSegment } from "./FenwickBase";

/**
 * Treap-based indexed column with block-sized leaf nodes and lazy chunked persistence.
 *
 * Design:
 * - Implicit treap where each node is a leaf holding an in-memory array (block) of values.
 * - Each leaf corresponds 1:1 to a "segment" object stored in meta.segments, which carries the count and provides
 *   a stable identity for persistence.
 * - The treap preserves in-order concatenation of leaves. Rotations never change the in-order order, only the shape.
 * - We support:
 *    - insertAt(i, v): find the leaf containing global index i, insert locally; split leaf if capacity exceeded.
 *    - get(i): locate leaf by subtree sizes and return arr[localIndex].
 *    - range(min, max): collect slices across consecutive leaves.
 *    - flush(): copy-on-write chunk persistence. If the segment layout changed since last flush, we rewrite all chunks
 *               to keep chunk boundaries aligned with the new segment order.
 *
 * Persistence layout:
 * - meta.segmentCount: maximum values per leaf (block).
 * - meta.chunkCount: number of segments per chunk (>=1). A chunk persists an array T[][] of length chunkCount.
 * - meta.segments: ordered list of segment descriptors (each leaf corresponds to one segment).
 * - meta.chunks: store keys for each chunk index.
 */

type Segment<T> = BaseSegment<T>;
type TreapMeta<T> = FenwickBaseMeta<T, Segment<T>>;

function defaults<T>(meta: Partial<TreapMeta<T>>): TreapMeta<T> {
  return {
    segmentCount: 1024,
    chunkCount: 128,
    segments: [],
    chunks: [],
    ...meta,
  } as TreapMeta<T>;
}

class Node<T> {
  left: Node<T> | null = null;
  right: Node<T> | null = null;
  // Number of values in the subtree rooted at this node.
  size = 0;
  // Priority for treap heap property (min-heap on priority).
  prio: number;
  // Backing segment meta for persistence and counts.
  seg: Segment<T>;
  // In-memory authoritative array (loaded lazily)
  arr: T[] | null = null;

  constructor(seg: Segment<T>, prio?: number) {
    this.seg = seg;
    this.prio = prio ?? Math.random();
    this.size = seg.count | 0;
  }
}

export class TreapColumn<T> implements IndexedColumnInterface<T> {
  private meta: TreapMeta<T>;
  private root: Node<T> | null = null;
  private totalCount = 0;

  private segToNode = new Map<Segment<T>, Node<T>>();
  private segmentIndexByRef = new Map<Segment<T>, number>();

  // persistence helpers
  private chunkCache = new Map<number, T[][]>();
  private dirty = new Set<Segment<T>>();
  private structureChangedSinceFlush = false;

  constructor(
    private readonly store: IStore,
    meta: Partial<TreapMeta<T>>,
  ) {
    this.meta = defaults(meta);
    this.rebuildFromMeta();
  }

  // ---- IndexedColumnInterface ----

  async insertAt(index: number, value: T): Promise<void> {
    // clamp index
    const i = Math.max(0, Math.min(index, this.totalCount));
    if (!this.root) {
      // create initial leaf/segment
      const seg: Segment<T> = { count: 1 };
      const n = new Node<T>(seg);
      n.arr = [value];
      n.size = 1;
      this.root = n;
      this.meta.segments.push(seg);
      this.rebuildSegmentIndexMap();
      this.segToNode.set(seg, n);
      this.totalCount = 1;
      this.dirty.add(seg);
      this.structureChangedSinceFlush = true;
      return;
    }

    if (i === this.totalCount) {
      // Append path: split at end to get [L, null], then try to append into rightmost leaf of L.
      const [L, R] = this.split(this.root, this.totalCount);
      const last = this.rightmost(L);
      if (last) {
        await this.ensureLeafLoaded(last);
        if (last.seg.count < this.meta.segmentCount) {
          last.arr!.push(value);
          last.seg.count += 1;
          this.fixSizeUp(last);
          this.totalCount += 1;
          this.root = this.merge(L, R);
          this.dirty.add(last.seg);
          return;
        }
      }
      // Need a fresh leaf
      const newSeg: Segment<T> = { count: 1 };
      const newLeaf = new Node<T>(newSeg);
      newLeaf.arr = [value];
      newLeaf.size = 1;
      // merge back: L + newLeaf + R
      const LR = this.merge(L, newLeaf);
      this.root = this.merge(LR, R);
      // Update meta ordering: append new segment at end
      this.meta.segments.push(newSeg);
      this.rebuildSegmentIndexMap();
      this.segToNode.set(newSeg, newLeaf);
      this.totalCount += 1;
      this.dirty.add(newSeg);
      this.structureChangedSinceFlush = true;
      return;
    }

    // General case: find leaf containing index i
    const found = this.findLeafByIndex(this.root, i);
    if (!found) {
      // Fallback to append if something odd happened
      return this.insertAt(this.totalCount, value);
    }
    const { node, before, local } = found;

    // Split tree into [A | node | C] by boundaries
    const [A, BC] = this.split(this.root, before);
    const [B, C] = this.split(BC, node.seg.count); // isolates node in B

    // Operate on node
    await this.ensureLeafLoaded(node);
    node.arr!.splice(local, 0, value);
    node.seg.count += 1;
    this.totalCount += 1;
    this.dirty.add(node.seg);

    let mid: Node<T>;
    if (node.seg.count > this.meta.segmentCount) {
      // Split the leaf into two balanced halves
      const arr = node.arr as T[];
      const midIdx = arr.length >>> 1;
      const leftArr = arr.slice(0, midIdx) as T[];
      const rightArr = arr.slice(midIdx) as T[];
      node.arr = leftArr;
      node.seg.count = leftArr.length;
      node.size = leftArr.length + this.size(node.left) + this.size(node.right);

      const newSeg: Segment<T> = { count: rightArr.length };
      const rightNode = new Node<T>(newSeg);
      rightNode.arr = rightArr;
      rightNode.size = rightArr.length;

      // Mid = node + rightNode
      mid = this.merge(node, rightNode)!;

      // Update meta.segments: insert newSeg immediately after node.seg
      const idx = this.segmentIndexByRef.get(node.seg);
      const insertPos = (idx ?? this.meta.segments.length - 1) + 1;
      this.meta.segments.splice(insertPos, 0, newSeg);
      this.rebuildSegmentIndexMap();
      this.segToNode.set(newSeg, rightNode);
      this.dirty.add(newSeg);
      this.structureChangedSinceFlush = true;
    } else {
      mid = node;
    }

    // Reassemble root
    const AB = this.merge(A, mid);
    this.root = this.merge(AB, C);
  }

  async range(min: number, max?: number): Promise<T[]> {
    const out: T[] = [];
    if (this.totalCount === 0) return out;
    const a = Math.max(0, Math.floor(min ?? 0));
    let b =
      max === undefined || !Number.isFinite(max)
        ? this.totalCount
        : Math.floor(max);
    if (b <= a) return out;
    if (a >= this.totalCount) return out;
    if (b > this.totalCount) b = this.totalCount;

    let cur = a;
    while (cur < b) {
      const found = this.findLeafByIndex(this.root, cur);
      if (!found) break;
      const { node, local } = found;
      await this.ensureLeafLoaded(node);
      const arr = node.arr as T[];
      const take = Math.min(arr.length - local, b - cur);
      if (take > 0) out.push(...arr.slice(local, local + take));
      cur += take;
    }
    return out;
  }

  async get(index: number): Promise<T | undefined> {
    if (index < 0 || index >= this.totalCount) return undefined;
    const found = this.findLeafByIndex(this.root, index);
    if (!found) return undefined;
    await this.ensureLeafLoaded(found.node);
    return (found.node.arr as T[])[found.local] as T;
  }

  async flush(): Promise<string[]> {
    const written: string[] = [];
    const chunkSize = this.effectiveChunkSize();
    if (!this.root) return written;

    if (this.structureChangedSinceFlush) {
      // Full rewrite to keep chunk boundaries aligned with updated segment order.
      const chunksNeeded = Math.ceil(this.meta.segments.length / chunkSize);
      for (let cidx = 0; cidx < chunksNeeded; cidx++) {
        const newChunk: T[][] = new Array<T[]>(chunkSize);
        for (let pos = 0; pos < chunkSize; pos++) {
          const segIndex = cidx * chunkSize + pos;
          if (segIndex >= this.meta.segments.length) {
            newChunk[pos] = [] as T[];
            continue;
          }
          const seg = this.meta.segments[segIndex] as Segment<T>;
          const node = this.segToNode.get(seg);
          if (!node) {
            newChunk[pos] = [] as T[];
            continue;
          }
          await this.ensureLeafLoaded(node);
          newChunk[pos] = (node.arr as T[]).slice() as T[];
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

    // Group dirty segments by chunk index
    const changedByChunk = new Map<number, Segment<T>[]>();
    for (const seg of this.dirty) {
      const idx = this.segmentIndexByRef.get(seg);
      if (idx === undefined || idx < 0) continue;
      const cidx = Math.floor(idx / chunkSize);
      const list = changedByChunk.get(cidx);
      if (list) list.push(seg);
      else changedByChunk.set(cidx, [seg]);
    }

    for (const [cidx, segs] of changedByChunk.entries()) {
      const chunk = await this.getOrLoadChunk(cidx);
      // copy current chunk
      const newChunk: T[][] = new Array<T[]>(chunkSize);
      for (let i = 0; i < chunkSize; i++) newChunk[i] = (chunk[i] ?? []) as T[];
      // override dirty segments
      for (const seg of segs) {
        const idx = this.segmentIndexByRef.get(seg) as number;
        const pos = idx % chunkSize;
        const node = this.segToNode.get(seg);
        if (!node) continue;
        await this.ensureLeafLoaded(node);
        newChunk[pos] = (node.arr as T[]).slice() as T[];
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

  getMeta(): TreapMeta<T> {
    return this.meta;
  }

  setMeta(meta: TreapMeta<T>): void {
    this.meta = defaults(meta);
    this.rebuildFromMeta();
  }

  // ---- Treap primitives ----

  private size(n: Node<T> | null): number {
    return n ? n.size : 0;
  }

  private recalc(n: Node<T> | null): void {
    if (!n) return;
    n.size = this.size(n.left) + (n.seg.count | 0) + this.size(n.right);
  }

  // When we mutate a leaf's arr/count, we must ensure its size is up-to-date while keeping subtree sizes sound.
  // We rely on split/merge reassembly to propagate recalc to parents. For nodes modified without reassembly (append path),
  // we call fixSizeUp by rebuilding root via a merge with nulls to trigger recalc through recursion (cheap).
  private fixSizeUp(n: Node<T>): void {
    n.size = this.size(n.left) + (n.seg.count | 0) + this.size(n.right);
    this.recomputeAllSizes();
  }

  private recomputeAllSizes(): void {
    const dfs = (x: Node<T> | null): number => {
      if (!x) return 0;
      const l = dfs(x.left);
      const r = dfs(x.right);
      x.size = l + (x.seg.count | 0) + r;
      return x.size;
    };
    dfs(this.root);
  }

  private merge(a: Node<T> | null, b: Node<T> | null): Node<T> | null {
    if (!a) return b;
    if (!b) return a;
    if (a.prio <= b.prio) {
      a.right = this.merge(a.right, b);
      this.recalc(a);
      return a;
    } else {
      b.left = this.merge(a, b.left);
      this.recalc(b);
      return b;
    }
  }

  // Split by first k elements (0..size). This implementation assumes k falls on leaf boundaries in normal operation.
  private split(
    root: Node<T> | null,
    k: number,
  ): [Node<T> | null, Node<T> | null] {
    if (!root) return [null, null];
    const leftSize = this.size(root.left);
    const selfSize = root.seg.count | 0;
    if (k < leftSize) {
      const [l, newLeft] = this.split(root.left, k);
      root.left = newLeft;
      this.recalc(root);
      return [l, root];
    } else if (k > leftSize + selfSize) {
      const [newRight, r] = this.split(root.right, k - leftSize - selfSize);
      root.right = newRight;
      this.recalc(root);
      return [root, r];
    } else if (k === leftSize) {
      // Entire node goes to right side
      const L = root.left;
      root.left = null;
      this.recalc(root);
      return [L, root];
    } else if (k === leftSize + selfSize) {
      // Entire node goes to left side
      const R = root.right;
      root.right = null;
      this.recalc(root);
      return [root, R];
    } else {
      // k falls inside this leaf. We support splitting the leaf by loading it and creating a new segment.
      // This path should be rare; we prefer boundary splits in our operations.
      const offset = k - leftSize;
      // Create two leaves by splitting this node's array at 'offset'
      const arr = this.ensureArrSync(root);
      const leftArr = arr.slice(0, offset) as T[];
      const rightArr = arr.slice(offset) as T[];

      // Left part stays in 'root'
      root.arr = leftArr;
      root.seg.count = leftArr.length;
      this.recalc(root);

      // Right part goes into a new node/segment
      const newSeg: Segment<T> = { count: rightArr.length };
      const rightNode = new Node<T>(newSeg);
      rightNode.arr = rightArr;
      rightNode.size = rightArr.length;

      // Insert newSeg in meta right after root.seg
      const idx = this.segmentIndexByRef.get(root.seg);
      const insertPos = (idx ?? this.meta.segments.length - 1) + 1;
      this.meta.segments.splice(insertPos, 0, newSeg);
      this.rebuildSegmentIndexMap();
      this.segToNode.set(newSeg, rightNode);
      this.dirty.add(newSeg);
      this.structureChangedSinceFlush = true;

      // Attach original right subtree to the rightNode chain
      rightNode.right = root.right;
      root.right = null;

      // Return [LeftTree, RightTree] = [root, rightNode subtree]
      return [root, rightNode];
    }
  }

  private rightmost(n: Node<T> | null): Node<T> | null {
    if (!n) return null;
    let cur: Node<T> | null = n;
    while (cur?.right) cur = cur.right;
    return cur;
  }

  private leftmost(n: Node<T> | null): Node<T> | null {
    if (!n) return null;
    let cur: Node<T> | null = n;
    while (cur?.left) cur = cur.left;
    return cur;
  }

  // Find leaf containing global index i, returning node reference, number of elements before the node, and local index.
  private findLeafByIndex(
    root: Node<T> | null,
    i: number,
  ): { node: Node<T>; before: number; local: number } | null {
    let cur = root;
    let acc = 0;
    while (cur) {
      const leftSize = this.size(cur.left);
      if (i < acc + leftSize) {
        cur = cur.left;
        continue;
      }
      const start = acc + leftSize;
      const end = start + (cur.seg.count | 0);
      if (i < end) {
        return { node: cur, before: start, local: i - start };
      }
      acc = end;
      cur = cur.right;
    }
    return null;
  }

  // ---- Loading & persistence helpers ----

  private async ensureLeafLoaded(node: Node<T>): Promise<void> {
    if (node.arr) {
      // Sync seg.count if drift exists
      if (node.arr.length !== (node.seg.count | 0)) {
        node.seg.count = node.arr.length;
        this.recalc(node);
      }
      return;
    }
    const arr = await this.loadSegmentArray(node.seg);
    node.arr = arr.slice() as T[];
    node.seg.count = node.arr.length;
    this.recalc(node);
  }

  private ensureArrSync(node: Node<T>): T[] {
    if (node.arr) return node.arr as T[];
    // Try lazy fetch from chunk cache/store synchronously unsafe; fallback to empty.
    // For mid-leaf split paths, we assume the array is available or we tolerate empty.
    const idx = this.segmentIndexByRef.get(node.seg);
    if (idx !== undefined) {
      const cidx = Math.floor(idx / this.effectiveChunkSize());
      const pos = idx % this.effectiveChunkSize();
      const chunk = this.chunkCache.get(cidx);
      if (chunk?.[pos]) {
        node.arr = (chunk[pos] as T[]).slice() as T[];
        return node.arr;
      }
    }
    node.arr = new Array<T>(node.seg.count);
    // The content is unknown here; callers should avoid this path for reads.
    return node.arr as T[];
  }

  private effectiveChunkSize(): number {
    return this.meta.chunkCount > 0 ? this.meta.chunkCount : 1;
  }

  private async loadSegmentArray(seg: Segment<T>): Promise<T[]> {
    const idx = this.segmentIndexByRef.get(seg);
    if (idx === undefined || idx < 0) return [];
    const chunkSize = this.effectiveChunkSize();
    const cidx = Math.floor(idx / chunkSize);
    const pos = idx % chunkSize;
    const chunk = await this.getOrLoadChunk(cidx);
    return (chunk[pos] ?? []) as T[];
  }

  private async getOrLoadChunk(cidx: number): Promise<T[][]> {
    const cached = this.chunkCache.get(cidx);
    if (cached) return cached as T[][];
    const chunkSize = this.effectiveChunkSize();
    const key = this.meta.chunks[cidx];
    let chunk = (key ? await this.store.get<T[][]>(key) : undefined) ?? [];
    if (!Array.isArray(chunk)) chunk = [];
    if (chunk.length < chunkSize) {
      const augmented = new Array<T[]>(chunkSize);
      for (let i = 0; i < chunkSize; i++)
        augmented[i] = (chunk[i] ?? []) as T[];
      chunk = augmented;
    }
    this.chunkCache.set(cidx, chunk as T[][]);
    return chunk as T[][];
  }

  private generateChunkKey(cidx: number): string {
    const suffix = `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
    return `treap_chunk_${cidx}_${suffix}`;
  }

  // ---- Meta rebuild helpers ----

  private rebuildFromMeta(): void {
    // Clear caches
    this.root = null;
    this.segToNode.clear();
    this.segmentIndexByRef.clear();
    this.chunkCache.clear();
    this.dirty.clear();
    this.structureChangedSinceFlush = false;

    // Build treap in-order from existing segments.
    this.rebuildSegmentIndexMap();
    this.totalCount = this.meta.segments.reduce(
      (sum, s) => sum + ((s?.count ?? 0) | 0),
      0,
    );
    const nodes: Node<T>[] = [];
    for (const seg of this.meta.segments) {
      const n = new Node<T>(seg);
      n.size = seg.count | 0;
      nodes.push(n);
      this.segToNode.set(seg, n);
    }
    this.root = this.buildTreapFromInorder(nodes);
  }

  private rebuildSegmentIndexMap(): void {
    this.segmentIndexByRef.clear();
    for (let i = 0; i < this.meta.segments.length; i++) {
      this.segmentIndexByRef.set(this.meta.segments[i] as Segment<T>, i);
    }
  }

  // Linear-time Cartesian tree construction for treap given in-order nodes and random priorities.
  private buildTreapFromInorder(nodes: Node<T>[]): Node<T> | null {
    if (nodes.length === 0) return null;
    // Assign random priorities (already set), use a stack-based Cartesian tree build
    const stack: Node<T>[] = [];
    for (const n of nodes) {
      n.left = n.right = null;
      // maintain increasing priorities on stack
      let last: Node<T> | null = null;
      while (stack.length > 0 && stack[stack.length - 1]!.prio > n.prio) {
        last = stack.pop() as Node<T>;
      }
      n.left = last;
      if (stack.length > 0) {
        stack[stack.length - 1]!.right = n;
      }
      stack.push(n);
    }
    // bottom of the stack is the root after re-linking
    let root = stack[0] as Node<T>;
    // Recompute sizes bottom-up via a post-order traversal
    const recalcPost = (x: Node<T> | null) => {
      if (!x) return 0;
      const l = recalcPost(x.left);
      const r = recalcPost(x.right);
      x.size = l + (x.seg.count | 0) + r;
      return x.size;
    };
    recalcPost(root);
    return root;
  }
}

export default TreapColumn;
