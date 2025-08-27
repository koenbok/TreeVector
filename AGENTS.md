# Repository Guidelines

## Project Structure & Module Organization
- `src/`: TypeScript source and colocated tests (`*.test.ts`). Core modules: `FenwickBase.ts`, `FenwickList.ts`, `FenwickOrderedList.ts`, `Column.ts`, `Table.ts`, `Chunks.ts`, `ChunkingStore.ts`, `Store.ts`.
- `bench/`: Runnable benchmarks (e.g., `bench/columns.ts`, `bench/ordered-list.ts`, `bench/table.ts`).
- No separate build output; Bun runs TS directly.

## Build, Test, and Development Commands
- Install deps: `bun install`
- Run all tests: `bun test`
- Run a single test: `bun test src/Column.test.ts`
- Run benchmarks (examples):
  - Ordered list: `bun run bench/ordered-list.ts 1000000 8192`
  - Columns sweep: `bun run bench/columns.ts 300000 8192 16 0.05`

## Coding Style & Naming Conventions
- Language: TypeScript (ESNext, ESM). Strict typing enabled; no emit.
- Indentation: 2 spaces; max line length by judgment (keep readable).
- Files: PascalCase for modules (e.g., `FenwickOrderedList.ts`); tests end with `.test.ts` next to the module.
- Names: classes/types/interfaces in PascalCase; variables/functions in camelCase.
- Imports: ESM with relative paths (no extension in `src` imports), prefer named exports.

## Testing Guidelines
- Framework: `bun:test` (`describe`, `it`, `expect`).
- Colocate tests in `src/` as `*.test.ts` mirroring the module name.
- Focus on correctness and performance invariants (ordering, range `[min,max)`, no waterfall loading, incremental Fenwick rebuilds).
- Run: `bun test` (optionally `bun test src/FenwickOrderedList.test.ts`).

## Commit & Pull Request Guidelines
- Commits: use Conventional Commit style with scopes seen in history, e.g. `feat(bench): ...`, `fix(fenwick): ...`, `perf: ...`, `refactor: ...`.
- PRs should include:
  - Clear summary and motivation; link related issues.
  - Tests for new behavior or bug fixes; update existing tests if semantics change.
  - Benchmark output for performance‑related changes (paste relevant `bench/` results).
  - Notes on API/semantics (e.g., scan/range are half‑open `[min, max)`).

## Architecture Overview
- Fenwick structures (`FenwickBase`, `FenwickList`, `FenwickOrderedList`) back columns and tables.
- Storage abstractions (`Store`, `ChunkingStore`, `Chunks`) handle segment/chunk IO and caching.
- Tables/Columns compose the primitives for indexed and ordered access.

