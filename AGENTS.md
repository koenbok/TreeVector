# AGENTS: How to Work on This Repo

Keep answers short and to the point.

## Quick Commands
- Install: `bun install`
- Test all: `bun test`
- Test one: `bun test src/Column.test.ts`
- Bench (examples):
  - `bun run bench/ordered-list.ts 1000000 8192`
  - `bun run bench/columns.ts 300000 8192 16 0.05`

## Code & Project Conventions
- Language: TypeScript (ESNext, ESM), strict types, no emit.
- Files: PascalCase modules in `src/`; tests colocated as `*.test.ts`.
- Names: PascalCase for classes/types; camelCase for vars/functions.
- Imports: ESM, relative paths, no extensions for `src` imports.
- Indent: 2 spaces; keep lines readable.

## Working Style (for agents)
- Be concise: brief updates, group related actions.
- Add a short preamble before running tool/terminal actions.
- Plan when multi‑step: outline 3–6 short steps and update as you go.
- Share progress: occasional one‑liners on what’s done/next.
- Prefer bullets; avoid heavy formatting. Use backticks for commands/paths.
- File references: `path[:line]` (e.g., `src/Table.ts:42`). No URLs/citations.
- Search fast: use `rg` (ripgrep). Read files in chunks ≤250 lines.

## Editing & Changes
- Make minimal, surgical diffs; keep style consistent with nearby code.
- Update or add colocated tests when semantics change.
- Don’t fix unrelated issues or reformat whole files.
- Don’t add license headers. Don’t rename files unless required.
- Use Conventional Commits if asked to commit (e.g., `fix(fenwick): ...`).

## Testing & Benchmarks
- Run tests: `bun test`; target a file for focused checks.
- Invariants to protect:
  - Ordering correctness and stable scans.
  - Half‑open ranges `[min, max)` across APIs.
  - No waterfall IO (load only touched segments/chunks).
  - Incremental Fenwick updates/rebuilds; copy‑on‑write chunk rotation.
- Bench when performance‑adjacent and paste results with parameters used.

## Architecture Snapshot
- Fenwick: `FenwickBase`, `FenwickList`, `FenwickOrderedList` underpin columns/tables.
- Storage: `Store` (plus chunking/caching behavior tested in `Chunks.test.ts`).
- Composition: `Column` and `Table` provide indexed and ordered access.

## PR Expectations
- Summary and motivation; link related issues.
- Tests for new behavior; update existing ones if semantics change.
- Benchmark output for perf changes (include command/params and gist numbers).
- Note API/semantic impacts (esp. range/scan contracts).

## Message Formatting (chat agents)
- Use short section headers only when helpful.
- Bullets with bold keywords are preferred for clarity.
- Wrap commands, file paths, and identifiers in backticks.
- Keep responses self‑contained; no external citations or ANSI codes.

## Gotchas
- Respect ESM import style (no extensions in `src` imports).
- Keep modules in `src/`; colocate tests next to implementations.
- Avoid destructive shell actions (e.g., `rm -rf`, history rewrites).
- Don’t introduce new tooling/config unless explicitly requested.
