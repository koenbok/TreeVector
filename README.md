# treevector

To install dependencies:

```bash
bun install
```

To run:

```bash
bun run src/index.ts
```

To run the HTTP server:

```bash
bun run src/server/index.ts
```

This project was created using `bun init` in bun v1.2.19. [Bun](https://bun.com) is a fast all-in-one JavaScript runtime.

## Semantics

- `FenwickOrderedList.scan(min, max)` returns values in the half-open interval `[min, max)`: includes `min`, excludes `max`.
- `FenwickOrderedColumn.scan(min, max)` forwards to `FenwickOrderedList.scan` and has the same `[min, max)` semantics.
