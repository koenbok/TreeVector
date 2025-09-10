export const DEFAULT_ORDER_KEY = "$time";
export const DEFAULT_ORDER_KEY_TYPE: "number" | "string" = "number";
export const DEFAULT_SEGMENT_COUNT = 8192;
// Target ~10 MiB per chunk for numeric data (8 bytes per number)
const TARGET_CHUNK_BYTES = 10 * 1024 * 1024; // 10 MiB
export const DEFAULT_CHUNK_COUNT = Math.max(
    1,
    Math.round(TARGET_CHUNK_BYTES / (8 * DEFAULT_SEGMENT_COUNT)),
);


