import path from "path";

const ROOT = process.cwd();

/** Final per-Bar CSV and merged output */
export const DIR_EXTRACTED = path.join(ROOT, "extracted-data");
export const DIR_FINAL_CSV = path.join(DIR_EXTRACTED, "final-csv");

/** Checkpoint JSON and seen-keys */
export const DIR_CHECKPOINTS = path.join(DIR_EXTRACTED, "checkpoints");
export const DIR_CHECKPOINT_BARS = path.join(DIR_CHECKPOINTS, "bars");

/** bar-scrape-order.json, run metadata */
export const DIR_META = path.join(DIR_EXTRACTED, "meta");

/** Partial saves on failure */
export const DIR_PARTIALS = path.join(DIR_EXTRACTED, "partials");
