/**
 * SAVE_JSON=1 — also write per-bar JSON (detail blobs; large). Default: CSV only.
 * BAR_CONCURRENCY — parallel isolated browser contexts (1 = sequential Bars).
 *   Default: 2. Use BAR_CONCURRENCY=1 for maximum stability on constrained hosts.
 * HEADLESS — `0` or `false` shows the browser; otherwise headless (default for prod).
 */
export const SAVE_JSON = process.env.SAVE_JSON === "1";

const rawConcEnv = process.env.BAR_CONCURRENCY;
const rawConc =
  rawConcEnv === undefined || rawConcEnv === ""
    ? 2
    : parseInt(rawConcEnv, 10);
export const BAR_CONCURRENCY =
  Number.isFinite(rawConc) && rawConc >= 1 ? Math.min(rawConc, 8) : 2;

const headlessEnv = process.env.HEADLESS;
export const HEADLESS =
  headlessEnv === undefined ||
  headlessEnv === "" ||
  !/^(0|false|no)$/i.test(String(headlessEnv));

/** Shorter delays during bulk resume (next-click loop to reach start page) */
export const RESUME_PAGE_DELAY_MS = Math.max(
  20,
  parseInt(process.env.RESUME_PAGE_DELAY_MS ?? "55", 10) || 55
);
export const RESUME_ACTION_DELAY_MS = Math.max(
  15,
  parseInt(process.env.RESUME_ACTION_DELAY_MS ?? "35", 10) || 35
);

export const PAGINATION_CLICK_MAX_RETRIES = Math.min(
  5,
  Math.max(1, parseInt(process.env.PAGINATION_CLICK_MAX_RETRIES ?? "3", 10) || 3)
);

/** Detail fetch attempts per card before page-level retry sweep */
export const DETAIL_FETCH_MAX_ATTEMPTS = Math.min(
  5,
  Math.max(1, parseInt(process.env.DETAIL_FETCH_MAX_ATTEMPTS ?? "3", 10) || 3)
);

/** Extra sweeps over failed cards on the same results page */
export const DETAIL_PAGE_RETRY_SWEEPS = Math.min(
  4,
  Math.max(1, parseInt(process.env.DETAIL_PAGE_RETRY_SWEEPS ?? "2", 10) || 2)
);

/** Attempts for bar-end retry (session still open) */
export const DETAIL_BAR_END_MAX_ATTEMPTS = Math.min(
  6,
  Math.max(1, parseInt(process.env.DETAIL_BAR_END_MAX_ATTEMPTS ?? "3", 10) || 3)
);
