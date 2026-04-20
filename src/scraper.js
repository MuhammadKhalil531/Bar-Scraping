import path from "path";
import { hostname as osHostname } from "os";
import { open as fsOpen, readFile, unlink as fsUnlink } from "fs/promises";
import puppeteer from "puppeteer";
import {
  BAR_CONCURRENCY,
  HEADLESS as HEADLESS_DEFAULT,
  PAGINATION_CLICK_MAX_RETRIES,
  RESUME_ACTION_DELAY_MS,
  RESUME_PAGE_DELAY_MS,
  SAVE_JSON,
} from "./config.js";
import {
  drainBarEndRetryQueue,
  extractPageSummaryKeysAudit,
  extractResultPageBatch,
  writeBarJsonSnapshot,
} from "./detailFetch.js";
import {
  DIR_CHECKPOINT_BARS,
  DIR_EXTRACTED,
  DIR_FINAL_CSV,
  DIR_META,
  DIR_PARTIALS,
} from "./paths.js";
import {
  SEL_BAR_ITEMS,
  SEL_BAR_LABEL,
  SEL_BAR_PANEL,
  SEL_BAR_ROOT,
  SEL_BAR_SELECT,
  SEL_BAR_TRIGGER,
  appendCsvRecords,
  applyCheckpointToCrawlStats,
  dedupeRecords,
  delay,
  ensureDir,
  fingerprintSetCoversStableKey,
  getPaginatorSnapshot,
  getSelectedBarInfo,
  goToFirstResultsPage,
  goToNextResultsPage,
  inferCurrentResultPage1Based,
  navigateAuditToResultsPage,
  loadBarRecordsFromCsvFile,
  loadSeenKeySetFromBarCsv,
  parseEntriesReport,
  sanitizeFilename,
  skipForwardToResultsStartingPage,
  stableRecordKey,
  writeCsv,
  writeJson,
} from "./utils.js";

// --- config (tune for environment / debugging) ---
export const HEADLESS = HEADLESS_DEFAULT;
export const ACTION_DELAY_MS = 150;
export const PAGE_DELAY_MS = 350;
export const DEBUG = process.env.DEBUG !== "0";

const TARGET_URL = "https://bravsearch.bea-brak.de/bravsearch/";
export const OUTPUT_DIR = DIR_EXTRACTED;

/** Bound checkpoint size; newest pages win if over cap */
const MAX_PAGE_AUDIT_IN_CHECKPOINT = 700;
const MAX_AUDITED_KEYS_IN_CHECKPOINT = 2500;
const BAR_ORDER_FILENAME = "bar-scrape-order.json";
const JOB_QUEUE_FILENAME = "job-queue.json";

const SEL_RESULTS_PANEL = "#resultForm\\:pnlResultList_content";
const SEL_RESULTS_LIST = "#resultForm\\:dlResultList";
const SEL_RESULT_CARDS = "#resultForm\\:dlResultList .resultCard";
const SEL_PAGINATOR_BOTTOM = "#resultForm\\:dlResultList_paginator_bottom";

const LINUX_CHROMIUM_ARGS =
  process.platform === "linux"
    ? ["--no-sandbox", "--disable-setuid-sandbox"]
    : [];

export function getLaunchOptions() {
  return {
    headless: HEADLESS,
    args: LINUX_CHROMIUM_ARGS,
    defaultViewport: null,
  };
}

const CHROME_USER_AGENT =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36";

/**
 * Wait for the Bar selectOneMenu root in the DOM and scroll it into view.
 * Using `{ visible: true }` alone often times out in headless (layout/PrimeFaces/CSS).
 */
async function waitForBarDropdownReady(page, timeoutMs = 180000) {
  await page.waitForSelector(SEL_BAR_ROOT, { timeout: timeoutMs });
  await page.evaluate((sel) => {
    const el = document.querySelector(sel);
    if (el) el.scrollIntoView({ block: "center", inline: "nearest" });
  }, SEL_BAR_ROOT);
  await delay(400);
}

async function applyBrowserPageDefaults(page) {
  await page.setUserAgent(CHROME_USER_AGENT);
  await page.setExtraHTTPHeaders({
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8,de;q=0.7",
  });
}

function locksDirPath() {
  const checkpointsDir = path.dirname(DIR_CHECKPOINT_BARS);
  return path.join(checkpointsDir, "locks");
}

function isProcessAlive(pid) {
  if (!Number.isFinite(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/** Max lock age before treating as stale if PID still responds (default 24h). */
function staleLockMaxAgeMs() {
  const raw = process.env.STALE_LOCK_MAX_AGE_MS;
  if (raw === "0" || raw === "") return 0;
  const n = Number(raw);
  if (Number.isFinite(n) && n >= 0) return n;
  return 24 * 60 * 60 * 1000;
}

async function acquireBarLock(barLabel) {
  const lockDir = locksDirPath();
  await ensureDir(lockDir);
  const lockPath = path.join(lockDir, `${sanitizeFilename(barLabel)}.lock`);
  const infoObj = {
    pid: process.pid,
    hostname: osHostname(),
    bar: barLabel,
    startedAt: new Date().toISOString(),
    nodeAppInstance:
      process.env.NODE_APP_INSTANCE ||
      process.env.pm_id ||
      process.env.PM2_INSTANCE_ID ||
      null,
    workerRole: "bar-scraper",
  };
  const info = `${JSON.stringify(infoObj, null, 2)}\n`;
  const maxAttempts = 12;
  const staleAgeMs = staleLockMaxAgeMs();

  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      const fh = await fsOpen(lockPath, "wx");
      await fh.writeFile(info, "utf8");
      console.log(
        `[lock] Acquired Bar lock pid=${process.pid} host=${osHostname()} ${lockPath}`
      );
      return { lockPath, fh };
    } catch (e) {
      if (e?.code !== "EEXIST") throw e;
      let stale = false;
      let staleReason = "";
      try {
        const raw = await readFile(lockPath, "utf8");
        const parsed = JSON.parse(raw);
        const pid = parsed?.pid;
        if (Number.isFinite(pid) && !isProcessAlive(pid)) {
          stale = true;
          staleReason = `dead pid ${pid}`;
        } else if (
          staleAgeMs > 0 &&
          parsed?.startedAt &&
          Number.isFinite(Date.parse(parsed.startedAt))
        ) {
          const ageMs = Date.now() - Date.parse(parsed.startedAt);
          if (ageMs > staleAgeMs) {
            stale = true;
            staleReason = `age ${(ageMs / 3600000).toFixed(1)}h (max ${(staleAgeMs / 3600000).toFixed(1)}h)`;
          }
        }
        if (stale) {
          console.warn(
            `[lock] Stale Bar lock removed (${staleReason}) → ${lockPath}`
          );
          await fsUnlink(lockPath).catch(() => {});
          continue;
        }
      } catch {
        /* malformed — try removing once */
        if (attempt === 0) {
          console.warn(
            `[lock] Unreadable lock file; removing → ${lockPath}`
          );
          await fsUnlink(lockPath).catch(() => {});
          continue;
        }
      }
      await delay(400 + attempt * 150);
    }
  }
  throw new Error(
    `Could not acquire Bar lock for "${barLabel}" after ${maxAttempts} attempt(s): ${lockPath}`
  );
}

async function releaseBarLock(lock) {
  if (!lock) return;
  try {
    await lock.fh?.close?.();
  } catch {}
  try {
    await fsUnlink(lock.lockPath);
    console.log(`[lock] Released Bar lock → ${lock.lockPath}`);
  } catch {}
}

async function setupResourceBlocking(page) {
  try {
    await page.setRequestInterception(true);
    page.on("request", (req) => {
      const t = req.resourceType();
      if (t === "image" || t === "media" || t === "font") {
        req.abort().catch(() => {});
      } else {
        req.continue().catch(() => {});
      }
    });
  } catch (e) {
    console.warn("Request interception not enabled:", e?.message || e);
  }
}

async function readExpectedTotalFromResultText(page) {
  const total = await page.evaluate(() => {
    const spans = [...document.querySelectorAll("span.resultText")];
    const norm = (s) =>
      (s ?? "")
        .replace(/\u00a0/g, " ")
        .replace(/\s+/g, " ")
        .trim();
    for (const sp of spans) {
      const t = norm(sp.textContent || "");
      if (!t) continue;
      const m = t.match(/Number of result entries:\s*([\d.]+)/i);
      if (m) return parseInt(String(m[1]).replace(/\./g, ""), 10);
      const m2 = t.match(/Anzahl der Treffer:\s*([\d.]+)/i);
      if (m2) return parseInt(String(m2[1]).replace(/\./g, ""), 10);
    }
    return null;
  });
  return Number.isFinite(total) && total >= 0 ? total : null;
}

async function loadBarCheckpoint(barName) {
  const base = sanitizeFilename(barName);
  const candidates = [
    path.join(DIR_CHECKPOINT_BARS, `${base}.json`),
    path.join(DIR_EXTRACTED, `${base}-checkpoint.json`),
  ];
  for (const p of candidates) {
    try {
      const raw = await readFile(p, "utf8");
      return JSON.parse(raw);
    } catch {
      /* try next */
    }
  }
  return null;
}

function hydrateNumberSet(cp, field) {
  const raw = cp?.[field];
  const out = new Set();
  if (!Array.isArray(raw)) return out;
  for (const n of raw) {
    if (Number.isFinite(n)) out.add(n);
  }
  return out;
}

function sortedNumberSet(set) {
  return [...set].sort((a, b) => a - b);
}

/** Pages referenced by retry queues (still-unresolved row work). */
function pageNumbersFromQueues(cp) {
  const s = new Set();
  for (const item of [...(cp?.retryQueue || []), ...(cp?.missingRowQueue || [])]) {
    if (Number.isFinite(item?.pageNumber)) s.add(item.pageNumber);
  }
  return s;
}

function pagesWithUnresolvedRowsFromQueue(queue) {
  const s = new Set();
  if (!Array.isArray(queue)) return s;
  for (const item of queue) {
    if (Number.isFinite(item?.pageNumber)) s.add(item.pageNumber);
  }
  return s;
}

/**
 * Collect candidate pages that still need work for targeted resume (not necessarily page 1).
 * @param metaRef optional `{ conservativeFallback?: boolean }` set when full 1..hint fallback runs
 */
function collectMissingPageNumbers(cp, metaRef) {
  const missing = new Set();
  const hint =
    cp?.expectedPagesHint != null && Number.isFinite(cp.expectedPagesHint)
      ? cp.expectedPagesHint
      : null;

  for (const p of cp?.failedPages || []) {
    if (Number.isFinite(p)) missing.add(p);
  }
  for (const p of cp?.pagesWithMissingRows || []) {
    if (Number.isFinite(p)) missing.add(p);
  }
  for (const p of cp?.recoveryPages || []) {
    if (Number.isFinite(p)) missing.add(p);
  }
  for (const p of cp?.auditedMissingPages || []) {
    if (Number.isFinite(p)) missing.add(p);
  }
  for (const p of pageNumbersFromQueues(cp)) missing.add(p);

  const completed = hydrateNumberSet(cp, "completedPages");

  if (hint != null && completed.size > 0) {
    for (let p = 1; p <= hint; p += 1) {
      if (!completed.has(p)) missing.add(p);
    }
  } else if (
    hint != null &&
    completed.size === 0 &&
    Number.isFinite(cp?.expectedTotalRows) &&
    Number.isFinite(cp?.totalRowsWritten) &&
    cp.totalRowsWritten < cp.expectedTotalRows
  ) {
    const last = Number.isFinite(cp?.lastCompletedPage) ? cp.lastCompletedPage : 0;
    if (last >= 1) {
      for (let p = last; p <= hint; p += 1) missing.add(p);
    } else {
      for (let p = 1; p <= hint; p += 1) missing.add(p);
    }
  }

  /**
   * Row-count mismatch but no structural page gaps — only safe fallback is revisiting all pages (dedupe/csv protect).
   */
  if (
    hint != null &&
    Number.isFinite(cp?.expectedTotalRows) &&
    Number.isFinite(cp?.totalRowsWritten) &&
    cp.totalRowsWritten < cp.expectedTotalRows &&
    missing.size === 0
  ) {
    console.warn(
      `[resume] "${cp.barName ?? cp.bar ?? "?"}" committed ${cp.totalRowsWritten}/${cp.expectedTotalRows} but derived missing pages empty — conservative full range 1..${hint}`
    );
    for (let p = 1; p <= hint; p += 1) missing.add(p);
    if (metaRef) metaRef.conservativeFallback = true;
  }

  return missing;
}

/**
 * Resume decision for logging + explicit conservative fallback detection.
 */
function getResumePlan(cp) {
  const metaRef = { conservativeFallback: false };
  if (!cp || cp.status === "completed") {
    return {
      startPage: 1,
      missingSorted: [],
      conservativeFallback: false,
      reason: "fresh_or_completed_checkpoint",
    };
  }
  const missing = collectMissingPageNumbers(cp, metaRef);
  const missingSorted = sortedNumberSet(missing);
  if (missing.size > 0) {
    return {
      startPage: Math.min(...missing),
      missingSorted,
      conservativeFallback: metaRef.conservativeFallback,
      reason: metaRef.conservativeFallback
        ? "min_page_conservative_row_mismatch_full_range"
        : "min_missing_or_failed_or_queued_or_gap",
    };
  }
  const last = Number.isFinite(cp.lastCompletedPage) ? cp.lastCompletedPage : 0;
  return {
    startPage: Math.max(1, last + 1),
    missingSorted,
    conservativeFallback: false,
    reason: "next_page_after_lastCompletedPage",
  };
}

async function mergeMetaJsonFile(filename, updater) {
  await ensureDir(DIR_META);
  const p = path.join(DIR_META, filename);
  let data = {};
  try {
    data = JSON.parse(await readFile(p, "utf8"));
  } catch {
    data = {};
  }
  const next = updater(data) ?? data;
  next.updatedAt = new Date().toISOString();
  await writeJson(p, next);
}

async function countFileLines(filePath) {
  try {
    const txt = await readFile(filePath, "utf8");
    if (!txt) return 0;
    const lines = txt.split(/\r?\n/);
    if (lines.length && lines[lines.length - 1] === "") lines.pop();
    return lines.length;
  } catch {
    return 0;
  }
}

/**
 * Build the client-facing CSV from working CSV rows, deduped by fingerprint.
 * Keeps collision-safe distinct rows while removing rerun/replay duplicates.
 */
async function writeFinalCleanBarCsv(baseName, workingCsvPath) {
  const finalDir = DIR_FINAL_CSV;
  await ensureDir(finalDir);
  const finalCsvPath = path.join(finalDir, `${baseName}.csv`);
  const workingRows = await loadBarRecordsFromCsvFile(workingCsvPath);
  const cleanRows = dedupeRecords(workingRows);
  await writeCsv(finalCsvPath, cleanRows);
  const workingLineCount = await countFileLines(workingCsvPath);
  const finalLineCount = await countFileLines(finalCsvPath);
  return {
    finalCsvPath,
    workingRawLineCount: workingLineCount,
    workingDataRowCount: Math.max(0, workingLineCount - 1),
    finalRawLineCount: finalLineCount,
    finalCleanCsvCount: cleanRows.length,
  };
}

async function persistAggregatedMetaFiles(payload) {
  const {
    baseKey,
    barLabel,
    barIndex,
    csvPath: csvPathMeta,
    expectedTotalRows,
    committedUniqueRows,
    completenessOk,
    unexpectedEarlyStop,
    missingRowsEstimate,
    completedPagesCount,
    failedPagesCount,
    pagesWithMissingRowsCount,
    retryQueueLength,
    missingRowQueueLength,
    failedPages,
    pagesWithMissingRows,
    unresolvedRowPages,
    missingRowQueue,
    pageSizeHint,
    expectedPagesHint,
    barDurationSec,
    pagesProcessedThisRun,
    approxPpm,
    approxRpm,
    paginationInnerRetries,
    finalCheckpointWritten,
    recoveryPages: recoveryPagesMeta,
    auditedMissingPages: auditedMissingPagesMeta,
    auditedMissingKeys: auditedMissingKeysMeta,
    lastMismatchAuditAt: lastMismatchAuditAtMeta,
    dedupeStableKeyCollisionEvents: dedupeStableKeyCollisionEventsMeta = 0,
    dedupeStableKeyCollisionKeysSample:
      dedupeStableKeyCollisionKeysSampleMeta = [],
    workingRawLineCount: workingRawLineCountMeta = 0,
    workingDataRowCount: workingDataRowCountMeta = 0,
    finalCleanCsvCount: finalCleanCsvCountMeta = 0,
    finalCsvPath: finalCsvPathMeta = "",
    finalValidationResult: finalValidationResultMeta = "unknown",
  } = payload;

  await mergeMetaJsonFile("bar-totals.json", (data) => {
    data.bars = data.bars || {};
    data.bars[baseKey] = {
      barName: barLabel,
      barIndex: barIndex ?? null,
      expectedTotalRows,
      committedUniqueRows,
      pageSizeHint,
      expectedPagesHint,
      updatedAt: new Date().toISOString(),
    };
    return data;
  });

  await mergeMetaJsonFile("bar-validation.json", (data) => {
    data.bars = data.bars || {};
    data.bars[baseKey] = {
      barIndex: barIndex ?? null,
      barLabel,
      expectedTotalRows,
      actualCommittedRows: committedUniqueRows,
      missingRows:
        missingRowsEstimate ??
        (expectedTotalRows != null && Number.isFinite(committedUniqueRows)
          ? Math.max(0, expectedTotalRows - committedUniqueRows)
          : null),
      completedPagesCount,
      failedPagesCount,
      pagesWithMissingRowsCount,
      retryQueueLength,
      missingRowQueueLength,
      status: completenessOk ? "completed" : "failed",
      csvPath: csvPathMeta,
      completenessOk,
      unexpectedEarlyStop,
      finalCheckpointWritten,
      failedPages,
      pagesWithMissingRows,
      unresolvedRowPages: [...new Set(unresolvedRowPages)].sort((a, b) => a - b),
      barDurationSec,
      pagesProcessedThisRun,
      pagesPerMinuteApprox: approxPpm,
      recordsPerMinuteApprox: approxRpm,
      paginationInnerRetries,
      recoveryPages: recoveryPagesMeta,
      auditedMissingPages: auditedMissingPagesMeta,
      auditedMissingKeysCount: Array.isArray(auditedMissingKeysMeta)
        ? auditedMissingKeysMeta.length
        : 0,
      lastMismatchAuditAt: lastMismatchAuditAtMeta,
      dedupeStableKeyCollisionEvents: dedupeStableKeyCollisionEventsMeta,
      dedupeStableKeyCollisionKeysSample:
        dedupeStableKeyCollisionKeysSampleMeta,
      dedupeFingerprintDedupeNote:
        "Rows dedupe by recordDedupeFingerprint; stableRecordKey collisions are preserved.",
      workingRawLineCount: workingRawLineCountMeta,
      workingDataRowCount: workingDataRowCountMeta,
      finalCleanCsvCount: finalCleanCsvCountMeta,
      finalCsvPath: finalCsvPathMeta,
      finalValidationResult: finalValidationResultMeta,
      updatedAt: new Date().toISOString(),
    };
    return data;
  });

  await mergeMetaJsonFile("missing-pages.json", (data) => {
    data.bars = data.bars || {};
    const merged = new Set([
      ...(failedPages || []),
      ...(pagesWithMissingRows || []),
      ...unresolvedRowPages,
      ...(recoveryPagesMeta || []),
      ...(auditedMissingPagesMeta || []),
    ]);
    data.bars[baseKey] = {
      barName: barLabel,
      barIndex: barIndex ?? null,
      missingPageNumbers: [...merged].sort((a, b) => a - b),
      failedPages,
      pagesWithMissingRows,
      recoveryPages: recoveryPagesMeta,
      auditedMissingPages: auditedMissingPagesMeta,
      lastMismatchAuditAt: lastMismatchAuditAtMeta,
      updatedAt: new Date().toISOString(),
    };
    return data;
  });

  await mergeMetaJsonFile("final-csv-summary.json", (data) => {
    data.bars = data.bars || {};
    data.bars[baseKey] = {
      barIndex: barIndex ?? null,
      barLabel,
      websiteExpectedCount: expectedTotalRows,
      workingCsvRawLineCount: workingRawLineCountMeta,
      workingCsvDataRowCount: workingDataRowCountMeta,
      finalCleanCsvCount: finalCleanCsvCountMeta,
      collisionCount: dedupeStableKeyCollisionEventsMeta,
      finalValidationResult: finalValidationResultMeta,
      finalCsvPath: finalCsvPathMeta,
      updatedAt: new Date().toISOString(),
    };
    return data;
  });

  await mergeMetaJsonFile("missing-rows.json", (data) => {
    data.byBar = data.byBar || {};
    const q = Array.isArray(missingRowQueue) ? missingRowQueue : [];
    const ak = Array.isArray(auditedMissingKeysMeta)
      ? auditedMissingKeysMeta
      : [];
    data.byBar[baseKey] = {
      barName: barLabel,
      barIndex: barIndex ?? null,
      unresolvedTasks: q,
      queueCount: q.length,
      auditedMissingKeys: ak.slice(0, 500),
      auditedMissingKeysCount: ak.length,
      lastMismatchAuditAt: lastMismatchAuditAtMeta,
      updatedAt: new Date().toISOString(),
    };
    return data;
  });
}

async function saveBarCheckpoint(barName, payload) {
  const p = path.join(DIR_CHECKPOINT_BARS, `${sanitizeFilename(barName)}.json`);
  await ensureDir(path.dirname(p));
  await writeJson(p, payload);
}

async function clickStartSearch(page) {
  await delay(ACTION_DELAY_MS);
  const clicked = await page.evaluate(() => {
    const buttons = [...document.querySelectorAll("button, a.ui-button")];
    const hit = buttons.find((b) =>
      /Start search!/i.test(b.textContent || "")
    );
    if (hit) {
      hit.click();
      return true;
    }
    const spans = [...document.querySelectorAll("span.ui-button-text")];
    const spanHit = spans.find((s) =>
      /Start search!/i.test(s.textContent || "")
    );
    const parentBtn = spanHit?.closest("button, .ui-button");
    if (parentBtn) {
      parentBtn.click();
      return true;
    }
    return false;
  });
  if (!clicked) {
    const ok = await page.evaluate(() => {
      const xpath = "//button[contains(., 'Start search!')]";
      const r = document.evaluate(
        xpath,
        document,
        null,
        XPathResult.FIRST_ORDERED_NODE_TYPE,
        null
      );
      const node = r.singleNodeValue;
      if (node && node instanceof HTMLElement) {
        node.click();
        return true;
      }
      return false;
    });
    if (!ok) {
      throw new Error('Could not find control containing "Start search!"');
    }
  }
}

async function clickWithFallback(page, handle, selectorLogged) {
  await handle.evaluate((el) =>
    el.scrollIntoView({ block: "center", inline: "nearest" })
  );
  try {
    await handle.click({ delay: 50 });
    console.log(`  click succeeded: Puppeteer (${selectorLogged})`);
  } catch {
    await page.evaluate((el) => el.click(), handle);
    console.log(`  click succeeded: evaluate(el.click()) (${selectorLogged})`);
  }
}

/**
 * After finishing a Bar, return to the search form for the next Bar.
 */
async function clickNewSearch(page) {
  const clicked = await page.evaluate(() => {
    const spans = [...document.querySelectorAll("span.ui-button-text.ui-c")];
    const hit = spans.find(
      (s) => (s.textContent || "").trim() === "New search"
    );
    if (!hit) return false;
    const btn = hit.closest("button, a.ui-button, a[href], .ui-button");
    if (btn) {
      btn.click();
      return true;
    }
    hit.click();
    return true;
  });
  if (!clicked) {
    throw new Error('Could not find "New search" control (span.ui-button-text.ui-c)');
  }
  await delay(PAGE_DELAY_MS);
}

async function waitForSearchFormBeforeNextBar(page) {
  await waitForBarDropdownReady(page, 120000);
  await delay(PAGE_DELAY_MS);
  await page
    .waitForFunction(
      () => {
        const buttons = [...document.querySelectorAll("button, a.ui-button")];
        return buttons.some((b) => /Start search!/i.test(b.textContent || ""));
      },
      { timeout: 60000 }
    )
    .catch(() => null);
  await delay(ACTION_DELAY_MS);
}

/**
 * Same discovery order as index-based selection; includes `value` when the hidden SELECT exposes it.
 */
async function collectRealBarOptions(page) {
  await waitForBarDropdownReady(page, 180000);
  await delay(200);

  let openSelector = SEL_BAR_TRIGGER;
  let opener;
  try {
    opener = await page.waitForSelector(SEL_BAR_TRIGGER, {
      timeout: 30000,
    });
  } catch {
    openSelector = SEL_BAR_LABEL;
    opener = await page.waitForSelector(SEL_BAR_LABEL, {
      timeout: 30000,
    });
  }

  await clickWithFallback(page, opener, openSelector);
  await delay(400);
  await page.waitForSelector(SEL_BAR_PANEL, { timeout: 30000 });
  await delay(300);
  await page.waitForSelector(SEL_BAR_ITEMS, { timeout: 30000 });

  const options = await page.evaluate((itemSel, selectSel) => {
    const items = [...document.querySelectorAll(itemSel)];
    const pairs = [];
    for (const el of items) {
      const raw = (el.textContent || "").replace(/\u00a0/g, " ").trim();
      const empty = raw === "" || /^[\s\-–—]+$/.test(raw);
      if (empty) continue;
      const st = window.getComputedStyle(el);
      if (
        el.offsetParent === null ||
        st.visibility === "hidden" ||
        st.display === "none"
      )
        continue;
      const dataVal =
        el.getAttribute("data-value") ||
        (el.dataset && el.dataset.value) ||
        "";
      pairs.push({ label: raw, value: dataVal || "" });
    }
    const sel = document.querySelector(selectSel);
    if (sel && sel.tagName === "SELECT" && pairs.length) {
      const opts = [...sel.options].map((opt) => ({
        t: (opt.textContent || "").replace(/\u00a0/g, " ").trim(),
        v: opt.value || "",
      }));
      for (const p of pairs) {
        const hit = opts.find((o) => o.t === p.label);
        if (hit && hit.v) p.value = hit.v;
      }
    }
    return pairs;
  }, SEL_BAR_ITEMS, SEL_BAR_SELECT);

  await page.keyboard.press("Escape");
  await delay(300);
  return options;
}

/**
 * Select the n-th real (non-blank) Bar option, 0-based. Same ordering as collectRealBarOptions.
 */
async function selectBarOptionByRealIndex(page, realIndex) {
  if (realIndex < 0) {
    throw new Error(`Invalid Bar index ${realIndex}`);
  }

  await waitForBarDropdownReady(page, 120000);
  await delay(200);

  const initialLabel = await page
    .$eval(SEL_BAR_LABEL, (el) => (el.textContent || "").trim())
    .catch(() => "");

  let openSelector = SEL_BAR_TRIGGER;
  let opener;
  try {
    opener = await page.waitForSelector(SEL_BAR_TRIGGER, {
      timeout: 30000,
    });
  } catch {
    openSelector = SEL_BAR_LABEL;
    opener = await page.waitForSelector(SEL_BAR_LABEL, {
      timeout: 30000,
    });
  }

  await clickWithFallback(page, opener, openSelector);
  await delay(400);
  await page.waitForSelector(SEL_BAR_PANEL, { timeout: 30000 });
  await delay(300);
  await page.waitForSelector(SEL_BAR_ITEMS, { timeout: 30000 });

  const itemHandles = await page.$$(SEL_BAR_ITEMS);
  const usable = [];
  for (const h of itemHandles) {
    const { text, ok } = await page.evaluate((el) => {
      const raw = (el.textContent || "").replace(/\u00a0/g, " ").trim();
      const empty = raw === "" || /^[\s\-–—]+$/.test(raw);
      return { text: raw, ok: !empty };
    }, h);
    if (!ok) continue;
    const visible = await page.evaluate((el) => el.offsetParent !== null, h);
    if (!visible) continue;
    usable.push({ handle: h, text });
  }

  if (realIndex >= usable.length) {
    throw new Error(
      `Bar index ${realIndex} out of range (only ${usable.length} non-empty option(s))`
    );
  }

  const { handle, text } = usable[realIndex];
  await clickWithFallback(
    page,
    handle,
    `${SEL_BAR_ITEMS} [index ${realIndex}]`
  );

  await page
    .waitForFunction(
      (labelSel, before) => {
        const el = document.querySelector(labelSel);
        const t = (el?.textContent || "").replace(/\u00a0/g, " ").trim();
        if (!t) return false;
        if (before === "") return true;
        return t !== before;
      },
      { timeout: 45000 },
      SEL_BAR_LABEL,
      initialLabel
    )
    .catch(() => null);

  await delay(ACTION_DELAY_MS);
  return { text };
}

async function waitForResultsRegion(page) {
  await Promise.race([
    page.waitForSelector(SEL_RESULT_CARDS, { visible: true, timeout: 120000 }),
    page.waitForSelector(SEL_PAGINATOR_BOTTOM, { visible: true, timeout: 120000 }),
  ]).catch(async () => {
    await page.waitForSelector(SEL_RESULTS_PANEL, { timeout: 30000 }).catch(() => null);
    await page.waitForSelector(SEL_RESULTS_LIST, { timeout: 30000 }).catch(() => null);
  });
  await delay(PAGE_DELAY_MS);
  await delay(ACTION_DELAY_MS);
}

async function savePartialIfNeeded(csvPath, barLabel, err) {
  let rows = [];
  try {
    rows = await loadBarRecordsFromCsvFile(csvPath);
  } catch {
    return;
  }
  if (!rows.length) return;
  await ensureDir(DIR_PARTIALS);
  const base = `${sanitizeFilename(barLabel || "partial")}_partial`;
  console.error("Saving partial results due to error:", err?.message || err);
  const outPath = path.join(DIR_PARTIALS, `${base}.csv`);
  await writeCsv(outPath, dedupeRecords(rows));
  console.log(`  Partial CSV: ${outPath}`);
}

async function persistBarOrderFile(bars) {
  await ensureDir(DIR_META);
  const p = path.join(DIR_META, BAR_ORDER_FILENAME);
  await writeJson(p, {
    dropdownOrder: bars,
    barCount: bars.length,
    concurrency: BAR_CONCURRENCY,
    sequential: BAR_CONCURRENCY <= 1,
    saveJson: SAVE_JSON,
    updatedAt: new Date().toISOString(),
  });
  console.log(
    `Recorded ${bars.length} ordered Bar label(s) → ${p} (for merging complete.csv)`
  );
}

function buildJobsFromBarOptions(options) {
  return options.map((o, barIndex) => ({
    barIndex,
    barLabel: o.label,
    barValue: o.value || "",
    status: "pending",
    lastError: null,
    completedAt: null,
  }));
}

async function persistJobQueue(jobs) {
  await ensureDir(DIR_META);
  const p = path.join(DIR_META, JOB_QUEUE_FILENAME);
  await writeJson(p, {
    version: 1,
    updatedAt: new Date().toISOString(),
    jobs,
  });
  console.log(`Job queue (${jobs.length} Bar job(s)) → ${p}`);
}

async function updateJobStatus(barIndex, patch) {
  const p = path.join(DIR_META, JOB_QUEUE_FILENAME);
  try {
    const raw = await readFile(p, "utf8");
    const data = JSON.parse(raw);
    const job = data.jobs?.find((j) => j.barIndex === barIndex);
    if (job) Object.assign(job, patch);
    data.updatedAt = new Date().toISOString();
    await writeJson(p, data);
  } catch {
    /* optional file */
  }
}

function capPageAuditList(pageAuditMap) {
  const arr = [...pageAuditMap.values()].sort((a, b) => a.p - b.p);
  return arr.length > MAX_PAGE_AUDIT_IN_CHECKPOINT
    ? arr.slice(-MAX_PAGE_AUDIT_IN_CHECKPOINT)
    : arr;
}

function paginatorResumeOpts() {
  return {
    resumePageDelayMs: RESUME_PAGE_DELAY_MS,
    resumeActionDelayMs: RESUME_ACTION_DELAY_MS,
    maxRetries: PAGINATION_CLICK_MAX_RETRIES,
  };
}

async function runFullSummaryMismatchAudit(
  page,
  selectedBarLabel,
  expectedPagesHint,
  seenState
) {
  const auditNavOpts = {
    pageDelayMs: PAGE_DELAY_MS,
    actionDelayMs: ACTION_DELAY_MS,
    maxRetries: PAGINATION_CLICK_MAX_RETRIES,
    resumePageDelayMs: RESUME_PAGE_DELAY_MS,
    resumeActionDelayMs: RESUME_ACTION_DELAY_MS,
  };

  console.warn(
    `  [${selectedBarLabel}] [mismatch-audit] Resetting paginator to page 1 (was on last page after main scrape)`
  );
  await goToFirstResultsPage(page, auditNavOpts);
  let approxAfterReset = await inferCurrentResultPage1Based(page).catch(() => null);
  console.warn(
    `  [${selectedBarLabel}] [mismatch-audit] Paginator reset OK — approxPage=${approxAfterReset ?? "?"} — sweeping ${expectedPagesHint} page(s) via sequential next`
  );

  const allKeys = new Set();
  const keysByPage = new Map();
  const missing = new Set();
  for (let p = 1; p <= expectedPagesHint; p += 1) {
    if (p > 1) {
      const ok = await goToNextResultsPage(page, {
        pageDelayMs: PAGE_DELAY_MS,
        actionDelayMs: ACTION_DELAY_MS,
        maxRetries: PAGINATION_CLICK_MAX_RETRIES,
      });
      if (!ok) {
        throw new Error(
          `[mismatch-audit] Cannot advance from page ${p - 1} to ${p} (paginator next disabled)`
        );
      }
    }
    console.warn(
      `  [${selectedBarLabel}] [mismatch-audit] Page ${p}/${expectedPagesHint} reached — extracting summary keys`
    );
    await delay(PAGE_DELAY_MS);
    const { keys } = await extractPageSummaryKeysAudit(
      page,
      selectedBarLabel,
      p
    );
    keysByPage.set(p, keys);
    keys.forEach((k) => allKeys.add(k));
    for (const k of keys) {
      if (!fingerprintSetCoversStableKey(seenState.fingerprints, k))
        missing.add(k);
    }
  }
  const auditedMissingPages = [];
  for (let p = 1; p <= expectedPagesHint; p += 1) {
    const ks = keysByPage.get(p) || [];
    if (ks.some((k) => missing.has(k))) auditedMissingPages.push(p);
  }
  return {
    auditedMissingKeys: [...missing],
    auditedMissingPages,
    summaryKeyTotal: allKeys.size,
  };
}

async function runSubsetSummaryMismatchAudit(
  page,
  selectedBarLabel,
  pageNums,
  seenState
) {
  const missing = new Set();
  const pagesHit = [];
  const uniq = [...new Set(pageNums)].sort((a, b) => a - b);
  const subsetOpts = {
    pageDelayMs: PAGE_DELAY_MS,
    actionDelayMs: ACTION_DELAY_MS,
    maxRetries: PAGINATION_CLICK_MAX_RETRIES,
    resumePageDelayMs: RESUME_PAGE_DELAY_MS,
    resumeActionDelayMs: RESUME_ACTION_DELAY_MS,
  };
  for (const p of uniq) {
    await navigateAuditToResultsPage(page, p, {
      ...subsetOpts,
      logPrefix: `  [${selectedBarLabel}] [mismatch-audit-subset]`,
    });
    await delay(PAGE_DELAY_MS);
    const { keys } = await extractPageSummaryKeysAudit(
      page,
      selectedBarLabel,
      p
    );
    let pageHasMiss = false;
    for (const k of keys) {
      if (!fingerprintSetCoversStableKey(seenState.fingerprints, k)) {
        missing.add(k);
        pageHasMiss = true;
      }
    }
    if (pageHasMiss) pagesHit.push(p);
  }
  return {
    auditedMissingKeys: [...missing],
    auditedMissingPages: pagesHit,
  };
}

async function runTargetedDetailRecoveryPages(
  page,
  selectedBarLabel,
  pageNums,
  csvPath,
  seenState,
  crawlStats,
  barRetryQueue,
  pageAuditMap,
  debugFlag
) {
  const uniq = [...new Set(pageNums)].sort((a, b) => a - b);
  const beforeSz = seenState.fingerprints.size;
  const recoveryNavOpts = {
    pageDelayMs: PAGE_DELAY_MS,
    actionDelayMs: ACTION_DELAY_MS,
    maxRetries: PAGINATION_CLICK_MAX_RETRIES,
    resumePageDelayMs: RESUME_PAGE_DELAY_MS,
    resumeActionDelayMs: RESUME_ACTION_DELAY_MS,
  };
  for (const p of uniq) {
    console.warn(
      `  [${selectedBarLabel}] Targeted detail recovery → page ${p}`
    );
    await navigateAuditToResultsPage(page, p, {
      ...recoveryNavOpts,
      logPrefix: `  [${selectedBarLabel}] [targeted-recovery-nav]`,
    });
    await delay(ACTION_DELAY_MS);
    const { rows, pagePendingDetailFailures } = await extractResultPageBatch(
      page,
      selectedBarLabel,
      {
        pageNumber: p,
        debug: debugFlag,
        statsRef: crawlStats,
        barRetryQueue,
      }
    );
    const keysFromRows = rows.map((r) => stableRecordKey(r));
    const { appended, deduped } = await appendCsvRecords(
      csvPath,
      rows,
      seenState
    );
    const snap = await getPaginatorSnapshot(page);
    pageAuditMap.set(p, {
      p,
      bar: selectedBarLabel,
      range: (snap.currentReport || "").slice(0, 140),
      cardsVis: rows.length,
      keys: keysFromRows,
      appended,
      deduped,
      unresolved: pagePendingDetailFailures,
      recoveryPass: true,
    });
  }
  console.warn(
    `  [${selectedBarLabel}] Targeted recovery finished: committed rows ${beforeSz} → ${seenState.fingerprints.size} (+${seenState.fingerprints.size - beforeSz})`
  );
}

async function scrapePaginationForBar(
  page,
  selectedBarLabel,
  crawlStats,
  checkpointExtra,
  paginationOpts = {}
) {
  const {
    resumeStartPage: resumeStartPageOpt = 1,
    barIndexForLog = null,
    totalBarsForLog = null,
    resumeCheckpoint = null,
    barIndexForJob = null,
  } = paginationOpts;

  const baseName = sanitizeFilename(selectedBarLabel);
  const csvPath = path.join(OUTPUT_DIR, `${baseName}.csv`);
  checkpointExtra.bar = selectedBarLabel;

  let resumeStartPage = Math.max(1, resumeStartPageOpt);
  const seenState = await loadSeenKeySetFromBarCsv(csvPath);
  const initialSeenKeysCount = seenState.fingerprints.size;
  let dedupeCollisionEventsTotal = 0;
  const dedupeCollisionKeysAccum = new Set();
  let barRetryQueue = Array.isArray(resumeCheckpoint?.retryQueue)
    ? [...resumeCheckpoint.retryQueue]
    : [];

  const pageAuditMap = new Map();
  for (const e of resumeCheckpoint?.pageAudit || []) {
    if (Number.isFinite(e?.p)) pageAuditMap.set(e.p, e);
  }
  let recoveryPages = Array.isArray(resumeCheckpoint?.recoveryPages)
    ? resumeCheckpoint.recoveryPages.filter(Number.isFinite)
    : [];
  let auditedMissingKeys = [];
  let auditedMissingPages = [];
  let lastMismatchAuditAt = resumeCheckpoint?.lastMismatchAuditAt ?? null;

  const completedPages = hydrateNumberSet(resumeCheckpoint, "completedPages");
  const failedPages = hydrateNumberSet(resumeCheckpoint, "failedPages");
  let pagesWithMissingRows = hydrateNumberSet(
    resumeCheckpoint,
    "pagesWithMissingRows"
  );
  let pageLevelLog = Array.isArray(resumeCheckpoint?.pageLevelLog)
    ? resumeCheckpoint.pageLevelLog.slice(-200)
    : [];

  if (resumeStartPage > 1) {
    applyCheckpointToCrawlStats(resumeCheckpoint, crawlStats);
  }

  if (resumeStartPage > 1) {
    try {
      const skip = await skipForwardToResultsStartingPage(
        page,
        resumeStartPage,
        {
          resumePageDelayMs: RESUME_PAGE_DELAY_MS,
          resumeActionDelayMs: RESUME_ACTION_DELAY_MS,
          maxRetries: PAGINATION_CLICK_MAX_RETRIES,
        }
      );
      console.log(
        `  Resume navigation complete: ${skip.steps} paginator step(s) in ${(skip.ms / 1000).toFixed(1)}s → starting scrape at page ${resumeStartPage}.`
      );
    } catch (e) {
      console.error(`  Resume skip failed: ${e?.message || e}`);
      throw e;
    }
  }

  const barSessionT0 = Date.now();
  const pageTimesMs = [];
  let paginationRetries = 0;
  let staleRecoveryEvents = 0;
  let paginationInnerRetries = 0;

  const startSnap = await getPaginatorSnapshot(page);
  const startStats = parseEntriesReport(startSnap.currentReport);
  const totalEntriesHint = startStats?.total ?? null;
  let expectedTotalRows = await readExpectedTotalFromResultText(page);
  if (
    expectedTotalRows == null &&
    resumeCheckpoint != null &&
    Number.isFinite(resumeCheckpoint.expectedTotalRows)
  ) {
    expectedTotalRows = resumeCheckpoint.expectedTotalRows;
    console.warn(
      `  [${selectedBarLabel}] Using checkpoint expectedTotalRows=${expectedTotalRows} (span.resultText missing this session)`
    );
  }
  const pageSizeHint =
    startStats && Number.isFinite(startStats.from) && Number.isFinite(startStats.to)
      ? Math.max(1, startStats.to - startStats.from + 1)
      : null;
  const expectedPagesHint =
    totalEntriesHint != null && pageSizeHint != null
      ? Math.ceil(totalEntriesHint / pageSizeHint)
      : null;
  if (expectedTotalRows != null) {
    console.log(
      `  [${selectedBarLabel}] Expected total rows (resultText): ${expectedTotalRows}`
    );
  } else {
    console.warn(
      `  [${selectedBarLabel}] Could not parse expected total from span.resultText — Bar will NOT be marked completed without it.`
    );
  }
  if (totalEntriesHint != null && expectedPagesHint != null) {
    const idx =
      barIndexForLog != null && totalBarsForLog != null
        ? ` | worker Bar ${barIndexForLog + 1}/${totalBarsForLog}`
        : "";
    const perPage =
      pageSizeHint != null && Number.isFinite(pageSizeHint) ? pageSizeHint : 6;
    console.log(
      `  [${selectedBarLabel}]${idx} ~${expectedPagesHint} page(s), ${totalEntriesHint} entries (@ ~${perPage}/page).`
    );
  }
  console.log(
    `  Start: active page #="${startSnap.activePage}" | report="${startSnap.currentReport.slice(0, 80)}${startSnap.currentReport.length > 80 ? "…" : ""}" | next disabled=${startSnap.nextDisabled}`
  );

  let pageIndex = resumeStartPage;
  let reachedTrueLastPage = false;
  let unexpectedEarlyStop = false;

  function buildCpPayload(overrides = {}) {
    const base = {
      barName: selectedBarLabel,
      status: "running",
      lastCompletedPage: pageIndex,
      totalRowsWritten: seenState.fingerprints.size,
      actualCommittedRows: seenState.fingerprints.size,
      expectedTotalRows,
      expectedPagesHint,
      pageSizeHint,
      detailOkCount: crawlStats.detailOk,
      detailFailedCount: crawlStats.failedDetail,
      summaryFailedCount: crawlStats.summaryFailed || 0,
      summaryRowsProcessed: crawlStats.summaryRows,
      retryQueue: barRetryQueue,
      missingRowQueue: [...barRetryQueue],
      seenKeyCount: seenState.fingerprints.size,
      csvPath,
      barIndex: barIndexForJob,
      updatedAt: new Date().toISOString(),
      paginationRetries,
      staleRecoveryEvents,
      paginationInnerRetries,
      completedPages: sortedNumberSet(completedPages),
      failedPages: sortedNumberSet(failedPages),
      pagesWithMissingRows: sortedNumberSet(pagesWithMissingRows),
      pageLevelLog: pageLevelLog.slice(-200),
      pageAudit: capPageAuditList(pageAuditMap),
      recoveryPages: [...new Set(recoveryPages)].sort((a, b) => a - b),
      auditedMissingPages,
      auditedMissingKeys: auditedMissingKeys.slice(
        0,
        MAX_AUDITED_KEYS_IN_CHECKPOINT
      ),
      lastMismatchAuditAt,
    };
    return { ...base, ...overrides };
  }

  while (true) {
    failedPages.delete(pageIndex);

    const beforeSnap = await getPaginatorSnapshot(page);
    const beforeStats = parseEntriesReport(beforeSnap.currentReport);
    let approxPage = null;
    try {
      approxPage = await inferCurrentResultPage1Based(page);
    } catch (_) {
      /* ignore */
    }
    const remainingHint =
      expectedPagesHint != null
        ? Math.max(0, expectedPagesHint - pageIndex)
        : "?";
    console.log(
      `  --- [${selectedBarLabel}] Page ${pageIndex}${approxPage != null ? ` (~dom ${approxPage})` : ""} | entries ${beforeStats ? `${beforeStats.from}–${beforeStats.to} of ${beforeStats.total}` : beforeSnap.currentReport || "?"} | ~${remainingHint} page(s) left ---`
    );

    const tPage = Date.now();

    const { rows, pagePendingDetailFailures } =
      await extractResultPageBatch(page, selectedBarLabel, {
        pageNumber: pageIndex,
        debug: DEBUG,
        statsRef: crawlStats,
        barRetryQueue,
      });

    const expectedRowsOnPage =
      beforeStats &&
      Number.isFinite(beforeStats.from) &&
      Number.isFinite(beforeStats.to)
        ? beforeStats.to - beforeStats.from + 1
        : pageSizeHint ?? 6;
    const extractedRows = rows.length;
    const keysFromRows = rows.map((r) => stableRecordKey(r));

    const appendMeta = await appendCsvRecords(csvPath, rows, seenState);
    const {
      appended,
      deduped,
      collisionDetected,
      collisionKeys,
      rowsKept,
      rowsPreviouslyDropped,
    } = appendMeta;
    if (collisionDetected > 0) {
      dedupeCollisionEventsTotal += collisionDetected;
      for (const ck of collisionKeys) dedupeCollisionKeysAccum.add(ck);
      console.warn(
        `    [${selectedBarLabel}] dedupe: stable_key_collision events=${collisionDetected} collisionKeys(sample)=${collisionKeys.slice(0, 3).join(";")}${collisionKeys.length > 3 ? "…" : ""} collisionRowsSaved=${rowsPreviouslyDropped} (old_logic_would_skip) newRowsThisAppend=${appended} rowsKept(field)=${rowsKept}`
      );
    }
    const pageSec = (Date.now() - tPage) / 1000;
    pageTimesMs.push(Date.now() - tPage);

    pageAuditMap.set(pageIndex, {
      p: pageIndex,
      bar: selectedBarLabel,
      range: (beforeSnap.currentReport || "").slice(0, 140),
      cardsVis: expectedRowsOnPage,
      keys: keysFromRows,
      appended,
      deduped,
      unresolved: pagePendingDetailFailures,
      collisionDetected,
      collisionKeys,
      rowsKept,
      rowsPreviouslyDropped,
    });

    const pageComplete = pagePendingDetailFailures === 0;
    if (pageComplete) {
      completedPages.add(pageIndex);
      pagesWithMissingRows.delete(pageIndex);
    } else {
      pagesWithMissingRows.add(pageIndex);
      completedPages.delete(pageIndex);
    }

    pageLevelLog.push({
      page: pageIndex,
      expectedRows: expectedRowsOnPage,
      extractedRows,
      unresolvedRows: pagePendingDetailFailures,
      complete: pageComplete,
      appendedNew: appended,
      deduped,
      collisionDetected,
      collisionKeys,
      rowsPreviouslyDropped,
    });
    if (pageLevelLog.length > 240) pageLevelLog.splice(0, pageLevelLog.length - 200);

    const avgSec =
      pageTimesMs.length > 0
        ? pageTimesMs.reduce((a, b) => a + b, 0) / pageTimesMs.length / 1000
        : 0;
    const eta =
      avgSec > 0 && expectedPagesHint != null
        ? (Math.max(0, expectedPagesHint - pageIndex) * avgSec).toFixed(0)
        : "?";
    const elapsedMinRun = Math.max((Date.now() - barSessionT0) / 60000, 1e-6);
    const newRowsThisRun = seenState.fingerprints.size - initialSeenKeysCount;
    const rpmSession = newRowsThisRun / elapsedMinRun;
    const ppmSession = pageTimesMs.length / elapsedMinRun;

    console.log(
      `    CSV +${appended} new row(s) | pagePending=${pagePendingDetailFailures} | detail OK ${crawlStats.detailOk} | fail ${crawlStats.failedDetail} | ${pageSec.toFixed(2)}s | avg ${avgSec.toFixed(2)}s/page | ETA ~${eta}s | committedRows=${seenState.fingerprints.size} | ~${rpmSession.toFixed(1)} rec/min ~${ppmSession.toFixed(2)} pg/min (session)`
    );

    await saveBarCheckpoint(selectedBarLabel, buildCpPayload());
    console.log(`    checkpoint saved (page ${pageIndex} committed)`);

    let moved = false;
    const maxNavAttempts = 3;
    for (let navTry = 0; navTry < maxNavAttempts; navTry += 1) {
      try {
        moved = await goToNextResultsPage(page, {
          pageDelayMs: PAGE_DELAY_MS,
          actionDelayMs: ACTION_DELAY_MS,
          maxRetries: PAGINATION_CLICK_MAX_RETRIES,
        });
        paginationInnerRetries += navTry;
        break;
      } catch (e) {
        staleRecoveryEvents += 1;
        paginationRetries += 1;
        console.warn(
          `    pagination failed (try ${navTry + 1}/${maxNavAttempts}): ${e?.message || e}`
        );
        if (navTry === maxNavAttempts - 1) {
          failedPages.add(pageIndex);
          await saveBarCheckpoint(
            selectedBarLabel,
            buildCpPayload({
              status: "failed",
              paginationFailedAtPage: pageIndex,
              lastPaginationError: String(e?.message || e),
            })
          );
          throw e;
        }
        await delay(400 * (navTry + 1));
      }
    }

    if (!moved) {
      const endSnap = await getPaginatorSnapshot(page);
      const endStats = parseEntriesReport(endSnap.currentReport);
      console.log(
        `  [${selectedBarLabel}] Next disabled. active=#${endSnap.activePage} | report="${endSnap.currentReport.slice(0, 100)}…" | next disabled=${endSnap.nextDisabled}`
      );
      if (
        totalEntriesHint != null &&
        endStats != null &&
        endStats.to < totalEntriesHint
      ) {
        unexpectedEarlyStop = true;
        console.warn(
          `[WARNING] [${selectedBarLabel}] Pagination stopped early (entries ${endStats.from}–${endStats.to} of ${totalEntriesHint}).`
        );
      } else if (
        endStats != null &&
        totalEntriesHint != null &&
        endStats.to >= totalEntriesHint
      ) {
        reachedTrueLastPage = true;
      } else if (totalEntriesHint == null) {
        reachedTrueLastPage = true;
      }
      console.log(`  [${selectedBarLabel}] Pagination finished.`);
      break;
    }

    pageIndex += 1;
  }

  if (
    !unexpectedEarlyStop &&
    totalEntriesHint != null &&
    expectedPagesHint != null &&
    pageIndex < expectedPagesHint - 1
  ) {
    console.warn(
      `[WARNING] [${selectedBarLabel}] Stopped after ~${pageIndex} page(s); ~${expectedPagesHint} expected from total ${totalEntriesHint}.`
    );
    unexpectedEarlyStop = true;
  }

  const { recoveredRows } = await drainBarEndRetryQueue(
    page,
    selectedBarLabel,
    barRetryQueue,
    DEBUG,
    crawlStats
  );
  if (recoveredRows.length) {
    const drainMeta = await appendCsvRecords(
      csvPath,
      recoveredRows,
      seenState
    );
    if (drainMeta.collisionDetected > 0) {
      dedupeCollisionEventsTotal += drainMeta.collisionDetected;
      for (const ck of drainMeta.collisionKeys)
        dedupeCollisionKeysAccum.add(ck);
    }
  }

  let finalPagesWithUnresolved =
    pagesWithUnresolvedRowsFromQueue(barRetryQueue);
  pagesWithMissingRows = finalPagesWithUnresolved;

  let committed = seenState.fingerprints.size;
  let mismatch =
    expectedTotalRows != null ? expectedTotalRows - committed : null;
  let retryQueueLen = Array.isArray(barRetryQueue) ? barRetryQueue.length : 0;
  let queueEmpty = retryQueueLen === 0;

  const silentGapRecovery =
    expectedTotalRows != null &&
    mismatch != null &&
    mismatch > 0 &&
    queueEmpty &&
    failedPages.size === 0 &&
    finalPagesWithUnresolved.size === 0 &&
    !unexpectedEarlyStop &&
    expectedPagesHint != null &&
    expectedPagesHint >= 1;

  if (silentGapRecovery) {
    console.warn(
      `  [${selectedBarLabel}] Row-count mismatch with empty queues (${committed}/${expectedTotalRows}) — running summary-only mismatch audit`
    );
    lastMismatchAuditAt = new Date().toISOString();
    const audit = await runFullSummaryMismatchAudit(
      page,
      selectedBarLabel,
      expectedPagesHint,
      seenState
    );
    auditedMissingKeys = audit.auditedMissingKeys;
    auditedMissingPages = audit.auditedMissingPages;
    console.warn(
      `  [${selectedBarLabel}] Mismatch audit: union_summary_keys=${audit.summaryKeyTotal} audited_missing_keys=${auditedMissingKeys.length} audited_missing_pages=[${auditedMissingPages.slice(0, 45).join(",")}${auditedMissingPages.length > 45 ? " …" : ""}]`
    );

    recoveryPages = [...new Set([...recoveryPages, ...auditedMissingPages])].sort(
      (a, b) => a - b
    );

    if (auditedMissingPages.length > 0) {
      await runTargetedDetailRecoveryPages(
        page,
        selectedBarLabel,
        auditedMissingPages,
        csvPath,
        seenState,
        crawlStats,
        barRetryQueue,
        pageAuditMap,
        DEBUG
      );
    }

    const drainAfterRecovery = await drainBarEndRetryQueue(
      page,
      selectedBarLabel,
      barRetryQueue,
      DEBUG,
      crawlStats
    );
    if (drainAfterRecovery.recoveredRows.length) {
      const d2 = await appendCsvRecords(
        csvPath,
        drainAfterRecovery.recoveredRows,
        seenState
      );
      if (d2.collisionDetected > 0) {
        dedupeCollisionEventsTotal += d2.collisionDetected;
        for (const ck of d2.collisionKeys) dedupeCollisionKeysAccum.add(ck);
      }
    }

    finalPagesWithUnresolved =
      pagesWithUnresolvedRowsFromQueue(barRetryQueue);
    pagesWithMissingRows = finalPagesWithUnresolved;

    committed = seenState.fingerprints.size;
    mismatch =
      expectedTotalRows != null ? expectedTotalRows - committed : null;
    retryQueueLen = barRetryQueue.length;
    queueEmpty = retryQueueLen === 0;

    if (mismatch === 0) {
      auditedMissingKeys = [];
      auditedMissingPages = [];
    } else if (
      mismatch != null &&
      mismatch > 0 &&
      queueEmpty &&
      auditedMissingPages.length > 0
    ) {
      console.warn(
        `  [${selectedBarLabel}] Post-recovery re-audit on ${auditedMissingPages.length} page(s)`
      );
      lastMismatchAuditAt = new Date().toISOString();
      const sub = await runSubsetSummaryMismatchAudit(
        page,
        selectedBarLabel,
        auditedMissingPages,
        seenState
      );
      auditedMissingKeys = sub.auditedMissingKeys;
      auditedMissingPages = sub.auditedMissingPages;
      recoveryPages = [...new Set([...recoveryPages, ...auditedMissingPages])].sort(
        (a, b) => a - b
      );
    }

    console.warn(
      `  [${selectedBarLabel}] Post-recovery validation: expected=${expectedTotalRows} committed=${seenState.fingerprints.size} missing≈${expectedTotalRows != null ? Math.max(0, expectedTotalRows - seenState.fingerprints.size) : "?"} auditedKeysRemaining=${auditedMissingKeys.length} auditedPagesRemaining=${auditedMissingPages.length}`
    );
  }

  finalPagesWithUnresolved = pagesWithUnresolvedRowsFromQueue(barRetryQueue);
  pagesWithMissingRows = finalPagesWithUnresolved;

  committed = seenState.fingerprints.size;
  mismatch = expectedTotalRows != null ? expectedTotalRows - committed : null;
  retryQueueLen = Array.isArray(barRetryQueue) ? barRetryQueue.length : 0;
  const missingRowQueueLen = retryQueueLen;
  queueEmpty = retryQueueLen === 0;
  const pagesWithMissingEmpty = pagesWithMissingRows.size === 0;
  const failedPagesEmpty = failedPages.size === 0;
  const unresolvedPagesEmpty = finalPagesWithUnresolved.size === 0;
  const auditedMissingKeysEmpty = auditedMissingKeys.length === 0;
  const auditedMissingPagesEmpty = auditedMissingPages.length === 0;

  let completenessOk =
    expectedTotalRows != null &&
    mismatch === 0 &&
    queueEmpty &&
    !unexpectedEarlyStop &&
    failedPagesEmpty &&
    unresolvedPagesEmpty &&
    pagesWithMissingEmpty &&
    auditedMissingKeysEmpty &&
    auditedMissingPagesEmpty;

  const barDurationSec = (Date.now() - barSessionT0) / 1000;
  const pagesProcessedThisRun = pageTimesMs.length;
  const newRowsThisRun = committed - initialSeenKeysCount;
  const approxRpm =
    barDurationSec > 0.5 ? (newRowsThisRun / barDurationSec) * 60 : 0;
  const approxPpm =
    barDurationSec > 0.5 ? (pagesProcessedThisRun / barDurationSec) * 60 : 0;

  const missingRowsEstimate =
    expectedTotalRows != null ? Math.max(0, expectedTotalRows - committed) : null;

  console.log(
    `  [${selectedBarLabel}] Final totals: expected=${expectedTotalRows ?? "?"} | committed_unique=${committed} | missing_rows≈${missingRowsEstimate ?? "?"} | retryQueue=${retryQueueLen} | missingRowQueue=${missingRowQueueLen} | mismatch=${mismatch ?? "?"} | failedPages(${failedPages.size})=${sortedNumberSet(failedPages).join(",") || "—"} | pagesWithMissingRows(${pagesWithMissingRows.size})=${sortedNumberSet(pagesWithMissingRows).join(",") || "—"} | unresolvedPages=${sortedNumberSet(finalPagesWithUnresolved).join(",") || "—"} | auditedMissingKeys=${auditedMissingKeys.length} | auditedMissingPages=${auditedMissingPages.length} | recoveryPages=${recoveryPages.length}`
  );
  console.log(
    `  [${selectedBarLabel}] Throughput: ${barDurationSec.toFixed(1)}s | ~${approxPpm.toFixed(2)} pages/min | ~${approxRpm.toFixed(1)} records/min | paginator inner retries=${paginationInnerRetries}`
  );

  let finalCheckpointWritten = false;
  try {
    await saveBarCheckpoint(selectedBarLabel, {
      ...buildCpPayload({
        status: completenessOk ? "completed" : "failed",
        lastCompletedPage: pageIndex,
        totalRowsWritten: seenState.fingerprints.size,
        actualCommittedRows: seenState.fingerprints.size,
        pagesWithMissingRows: sortedNumberSet(pagesWithMissingRows),
        missingRowQueue: [...barRetryQueue],
        dedupeStableKeyCollisionEventsTotal: dedupeCollisionEventsTotal,
        dedupeStableKeyCollisionKeysSample: [
          ...dedupeCollisionKeysAccum,
        ].slice(0, 40),
      }),
    });
    finalCheckpointWritten = true;
  } catch (e) {
    console.error(
      `  [${selectedBarLabel}] Final checkpoint write FAILED — Bar left incomplete:`,
      e?.message || e
    );
    completenessOk = false;
  }

  console.log(
    `  [${selectedBarLabel}] Validation summary: status=${completenessOk ? "completed" : "failed"} | expected=${expectedTotalRows ?? "?"} | committed=${committed} | missing≈${missingRowsEstimate ?? "?"} | failedPages=${failedPages.size} | pagesWithMissingRows=${pagesWithMissingRows.size} | retryQ=${retryQueueLen} | missingRowQ=${missingRowQueueLen} | auditedKeys=${auditedMissingKeys.length} | auditedPages=${auditedMissingPages.length} | unexpectedEarlyStop=${unexpectedEarlyStop} | finalCheckpointWritten=${finalCheckpointWritten} | lastMismatchAuditAt=${lastMismatchAuditAt ?? "—"} | dedupeStableKeyCollisions=${dedupeCollisionEventsTotal}`
  );

  const finalExport = await writeFinalCleanBarCsv(baseName, csvPath);
  const finalValidationResult =
    expectedTotalRows == null
      ? "expected_unknown"
      : finalExport.finalCleanCsvCount === expectedTotalRows
        ? "ok"
        : "count_mismatch";
  console.log(
    `  [${selectedBarLabel}] Final clean CSV: expected=${expectedTotalRows ?? "?"} | workingRawLines=${finalExport.workingRawLineCount} (rows=${finalExport.workingDataRowCount}) | finalRows=${finalExport.finalCleanCsvCount} | collisions=${dedupeCollisionEventsTotal} | finalValidation=${finalValidationResult} | path=${finalExport.finalCsvPath}`
  );

  await writeBarJsonSnapshot(baseName, csvPath);

  await persistAggregatedMetaFiles({
    baseKey: baseName,
    barLabel: selectedBarLabel,
    barIndex: barIndexForJob,
    csvPath,
    expectedTotalRows,
    committedUniqueRows: committed,
    completenessOk,
    unexpectedEarlyStop,
    missingRowsEstimate,
    completedPagesCount: sortedNumberSet(completedPages).length,
    failedPagesCount: sortedNumberSet(failedPages).length,
    pagesWithMissingRowsCount: sortedNumberSet(pagesWithMissingRows).length,
    retryQueueLength: retryQueueLen,
    missingRowQueueLength: missingRowQueueLen,
    failedPages: sortedNumberSet(failedPages),
    pagesWithMissingRows: sortedNumberSet(pagesWithMissingRows),
    unresolvedRowPages: sortedNumberSet(finalPagesWithUnresolved),
    missingRowQueue: [...barRetryQueue],
    pageSizeHint,
    expectedPagesHint,
    barDurationSec,
    pagesProcessedThisRun,
    approxPpm,
    approxRpm,
    paginationInnerRetries,
    finalCheckpointWritten,
    recoveryPages,
    auditedMissingPages,
    auditedMissingKeys,
    lastMismatchAuditAt,
    dedupeStableKeyCollisionEvents: dedupeCollisionEventsTotal,
    dedupeStableKeyCollisionKeysSample: [...dedupeCollisionKeysAccum].slice(
      0,
      40
    ),
    workingRawLineCount: finalExport.workingRawLineCount,
    workingDataRowCount: finalExport.workingDataRowCount,
    finalCleanCsvCount: finalExport.finalCleanCsvCount,
    finalCsvPath: finalExport.finalCsvPath,
    finalValidationResult,
  });

  return {
    totalEntriesHint,
    expectedTotalRows,
    expectedPagesHint,
    pageSizeHint,
    committedUniqueRows: committed,
    completenessOk,
    pageIndex,
    unexpectedEarlyStop,
    reachedTrueLastPage,
    baseName,
    csvPath,
    paginationRetries,
    staleRecoveryEvents,
    paginationInnerRetries,
    pageTimesMs,
    barDurationSec,
    approxPagesPerMin: approxPpm,
    approxRecordsPerMin: approxRpm,
  };
}

/**
 * Run up to `concurrency` workers pulling from a shared job index (parallel Bar scrape).
 */
async function runPoolConcurrent(jobs, concurrency, runOne) {
  let next = 0;
  const results = new Array(jobs.length);
  async function worker() {
    while (true) {
      const idx = next;
      next += 1;
      if (idx >= jobs.length) return;
      results[idx] = await runOne(jobs[idx], idx);
    }
  }
  const n = Math.min(concurrency, jobs.length);
  await Promise.all(Array.from({ length: n }, () => worker()));
  return results;
}

async function scrapeSingleBarSession(browser, barIndex, totalBars, barLabelHint) {
  const context = await browser.createBrowserContext();
  const page = await context.newPage();
  await applyBrowserPageDefaults(page);
  await setupResourceBlocking(page);
  await page.setViewport({ width: 1440, height: 900 });

  let fatal = false;
  let selectedBarLabel = barLabelHint || "unknown";
  let reachedTrueLastPage = false;
  let unexpectedEarlyStop = false;
  let csvPathForPartial = path.join(
    OUTPUT_DIR,
    `${sanitizeFilename(barLabelHint)}.csv`
  );
  let lock = null;

  const crawlStats = {
    summaryRows: 0,
    detailOk: 0,
    failedDetail: 0,
    summaryFailed: 0,
  };
  const checkpointExtra = {
    bar: "",
    processedDetailIds: [],
  };

  try {
    const cpEarly = await loadBarCheckpoint(barLabelHint);
    if (
      cpEarly?.status === "completed" &&
      process.env.SKIP_COMPLETED_BARS !== "0"
    ) {
      console.log(
        `[parallel worker] Skipping completed Bar ${barIndex + 1}/${totalBars}: "${barLabelHint}"`
      );
      await context.close().catch(() => null);
      return { skipped: true, selectedBarLabel: barLabelHint };
    }

    await updateJobStatus(barIndex, {
      status: "running",
      startedAt: new Date().toISOString(),
    });

    const barT0 = Date.now();
    console.log(
      `[parallel worker] Bar ${barIndex + 1}/${totalBars} START "${barLabelHint}" (workers=${BAR_CONCURRENCY})`
    );
    await page.goto(TARGET_URL, { waitUntil: "networkidle2", timeout: 180000 });
    await delay(ACTION_DELAY_MS);

    const pick = await selectBarOptionByRealIndex(page, barIndex);
    const barInfo = (await getSelectedBarInfo(page)) || pick;
    selectedBarLabel = (barInfo?.text || pick.text || "").trim();
    checkpointExtra.bar = selectedBarLabel;
    csvPathForPartial = path.join(
      OUTPUT_DIR,
      `${sanitizeFilename(selectedBarLabel)}.csv`
    );

    const cp = await loadBarCheckpoint(selectedBarLabel);
    const resumePlan =
      cp && cp.status !== "completed" ? getResumePlan(cp) : null;
    const resumeStartPage = resumePlan?.startPage ?? 1;
    if (resumePlan) {
      const preview = resumePlan.missingSorted.slice(0, 35);
      const tail = resumePlan.missingSorted.length > 35 ? " …" : "";
      console.warn(
        `[parallel worker] Resume decision: startPage=${resumeStartPage} reason=${resumePlan.reason} conservativeFullRange=${resumePlan.conservativeFallback} missingPages=[${preview.join(",")}${tail}] (${resumePlan.missingSorted.length}) lastCompletedPage=${cp?.lastCompletedPage ?? "?"}`
      );
    }
    if (
      cp &&
      cp.status !== "completed" &&
      Number.isFinite(cp.totalRowsWritten) &&
      Number.isFinite(cp.expectedTotalRows) &&
      cp.totalRowsWritten < cp.expectedTotalRows
    ) {
      console.warn(
        `[parallel worker] Recovery context: committed ${cp.totalRowsWritten}/${cp.expectedTotalRows}` +
          (Array.isArray(cp.completedPages) && cp.completedPages.length
            ? ""
            : " | legacy/no completedPages array — tail or conservative mode may apply")
      );
    }
    if (resumeStartPage > 1) {
      console.log(
        `[parallel worker] Paginator skip target: page ${resumeStartPage} (checkpoint lastCompletedPage=${cp?.lastCompletedPage ?? "?"}).`
      );
    }

    console.log(
      `[parallel worker] Selected: "${selectedBarLabel}" (index ${barIndex})`
    );

    lock = await acquireBarLock(selectedBarLabel);
    console.log(
      `[parallel worker] Claimed Bar lock for "${selectedBarLabel}" → ${lock.lockPath}`
    );

    await clickStartSearch(page);
    await waitForResultsRegion(page);

    const meta = await scrapePaginationForBar(
      page,
      selectedBarLabel,
      crawlStats,
      checkpointExtra,
      {
        resumeStartPage,
        barIndexForLog: barIndex,
        totalBarsForLog: totalBars,
        resumeCheckpoint: cp,
        barIndexForJob: barIndex,
      }
    );
    unexpectedEarlyStop = meta.unexpectedEarlyStop;
    reachedTrueLastPage = meta.reachedTrueLastPage;
    csvPathForPartial = meta.csvPath;

    const barSec = ((Date.now() - barT0) / 1000).toFixed(1);
    console.log(`  --- [${selectedBarLabel}] Run summary (${barSec}s) ---`);
    console.log(`    Summary rows: ${crawlStats.summaryRows}`);
    console.log(`    Detail OK: ${crawlStats.detailOk}`);
    console.log(`    Detail failed (after retries): ${crawlStats.failedDetail}`);
    console.log(
      `    Pagination retries: ${meta.paginationRetries ?? 0} | stale recoveries: ${meta.staleRecoveryEvents ?? 0} | pages scraped: ${meta.pageIndex}`
    );

    await updateJobStatus(barIndex, {
      status: meta.completenessOk ? "completed" : "failed",
      completedAt: new Date().toISOString(),
      lastError: meta.completenessOk
        ? null
        : unexpectedEarlyStop
          ? "unexpected_early_stop"
          : "count_mismatch_or_pending_retries",
    });
    console.log(
      `[parallel worker] Bar ${barIndex + 1}/${totalBars} DONE "${selectedBarLabel}"`
    );

    return {
      selectedBarLabel,
      unexpectedEarlyStop,
      reachedTrueLastPage,
      fatal: false,
    };
  } catch (err) {
    fatal = true;
    console.error(
      `[parallel worker] Bar ${barIndex + 1}/${totalBars} FAILED:`,
      err?.message || err
    );
    await updateJobStatus(barIndex, {
      status: "failed",
      lastError: String(err?.message || err),
      completedAt: new Date().toISOString(),
    });
    await savePartialIfNeeded(csvPathForPartial, selectedBarLabel, err);
    throw err;
  } finally {
    await releaseBarLock(lock);
    await context.close().catch(() => null);
  }
}

async function runSequentialAllBars(browser, page, bars) {
  const mode = "sequential";
  console.log(
    `Mode: ${mode} (workers=1). Total Bar options: ${bars.length}.`
  );

  let firstBarThisRun = true;

  for (let i = 0; i < bars.length; i += 1) {
    const barNameFromList = bars[i];

    const cpPre = await loadBarCheckpoint(barNameFromList);
    if (
      cpPre?.status === "completed" &&
      process.env.SKIP_COMPLETED_BARS !== "0"
    ) {
      console.log(
        `\nSkipping completed Bar ${i + 1}/${bars.length}: "${barNameFromList}"`
      );
      continue;
    }

    console.log(
      `\n######## Bar ${i + 1}/${bars.length} (ordered): "${barNameFromList}" ########`
    );

    if (!firstBarThisRun) {
      console.log('  Invoking "New search" to return to search form…');
      await clickNewSearch(page);
      await waitForSearchFormBeforeNextBar(page);
      console.log('  "New search" completed; Bar dropdown ready.');
    }

    const pick = await selectBarOptionByRealIndex(page, i);
    const barInfo = (await getSelectedBarInfo(page)) || pick;
    const selectedBarLabel = (barInfo?.text || pick.text || "").trim();
    console.log(`  Selected Bar: "${selectedBarLabel}"`);

    const cp = await loadBarCheckpoint(selectedBarLabel);
    const resumePlan =
      cp && cp.status !== "completed" ? getResumePlan(cp) : null;
    const resumeStartPage = resumePlan?.startPage ?? 1;
    if (resumePlan) {
      const preview = resumePlan.missingSorted.slice(0, 35);
      const tail = resumePlan.missingSorted.length > 35 ? " …" : "";
      console.warn(
        `  Resume decision: startPage=${resumeStartPage} reason=${resumePlan.reason} conservativeFullRange=${resumePlan.conservativeFallback} missingPages=[${preview.join(",")}${tail}] (${resumePlan.missingSorted.length}) lastCompletedPage=${cp?.lastCompletedPage ?? "?"}`
      );
    }
    if (
      cp &&
      cp.status !== "completed" &&
      Number.isFinite(cp.totalRowsWritten) &&
      Number.isFinite(cp.expectedTotalRows) &&
      cp.totalRowsWritten < cp.expectedTotalRows
    ) {
      console.warn(
        `  Recovery context: committed ${cp.totalRowsWritten}/${cp.expectedTotalRows}` +
          (Array.isArray(cp.completedPages) && cp.completedPages.length
            ? ""
            : " | legacy/no completedPages — tail or conservative mode may apply")
      );
    }
    if (resumeStartPage > 1) {
      console.log(
        `  Paginator skip target: page ${resumeStartPage} (lastCompletedPage was ${cp?.lastCompletedPage ?? "?"}).`
      );
    }

    const crawlStats = {
      summaryRows: 0,
      detailOk: 0,
      failedDetail: 0,
      summaryFailed: 0,
    };
    const checkpointExtra = {
      bar: selectedBarLabel,
      processedDetailIds: [],
    };
    let csvPathForPartial = path.join(
      OUTPUT_DIR,
      `${sanitizeFilename(selectedBarLabel)}.csv`
    );

    await updateJobStatus(i, {
      status: "running",
      startedAt: new Date().toISOString(),
    });

    const lock = await acquireBarLock(selectedBarLabel);
    console.log(
      `  Claimed Bar lock for "${selectedBarLabel}" → ${lock.lockPath}`
    );

    console.log("  Starting search…");
    await clickStartSearch(page);
    await waitForResultsRegion(page);

    let reachedTrueLastPage = false;
    let unexpectedEarlyStop = false;
    let fatal = false;

    const barT0 = Date.now();

    try {
      const meta = await scrapePaginationForBar(
        page,
        selectedBarLabel,
        crawlStats,
        checkpointExtra,
        {
          resumeStartPage,
          barIndexForLog: i,
          totalBarsForLog: bars.length,
          resumeCheckpoint: cp,
          barIndexForJob: i,
        }
      );
      unexpectedEarlyStop = meta.unexpectedEarlyStop;
      reachedTrueLastPage = meta.reachedTrueLastPage;
      csvPathForPartial = meta.csvPath;

      const barSec = ((Date.now() - barT0) / 1000).toFixed(1);
      console.log(`  --- [${selectedBarLabel}] Final for this Bar (${barSec}s) ---`);
      if (meta.totalEntriesHint != null) {
        console.log(`    Paginator total entries: ${meta.totalEntriesHint}`);
      }
      if (meta.expectedPagesHint != null) {
        console.log(
          `    Pages scraped: ${meta.pageIndex} | expected≈${meta.expectedPagesHint}`
        );
      } else {
        console.log(`    Pages scraped: ${meta.pageIndex}`);
      }
      console.log(`    Summary rows: ${crawlStats.summaryRows}`);
      console.log(`    Detail OK: ${crawlStats.detailOk}`);
      console.log(`    Detail failed: ${crawlStats.failedDetail}`);
      console.log(
        `    Pagination retries (outer): ${meta.paginationRetries ?? 0} | stale DOM recoveries: ${meta.staleRecoveryEvents ?? 0}`
      );

      await updateJobStatus(i, {
        status: meta.completenessOk ? "completed" : "failed",
        completedAt: new Date().toISOString(),
        lastError: meta.completenessOk
          ? null
          : unexpectedEarlyStop
            ? "unexpected_early_stop"
            : "count_mismatch_or_pending_retries",
      });
    } catch (err) {
      fatal = true;
      console.error(`  Bar scrape failed:`, err?.message || err);
      await updateJobStatus(i, {
        status: "failed",
        lastError: String(err?.message || err),
        completedAt: new Date().toISOString(),
      });
      await savePartialIfNeeded(csvPathForPartial, selectedBarLabel, err);
      throw err;
    } finally {
      await releaseBarLock(lock);
      const keepOpenForUnexpectedDebug =
        DEBUG && !fatal && unexpectedEarlyStop && !reachedTrueLastPage;
      if (keepOpenForUnexpectedDebug) {
        console.log(
          "DEBUG=true: possible pagination mismatch — inspect before closing."
        );
      }
    }

    firstBarThisRun = false;
  }
}

export async function runScrape() {
  console.log(
    `SAVE_JSON=${SAVE_JSON ? "1 (on)" : "0 (off, CSV primary)"} | BAR_CONCURRENCY=${BAR_CONCURRENCY}`
  );

  let browser;
  let fatal = false;

  try {
    browser = await puppeteer.launch(getLaunchOptions());
    const page = await browser.newPage();
    await applyBrowserPageDefaults(page);
    await setupResourceBlocking(page);
    await page.setViewport({ width: 1440, height: 900 });

    console.log(
      `Opening ${TARGET_URL} (headless=${HEADLESS}, BAR_CONCURRENCY=${BAR_CONCURRENCY})`
    );
    await page.goto(TARGET_URL, { waitUntil: "networkidle2", timeout: 180000 });
    await delay(ACTION_DELAY_MS);

    console.log("Discovering Bar dropdown options (ordered)…");
    const barOptions = await collectRealBarOptions(page);
    const bars = barOptions.map((o) => o.label);
    if (!bars.length) {
      throw new Error("No non-empty Bar options found in the dropdown.");
    }
    console.log(`Found ${bars.length} real Bar option(s).`);
    await persistBarOrderFile(bars);
    await persistJobQueue(buildJobsFromBarOptions(barOptions));

    if (BAR_CONCURRENCY <= 1) {
      await runSequentialAllBars(browser, page, bars);
      return;
    }

    await browser.close();
    browser = null;
    console.log(
      `Discovery browser closed. Starting parallel scrape (workers=${BAR_CONCURRENCY}).`
    );

    const poolBrowser = await puppeteer.launch(getLaunchOptions());
    browser = poolBrowser;

    await runPoolConcurrent(bars, BAR_CONCURRENCY, async (barLabel, idx) => {
      await scrapeSingleBarSession(poolBrowser, idx, bars.length, barLabel);
    });

    console.log(`Parallel Bar scrape finished (${bars.length} job(s)).`);
  } catch (err) {
    fatal = true;
    console.error("Scrape failed:", err?.message || err);
    throw err;
  } finally {
    if (browser) {
      if (!(DEBUG && fatal)) {
        await browser.close();
        console.log("Browser closed.");
      } else {
        console.log("DEBUG=true: leaving browser open after fatal error.");
      }
    }
  }
}
