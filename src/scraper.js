import path from "path";
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
  extractResultPageBatch,
  writeBarJsonSnapshot,
} from "./detailFetch.js";
import {
  DIR_CHECKPOINT_BARS,
  DIR_EXTRACTED,
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
  getPaginatorSnapshot,
  getSelectedBarInfo,
  goToNextResultsPage,
  inferCurrentResultPage1Based,
  loadBarRecordsFromCsvFile,
  loadSeenKeySetFromBarCsv,
  parseEntriesReport,
  sanitizeFilename,
  skipForwardToResultsStartingPage,
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

async function acquireBarLock(barLabel) {
  const lockDir = locksDirPath();
  await ensureDir(lockDir);
  const lockPath = path.join(lockDir, `${sanitizeFilename(barLabel)}.lock`);
  const info = JSON.stringify(
    { pid: process.pid, bar: barLabel, startedAt: new Date().toISOString() },
    null,
    2
  );
  const fh = await fsOpen(lockPath, "wx");
  await fh.writeFile(info + "\n", "utf8");
  return { lockPath, fh };
}

async function releaseBarLock(lock) {
  if (!lock) return;
  try {
    await lock.fh?.close?.();
  } catch {}
  try {
    await fsUnlink(lock.lockPath);
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
  const seenKeys = await loadSeenKeySetFromBarCsv(csvPath);
  let barRetryQueue = Array.isArray(resumeCheckpoint?.retryQueue)
    ? [...resumeCheckpoint.retryQueue]
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

  const pageTimesMs = [];
  let paginationRetries = 0;
  let staleRecoveryEvents = 0;

  const startSnap = await getPaginatorSnapshot(page);
  const startStats = parseEntriesReport(startSnap.currentReport);
  const totalEntriesHint = startStats?.total ?? null;
  const expectedTotalRows = await readExpectedTotalFromResultText(page);
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
    console.log(
      `  [${selectedBarLabel}]${idx} ~${expectedPagesHint} page(s), ${totalEntriesHint} entries (@ 6/page).`
    );
  }
  console.log(
    `  Start: active page #="${startSnap.activePage}" | report="${startSnap.currentReport.slice(0, 80)}${startSnap.currentReport.length > 80 ? "…" : ""}" | next disabled=${startSnap.nextDisabled}`
  );

  let pageIndex = resumeStartPage;
  let reachedTrueLastPage = false;
  let unexpectedEarlyStop = false;

  while (true) {
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

    const { appended } = await appendCsvRecords(csvPath, rows, seenKeys);
    const pageSec = (Date.now() - tPage) / 1000;
    pageTimesMs.push(Date.now() - tPage);

    const avgSec =
      pageTimesMs.length > 0
        ? pageTimesMs.reduce((a, b) => a + b, 0) / pageTimesMs.length / 1000
        : 0;
    const eta =
      avgSec > 0 && expectedPagesHint != null
        ? (Math.max(0, expectedPagesHint - pageIndex) * avgSec).toFixed(0)
        : "?";

    console.log(
      `    CSV +${appended} new row(s) | pagePending=${pagePendingDetailFailures} | detail OK ${crawlStats.detailOk} | fail ${crawlStats.failedDetail} | ${pageSec.toFixed(2)}s | avg ${avgSec.toFixed(2)}s/page | ETA ~${eta}s | seenKeys=${seenKeys.size}`
    );

    const cpPayload = {
      barName: selectedBarLabel,
      status: "running",
      lastCompletedPage: pageIndex,
      totalRowsWritten: seenKeys.size,
      expectedTotalRows,
      detailOkCount: crawlStats.detailOk,
      detailFailedCount: crawlStats.failedDetail,
      summaryFailedCount: crawlStats.summaryFailed || 0,
      summaryRowsProcessed: crawlStats.summaryRows,
      retryQueue: barRetryQueue,
      seenKeyCount: seenKeys.size,
      csvPath,
      barIndex: barIndexForJob,
      updatedAt: new Date().toISOString(),
      paginationRetries,
      staleRecoveryEvents,
    };

    await saveBarCheckpoint(selectedBarLabel, cpPayload);
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
        break;
      } catch (e) {
        staleRecoveryEvents += 1;
        paginationRetries += 1;
        console.warn(
          `    pagination failed (try ${navTry + 1}/${maxNavAttempts}): ${e?.message || e}`
        );
        if (navTry === maxNavAttempts - 1) throw e;
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
    await appendCsvRecords(csvPath, recoveredRows, seenKeys);
  }

  const committed = seenKeys.size;
  const mismatch =
    expectedTotalRows != null ? expectedTotalRows - committed : null;
  const noPendingRetries =
    Array.isArray(barRetryQueue) && barRetryQueue.length === 0;
  const completenessOk =
    expectedTotalRows != null &&
    mismatch === 0 &&
    noPendingRetries &&
    !unexpectedEarlyStop;

  console.log(
    `  [${selectedBarLabel}] Final totals: expected=${expectedTotalRows ?? "?"} | committed_unique=${committed} | retryQueue=${barRetryQueue.length} | mismatch=${mismatch ?? "?"}`
  );

  await saveBarCheckpoint(selectedBarLabel, {
    barName: selectedBarLabel,
    status: completenessOk ? "completed" : "failed",
    lastCompletedPage: pageIndex,
    totalRowsWritten: seenKeys.size,
    expectedTotalRows,
    detailOkCount: crawlStats.detailOk,
    detailFailedCount: crawlStats.failedDetail,
    summaryFailedCount: crawlStats.summaryFailed || 0,
    summaryRowsProcessed: crawlStats.summaryRows,
    retryQueue: barRetryQueue,
    seenKeyCount: seenKeys.size,
    csvPath,
    barIndex: barIndexForJob,
    updatedAt: new Date().toISOString(),
  });

  await writeBarJsonSnapshot(baseName, csvPath);

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
    pageTimesMs,
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
    const needsFullRecovery =
      cp &&
      Number.isFinite(cp.expectedTotalRows) &&
      Number.isFinite(cp.totalRowsWritten) &&
      cp.totalRowsWritten < cp.expectedTotalRows;
    const resumeStartPage =
      cp &&
      cp.status !== "completed" &&
      Number.isFinite(cp.lastCompletedPage) &&
      cp.lastCompletedPage >= 0
        ? needsFullRecovery
          ? 1
          : cp.lastCompletedPage + 1
        : 1;
    if (needsFullRecovery) {
      console.warn(
        `[parallel worker] Recovery mode: committed ${cp.totalRowsWritten}/${cp.expectedTotalRows}. Restarting Bar scan from page 1 (dedupe prevents duplicates).`
      );
    }
    if (resumeStartPage > 1) {
      console.log(
        `[parallel worker] Resume from page ${resumeStartPage} (checkpoint lastCompletedPage=${cp.lastCompletedPage}).`
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
    const needsFullRecovery =
      cp &&
      Number.isFinite(cp.expectedTotalRows) &&
      Number.isFinite(cp.totalRowsWritten) &&
      cp.totalRowsWritten < cp.expectedTotalRows;
    const resumeStartPage =
      cp &&
      cp.status !== "completed" &&
      Number.isFinite(cp.lastCompletedPage) &&
      cp.lastCompletedPage >= 0
        ? needsFullRecovery
          ? 1
          : cp.lastCompletedPage + 1
        : 1;
    if (needsFullRecovery) {
      console.warn(
        `  Recovery mode: committed ${cp.totalRowsWritten}/${cp.expectedTotalRows}. Restarting Bar scan from page 1 (dedupe prevents duplicates).`
      );
    }
    if (resumeStartPage > 1) {
      console.log(
        `  Checkpoint: resume from page ${resumeStartPage} (lastCompletedPage was ${cp.lastCompletedPage}).`
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
