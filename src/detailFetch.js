import path from "path";
import { mkdir, writeFile, readFile } from "fs/promises";
import {
  DETAIL_BAR_END_MAX_ATTEMPTS,
  DETAIL_FETCH_MAX_ATTEMPTS,
  DETAIL_PAGE_RETRY_SWEEPS,
  SAVE_JSON,
} from "./config.js";
import { DIR_EXTRACTED } from "./paths.js";
import {
  SEL_RESULT_CARDS,
  SEL_RESULT_CARD_INFO_LINK,
  delay,
  extractSummaryFromResultCardByIndex,
  getVisibleCardsCount,
  loadBarRecordsFromCsvFile,
  writeJson,
} from "./utils.js";

const OUTPUT_DIR = DIR_EXTRACTED;

/**
 * Parse `PrimeFaces.ab({ ... })` argument object from an onclick attribute string.
 */
export function parsePrimeFacesAbFromOnclick(onclick) {
  if (!onclick || typeof onclick !== "string") return null;
  const idx = onclick.indexOf("PrimeFaces.ab(");
  if (idx < 0) return null;
  let depth = 0;
  let start = -1;
  for (let j = idx; j < onclick.length; j += 1) {
    const ch = onclick[j];
    if (ch === "{") {
      if (depth === 0) start = j;
      depth += 1;
    } else if (ch === "}") {
      depth -= 1;
      if (depth === 0 && start >= 0) {
        const slice = onclick.slice(start, j + 1);
        try {
          return Function(`"use strict"; return (${slice});`)();
        } catch {
          return null;
        }
      }
    }
  }
  return null;
}

/**
 * Run PrimeFaces.ab in-page (same session cookies), poll `#resultDetailForm` until HTML updates.
 * Fallback: JSF `fetch` partial POST with ViewState.
 */
export async function fetchDetailHtmlForAbConfig(page, abCfg) {
  if (!abCfg || typeof abCfg !== "object") {
    throw new Error("Invalid PrimeFaces.ab config");
  }

  const viaAb = await page.evaluate(async (cfg) => {
    if (typeof PrimeFaces === "undefined" || !PrimeFaces.ab) {
      return { ok: false, reason: "no PrimeFaces.ab", html: "" };
    }
    const el0 = document.getElementById("resultDetailForm");
    const len0 = (el0?.innerHTML || "").length;
    PrimeFaces.ab(cfg);
    const deadline = Date.now() + 25000;
    while (Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 90));
      const el = document.getElementById("resultDetailForm");
      const html = el?.innerHTML || "";
      if (html.length > 120 && html.length !== len0) {
        return { ok: true, html: el.outerHTML };
      }
      if (html.length > 120 && len0 === 0) {
        return { ok: true, html: el.outerHTML };
      }
    }
    const el = document.getElementById("resultDetailForm");
    const html = el?.innerHTML || "";
    return {
      ok: html.length > 40,
      html: el ? el.outerHTML : "",
      reason: "timeout_poll",
    };
  }, abCfg);

  if (viaAb.ok && viaAb.html && viaAb.html.length > 40) {
    return viaAb.html;
  }

  const s = abCfg.s || abCfg.source;
  const f = abCfg.f || abCfg.formId || "resultForm";
  const u = abCfg.u || abCfg.update;
  if (!s || !u) throw new Error("Cannot build JSF partial: missing s/u");

  return await page.evaluate(
    async (source, formId, render) => {
      const vsEl = document.querySelector(
        'input[name="javax.faces.ViewState"]'
      );
      const viewState = vsEl ? vsEl.value : "";
      const search = window.location.search || "";
      const action =
        (document.getElementById(formId) || document.querySelector("form"))
          ?.getAttribute("action") || window.location.pathname + search;
      const url = new URL(action, window.location.origin).href;

      const body = new URLSearchParams();
      body.set("javax.faces.partial.ajax", "true");
      body.set("javax.faces.source", source);
      body.set("javax.faces.partial.execute", source);
      body.set("javax.faces.partial.render", render);
      body.set("javax.faces.behavior.event", "click");
      body.set("javax.faces.partial.event", "click");
      body.set("javax.faces.ViewState", viewState);
      body.set(formId, formId);

      const res = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
          "Faces-Request": "partial",
          "X-Requested-With": "XMLHttpRequest",
        },
        body: body.toString(),
        credentials: "same-origin",
      });
      const text = await res.text();
      const lower = text.toLowerCase();
      if (lower.includes("partial-response") || lower.includes("<update")) {
        const p = new DOMParser();
        const doc = p.parseFromString(text, "text/xml");
        if (doc.querySelector("parsererror")) {
          const hdoc = p.parseFromString(text, "text/html");
          const cdata = hdoc.querySelector("update");
          return cdata ? cdata.textContent || "" : text;
        }
        const updates = doc.querySelectorAll("update");
        for (const upd of updates) {
          const id = upd.getAttribute("id") || "";
          if (id.includes("resultDetailForm")) {
            return upd.textContent || "";
          }
        }
      }
      return text;
    },
    s,
    f,
    u
  );
}

export async function extractDetailDataFromResultDetailHtml(page, html, debug) {
  if (!html || html.length < 20) {
    return { detailData: {}, detailRawText: "" };
  }
  return await page.evaluate(
    (fragment, dbg) => {
      const norm = (x) => (x ?? "").replace(/\s+/g, " ").trim();
      let doc = new DOMParser().parseFromString(fragment, "text/html");
      let root =
        doc.getElementById("resultDetailForm") ||
        doc.querySelector("#resultDetailForm");
      if (!root) {
        doc = new DOMParser().parseFromString(
          `<div id="wrap">${fragment}</div>`,
          "text/html"
        );
        root =
          doc.getElementById("wrap")?.querySelector("#resultDetailForm") ||
          doc.body;
      }
      if (!root) return { detailData: {}, detailRawText: "" };
      const detailData = {};
      root.querySelectorAll("tr").forEach((tr) => {
        const cells = tr.querySelectorAll("th, td");
        if (cells.length >= 2) {
          const k = norm(cells[0].innerText).replace(/:\s*$/u, "");
          const v = norm(cells[1].innerText);
          if (k && v) detailData[k] = v;
        }
      });
      root.querySelectorAll("dl dt").forEach((dt) => {
        const dd = dt.nextElementSibling;
        if (dd && dd.tagName === "DD") {
          const k = norm(dt.innerText).replace(/:\s*$/u, "");
          const v = norm(dd.innerText);
          if (k && v) detailData[k] = v;
        }
      });
      root.querySelectorAll("label").forEach((lab) => {
        const id = lab.getAttribute("for");
        if (id) {
          const inp = root.ownerDocument.getElementById(id);
          if (inp && root.contains(inp)) {
            const k = norm(lab.innerText).replace(/:\s*$/u, "");
            let v = "";
            if (inp.tagName === "SELECT") {
              const opt = inp.options[inp.selectedIndex];
              v = opt ? norm(opt.textContent) : "";
            } else {
              v = norm(inp.value || inp.innerText);
            }
            if (k && v) detailData[k] = v;
          }
        }
      });
      const raw = norm(root.innerText);
      const detailRawText = dbg && raw.length > 0 ? raw : "";
      return { detailData, detailRawText };
    },
    html,
    Boolean(debug)
  );
}

async function appendMissed(missed) {
  await mkdir(OUTPUT_DIR, { recursive: true });
  const p = path.join(OUTPUT_DIR, "missed-records.json");
  let list = [];
  try {
    const raw = await readFile(p, "utf8").catch(() => "[]");
    list = JSON.parse(raw);
  } catch {
    list = [];
  }
  list.push(...missed);
  await writeFile(p, `${JSON.stringify(list, null, 2)}\n`, "utf8");
}

async function readLinkMeta(page, cardIndex) {
  return await page.evaluate(
    (idx, cardSel, linkSel) => {
      function visible(el) {
        if (!el) return false;
        const st = window.getComputedStyle(el);
        const r = el.getBoundingClientRect();
        return (
          el.offsetParent !== null &&
          st.visibility !== "hidden" &&
          st.display !== "none" &&
          r.width > 0 &&
          r.height > 0
        );
      }
      const roots = [...document.querySelectorAll(cardSel)].filter(visible);
      const card = roots[idx];
      if (!card) return null;
      const link = card.querySelector(linkSel);
      if (!link) return null;
      return {
        infoLinkId: link.id || "",
        infoOnclick: link.getAttribute("onclick") || "",
        infoHref: link.getAttribute("href") || "",
      };
    },
    cardIndex,
    SEL_RESULT_CARDS,
    SEL_RESULT_CARD_INFO_LINK
  );
}

/**
 * Fetch detail for one card; multiple attempts with backoff.
 */
async function fetchDetailWithAttempts(page, abCfg, maxAttempts) {
  let detailHtml = "";
  let lastErr = null;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      if (abCfg) {
        detailHtml = await fetchDetailHtmlForAbConfig(page, abCfg);
      } else {
        throw new Error("no PrimeFaces.ab in onclick");
      }
      if (detailHtml && detailHtml.length > 40) break;
      lastErr = new Error("empty detail html");
    } catch (e) {
      lastErr = e;
    }
    const backoff = 400 * 2 ** attempt;
    await delay(backoff);
  }
  return { detailHtml, lastErr };
}

/**
 * One pass over the page: extract summaries + detail for each visible card.
 * Returns records in card order and a list of pending retry payloads.
 */
async function scrapeResultPageFirstPass(
  page,
  selectedBar,
  pageNumber,
  debug,
  statsRef,
  maxAttempts
) {
  const count = await getVisibleCardsCount(page);
  const records = new Array(count).fill(null);
  const pending = [];

  for (let i = 0; i < count; i += 1) {
    let summary = null;
    try {
      summary = await extractSummaryFromResultCardByIndex(page, i, selectedBar);
    } catch (e) {
      const msg = String(e?.message || e);
      console.warn(
        `summary_failed bar="${selectedBar}" page=${pageNumber} card=${i} err="${msg}"`
      );
      pending.push({
        type: "summary",
        cardIndex: i,
        pageNumber,
        retryCount: 0,
        error: msg,
      });
      continue;
    }
    if (!summary) {
      console.warn(
        `summary_empty bar="${selectedBar}" page=${pageNumber} card=${i}`
      );
      pending.push({
        type: "summary",
        cardIndex: i,
        pageNumber,
        retryCount: 0,
        error: "summary_empty",
      });
      continue;
    }
    statsRef.summaryRows += 1;

    const meta = await readLinkMeta(page, i);
    if (!meta) {
      statsRef.failedDetail += 1;
      pending.push({
        type: "detail",
        cardIndex: i,
        summary,
        infoOnclick: "",
        infoLinkId: "",
        error: "no_info_link",
        retryCount: 0,
        pageNumber,
      });
      records[i] = {
        ...summary,
        detailData: {},
        detailError: "no_info_link",
      };
      continue;
    }

    const abCfg = parsePrimeFacesAbFromOnclick(meta.infoOnclick);
    const detailId =
      abCfg?.s || meta.infoLinkId || `page${pageNumber}-card${i}`;

    const { detailHtml, lastErr } = await fetchDetailWithAttempts(
      page,
      abCfg,
      maxAttempts
    );

    if (!detailHtml || detailHtml.length < 40) {
      pending.push({
        type: "detail",
        cardIndex: i,
        summary,
        infoOnclick: meta.infoOnclick || "",
        infoLinkId: meta.infoLinkId || "",
        error: String(lastErr?.message || lastErr || "detail_fetch_failed"),
        retryCount: 0,
        pageNumber,
        detailId,
      });
      records[i] = null;
      continue;
    }

    const { detailData, detailRawText } =
      await extractDetailDataFromResultDetailHtml(page, detailHtml, debug);
    statsRef.detailOk += 1;
    const merged = {
      ...summary,
      detailData: detailData && Object.keys(detailData).length ? detailData : {},
      infoSourceId: detailId,
    };
    if (debug && detailRawText) merged.detailRawText = detailRawText;
    records[i] = merged;
  }

  return { records, pending };
}

/**
 * Additional sweeps: retry pending items while still on the same results page.
 */
async function sweepPageRetries(
  page,
  pending,
  selectedBar,
  pageNumber,
  debug,
  statsRef,
  maxAttempts,
  records
) {
  const stillPending = [];
  for (const item of pending) {
    item.retryCount = (item.retryCount || 0) + 1;
    if (item.type === "summary") {
      try {
        const summary = await extractSummaryFromResultCardByIndex(
          page,
          item.cardIndex,
          selectedBar
        );
        if (!summary) throw new Error("summary_empty");
        // After recovering summary, attempt detail right away.
        const meta = await readLinkMeta(page, item.cardIndex);
        if (!meta) {
          stillPending.push({
            ...item,
            error: "no_info_link_after_summary_retry",
          });
          continue;
        }
        const abCfg = parsePrimeFacesAbFromOnclick(meta.infoOnclick);
        const detailId =
          abCfg?.s ||
          meta.infoLinkId ||
          `page${pageNumber}-card${item.cardIndex}`;
        const { detailHtml, lastErr } = await fetchDetailWithAttempts(
          page,
          abCfg,
          maxAttempts
        );
        if (!detailHtml || detailHtml.length < 40) {
          stillPending.push({
            type: "detail",
            cardIndex: item.cardIndex,
            summary,
            infoOnclick: meta.infoOnclick || "",
            infoLinkId: meta.infoLinkId || "",
            error: String(
              lastErr?.message || lastErr || "detail_fetch_failed_after_summary"
            ),
            retryCount: item.retryCount,
            pageNumber,
            detailId,
          });
          continue;
        }
        const { detailData, detailRawText } =
          await extractDetailDataFromResultDetailHtml(page, detailHtml, debug);
        statsRef.detailOk += 1;
        const merged = {
          ...summary,
          detailData:
            detailData && Object.keys(detailData).length ? detailData : {},
          infoSourceId: detailId,
        };
        if (debug && detailRawText) merged.detailRawText = detailRawText;
        const idx = item.cardIndex;
        if (idx >= 0 && idx < records.length) records[idx] = merged;
      } catch (e) {
        stillPending.push({
          ...item,
          error: String(e?.message || e || "summary_retry_failed"),
        });
      }
      continue;
    }

    const abCfg = parsePrimeFacesAbFromOnclick(item.infoOnclick);
    const { detailHtml, lastErr } = await fetchDetailWithAttempts(
      page,
      abCfg,
      maxAttempts
    );
    if (!detailHtml || detailHtml.length < 40) {
      stillPending.push({
        ...item,
        error: String(lastErr?.message || lastErr || "detail_fetch_failed"),
      });
      continue;
    }
    const { detailData, detailRawText } =
      await extractDetailDataFromResultDetailHtml(page, detailHtml, debug);
    statsRef.detailOk += 1;
    const merged = {
      ...item.summary,
      detailData: detailData && Object.keys(detailData).length ? detailData : {},
      infoSourceId: item.detailId || item.infoLinkId,
    };
    if (debug && detailRawText) merged.detailRawText = detailRawText;
    const idx = item.cardIndex;
    if (idx >= 0 && idx < records.length) records[idx] = merged;
  }
  return stillPending;
}

/**
 * Extract all rows for the current results page (AJAX detail), with page-level retry sweeps.
 * Does not write CSV — caller appends. Pushes still-failing items onto barRetryQueue.
 */
export async function extractResultPageBatch(
  page,
  selectedBar,
  {
    pageNumber,
    debug,
    statsRef,
    barRetryQueue,
    maxAttempts = DETAIL_FETCH_MAX_ATTEMPTS,
    pageSweeps = DETAIL_PAGE_RETRY_SWEEPS,
  }
) {
  let { records, pending } = await scrapeResultPageFirstPass(
    page,
    selectedBar,
    pageNumber,
    debug,
    statsRef,
    maxAttempts
  );

  let sweep = 0;
  while (pending.length && sweep < pageSweeps) {
    pending = await sweepPageRetries(
      page,
      pending,
      selectedBar,
      pageNumber,
      debug,
      statsRef,
      maxAttempts,
      records
    );
    sweep += 1;
  }

  const out = [];
  for (let i = 0; i < records.length; i += 1) {
    if (records[i]) out.push(records[i]);
  }

  for (const p of pending) {
    if (p.type === "summary") {
      statsRef.summaryFailed = (statsRef.summaryFailed || 0) + 1;
      barRetryQueue.push({
        type: "summary",
        bar: selectedBar,
        pageNumber: p.pageNumber,
        cardIndex: p.cardIndex,
        error: p.error,
        retryCount: p.retryCount || 0,
      });
    } else {
      statsRef.failedDetail += 1;
      barRetryQueue.push({
        type: "detail",
        bar: selectedBar,
        pageNumber: p.pageNumber,
        cardIndex: p.cardIndex,
        summary: p.summary,
        infoOnclick: p.infoOnclick,
        infoLinkId: p.infoLinkId,
        detailId: p.detailId,
        error: p.error,
        retryCount: p.retryCount || 0,
      });
    }
  }

  return { rows: out, pagePendingDetailFailures: pending.length };
}

/**
 * After all pages: retry bar-level queue using stored onclick (same session).
 */
export async function drainBarEndRetryQueue(
  page,
  selectedBar,
  barRetryQueue,
  debug,
  statsRef
) {
  const missed = [];
  const recoveredRows = [];
  const queue = [...barRetryQueue];
  barRetryQueue.length = 0;

  // Keep summary failures so they can be recovered by a full re-scan (page 1 + dedupe) on next run.
  const summaryOnly = queue.filter((q) => q && q.type === "summary");
  for (const s of summaryOnly) {
    missed.push({
      bar: selectedBar,
      page: s.pageNumber,
      cardIndex: s.cardIndex,
      summary: null,
      error: `summary_unresolved: ${s.error || "unknown"}`,
    });
    barRetryQueue.push(s);
  }

  const detailQueue = queue.filter((q) => !q || q.type !== "summary");

  for (const item of detailQueue) {
    let ok = false;
    let lastErr = null;
    for (let a = 0; a < DETAIL_BAR_END_MAX_ATTEMPTS && !ok; a += 1) {
      try {
        const abCfg = parsePrimeFacesAbFromOnclick(item.infoOnclick);
        if (!abCfg) throw new Error("no PrimeFaces.ab");
        const detailHtml = await fetchDetailHtmlForAbConfig(page, abCfg);
        if (!detailHtml || detailHtml.length < 40) {
          lastErr = new Error("empty detail html");
          await delay(500 * (a + 1));
          continue;
        }
        const { detailData, detailRawText } =
          await extractDetailDataFromResultDetailHtml(page, detailHtml, debug);
        statsRef.detailOk += 1;
        statsRef.failedDetail = Math.max(0, statsRef.failedDetail - 1);
        const merged = {
          ...item.summary,
          detailData:
            detailData && Object.keys(detailData).length ? detailData : {},
          infoSourceId: item.detailId || item.infoLinkId,
        };
        if (debug && detailRawText) merged.detailRawText = detailRawText;
        recoveredRows.push(merged);
        ok = true;
      } catch (e) {
        lastErr = e;
        await delay(600 * (a + 1));
      }
    }
    if (!ok) {
      missed.push({
        bar: selectedBar,
        page: item.pageNumber,
        cardIndex: item.cardIndex,
        summary: item.summary,
        error: String(lastErr?.message || lastErr || "bar_end_retry_failed"),
      });
      recoveredRows.push({
        ...item.summary,
        detailData: {},
        detailError: String(
          lastErr?.message || lastErr || "bar_end_retry_failed"
        ),
      });
    }
  }

  if (missed.length) {
    await appendMissed(missed);
  }
  return { recoveredRows };
}

export async function writeBarJsonSnapshot(baseName, csvPath) {
  if (!SAVE_JSON) return;
  const rows = await loadBarRecordsFromCsvFile(csvPath);
  await writeJson(path.join(OUTPUT_DIR, `${baseName}.json`), rows);
}

export { OUTPUT_DIR };
