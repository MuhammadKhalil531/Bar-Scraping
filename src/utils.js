import {
  mkdir,
  writeFile as fsWriteFile,
  readFile as fsReadFile,
  appendFile as fsAppendFile,
  stat as fsStat,
  rename as fsRename,
} from "fs/promises";
import path from "path";
import { DIR_EXTRACTED } from "./paths.js";

export const SEL_BAR_ROOT = "#searchForm\\:ddRAKammer";
export const SEL_BAR_SELECT = "#searchForm\\:ddRAKammer_input";
export const SEL_BAR_LABEL = "#searchForm\\:ddRAKammer_label";
/** PrimeFaces p:selectOneMenu visible pieces */
export const SEL_BAR_TRIGGER = "#searchForm\\:ddRAKammer .ui-selectonemenu-trigger";
export const SEL_BAR_PANEL = "#searchForm\\:ddRAKammer_panel";
export const SEL_BAR_ITEMS = "#searchForm\\:ddRAKammer_panel .ui-selectonemenu-item";
export const SEL_RESULTS_PANEL = "#resultForm\\:pnlResultList_content";
export const SEL_RESULTS_LIST = "#resultForm\\:dlResultList";
export const SEL_RESULT_CARDS = "#resultForm\\:dlResultList .resultCard";
export const SEL_PAGINATOR_BOTTOM = "#resultForm\\:dlResultList_paginator_bottom";
export const SEL_PAGINATOR_NEXT =
  "#resultForm\\:dlResultList_paginator_bottom .ui-paginator-next";
export const SEL_PAGINATOR_ACTIVE_PAGE =
  "#resultForm\\:dlResultList_paginator_bottom .ui-paginator-page.ui-state-active";
export const SEL_PAGINATOR_CURRENT =
  "#resultForm\\:dlResultList_paginator_bottom .ui-paginator-current";

/** PrimeFaces result detail overlay */
export const SEL_DETAIL_FORM = "#resultDetailForm";
export const SEL_RESULT_CARD_INFO_LINK = ".resultCardDetailLink";

export function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Safe filename for Windows/macOS/Linux; keeps umlauts, strips path/control chars.
 */
export function sanitizeFilename(name) {
  const trimmed = (name ?? "").trim();
  const cleaned = trimmed
    .normalize("NFKC")
    .replace(/[/\\:*?"<>|\u0000-\u001f]+/g, "")
    .replace(/\s+/g, "_")
    .replace(/_+/g, "_");
  const safe = cleaned.slice(0, 120);
  return safe.length > 0 ? safe : "export";
}

export async function ensureDir(dirPath) {
  await mkdir(dirPath, { recursive: true });
}

async function atomicWriteTextFile(filePath, text) {
  await ensureDir(path.dirname(filePath));
  const dir = path.dirname(filePath);
  const base = path.basename(filePath);
  const tmp = path.join(
    dir,
    `.${base}.tmp-${process.pid}-${Date.now()}-${Math.random()
      .toString(16)
      .slice(2)}`
  );
  await fsWriteFile(tmp, text, "utf8");
  await fsRename(tmp, filePath);
}

export async function writeJson(filePath, data) {
  const text = `${JSON.stringify(data, null, 2)}\n`;
  await atomicWriteTextFile(filePath, text);
}

function escapeCsvField(value) {
  if (value === null || value === undefined) return "";
  const s = String(value);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

/** Modal tab titles — never emitted as label keys or as address lines. */
const DETAIL_TAB_NAMES = new Set([
  "Data",
  "Branches",
  "Delivery representative",
  "Liquidator",
  "Deputy",
  "Daten",
  "Zweigstellen",
  "Zustellungsbevollmächtigter",
  "Stellvertreter",
]);

/**
 * Ordered flat columns for the main export CSV (summary + mapped detail fields).
 */
export const CSV_FLAT_COLUMN_ORDER = [
  "fullName",
  "academicTitle",
  "firstName",
  "lastName",
  "profession",
  "companyOrOffice",
  "street",
  "postalCode",
  "city",
  "selectedBar",
  "formOfAddress",
  "professionalTitle",
  "interestForCourtDefenceCounsel",
  "firstNameLastName",
  "dateOfAdmission",
  "dateOfFirstAdmission",
  "barMembership",
  "lawOffice",
  "officeAddress",
  "telephone",
  "mobilePhone",
  "telefax",
  "email",
  "internetAddress",
  "beaSafeId",
];

/**
 * Dialog label variants (normalized match) → flat CSV key for detail section.
 * English strings from spec first; common German UI variants as fallbacks.
 */
const DETAIL_LABELS_TO_FLAT_KEY = [
  {
    key: "academicTitle",
    labels: [
      "Academic title",
      "Akademischer Titel",
      "Akademischer Grad",
      "Akademische(r) Grad",
    ],
  },
  {
    key: "firstName",
    labels: ["First name", "Vorname", "Given name"],
  },
  {
    key: "lastName",
    labels: ["Last name", "Nachname", "Family name", "Surname"],
  },
  {
    key: "formOfAddress",
    labels: ["Form of address", "Anrede"],
  },
  {
    key: "professionalTitle",
    labels: [
      "Professional title",
      "Akademischer Grad",
      "Berufliche Bezeichnung",
    ],
  },
  {
    key: "interestForCourtDefenceCounsel",
    labels: [
      "Interest for getting appointed by court as defence counsel",
      "Interesse an Bestellung als Pflichtverteidiger",
      "Interesse an gerichtlicher Bestellung als Pflichtverteidiger",
    ],
  },
  {
    key: "firstNameLastName",
    labels: ["First name, Last name", "Vorname, Name", "Vorname, Nachname"],
  },
  { key: "dateOfAdmission", labels: ["Date of admission", "Zulassungsdatum"] },
  {
    key: "dateOfFirstAdmission",
    labels: ["Date of first admission", "Datum der Erstzulassung"],
  },
  {
    key: "barMembership",
    labels: ["Bar membership", "Mitgliedschaft", "Rechtsanwaltskammer"],
  },
  { key: "lawOffice", labels: ["Law office", "Kanzlei"] },
  {
    key: "officeAddress",
    labels: ["Office address", "Geschäftsanschrift", "Büroadresse"],
  },
  { key: "telephone", labels: ["Telephone", "Telefon"] },
  { key: "mobilePhone", labels: ["Mobile phone", "Mobiltelefon", "Mobilfunk"] },
  { key: "telefax", labels: ["Telefax", "Fax"] },
  { key: "email", labels: ["E-mail", "E-Mail", "Email"] },
  {
    key: "internetAddress",
    labels: ["Internet address", "Internetadresse", "Internet"],
  },
  {
    key: "beaSafeId",
    labels: ["beA SAFE-ID", "beA-SAFE-ID", "bea SAFE-ID", "beA Safe-ID"],
  },
];

function normalizeDetailLabel(s) {
  return String(s ?? "")
    .replace(/:\s*$/u, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function sanitizeCsvPlainText(value) {
  const t = String(value ?? "")
    .trim()
    .replace(/\r?\n+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return t;
}

/** Strip modal button / noise from detail-derived cells only */
function sanitizeDetailCsvPlain(value) {
  let t = sanitizeCsvPlainText(value);
  if (t === "Info") return "";
  return t;
}

/**
 * Flatten multiline address; drop standalone tab-title lines (modal chrome).
 */
export function normalizeOfficeAddressForCsv(value) {
  const raw = String(value ?? "");
  const parts = [];
  for (const line of raw.split(/\r?\n/)) {
    const t = line.trim();
    if (!t) continue;
    if (DETAIL_TAB_NAMES.has(t)) continue;
    parts.push(t);
  }
  let out = parts.join(" ").replace(/\s+/g, " ").trim();

  // Insert missing spaces in common German address patterns produced by HTML flattening.
  // Examples: "Str. 2297318 KitzingenDE" -> "Str. 22 97318 Kitzingen DE"
  // 1) house number + postal code glued: 22 + 97318 => 2297318
  out = out.replace(/\b(\d{1,6})(\d{5})\b/g, "$1 $2");
  // 2) postal code + city glued: 97318Kitzingen
  out = out.replace(/(\d{5})(?=[\p{L}])/gu, "$1 ");
  // 3) city + country code glued: KitzingenDE
  out = out.replace(/([\p{L}\-]+)(DE)\b/gu, "$1 $2");

  return out.replace(/\s+/g, " ").trim();
}

function findDetailValue(detailData, flatKey) {
  if (!detailData || typeof detailData !== "object") return "";
  const group = DETAIL_LABELS_TO_FLAT_KEY.find((g) => g.key === flatKey);
  if (!group) return "";
  const candidates = new Set(
    group.labels.map((l) => normalizeDetailLabel(l))
  );
  for (const [k, v] of Object.entries(detailData)) {
    const kt = (k ?? "").trim();
    if (DETAIL_TAB_NAMES.has(kt)) continue;
    if (candidates.has(normalizeDetailLabel(k))) {
      return v == null ? "" : String(v);
    }
  }
  return "";
}

/**
 * Split combined "First name, Last name" / "Vorname, Nachname" detail text for CSV columns.
 */
function splitCombinedFirstLastName(combinedRaw) {
  const t = sanitizeDetailCsvPlain(combinedRaw);
  if (!t) return { firstName: "", lastName: "" };
  if (/,/.test(t)) {
    const parts = t.split(",").map((s) => s.trim()).filter(Boolean);
    if (parts.length >= 2) {
      return {
        firstName: parts[0],
        lastName: parts.slice(1).join(", "),
      };
    }
  }
  const words = t.split(/\s+/).filter(Boolean);
  if (words.length >= 2) {
    return {
      firstName: words[0],
      lastName: words.slice(1).join(" "),
    };
  }
  return { firstName: t, lastName: "" };
}

/**
 * Map a scraped record (summary + optional detailData object) to one CSV row object.
 */
export function recordToFlatCsvRow(record) {
  const d = record?.detailData;
  const row = {};
  row.fullName = sanitizeCsvPlainText(record?.fullName);
  row.profession = sanitizeCsvPlainText(record?.profession);
  row.companyOrOffice = sanitizeCsvPlainText(record?.companyOrOffice);
  row.street = sanitizeCsvPlainText(record?.street);
  row.postalCode = sanitizeCsvPlainText(record?.postalCode);
  row.city = sanitizeCsvPlainText(record?.city);
  row.selectedBar = sanitizeCsvPlainText(record?.selectedBar);
  for (const { key } of DETAIL_LABELS_TO_FLAT_KEY) {
    let v = findDetailValue(d, key);
    if (key === "officeAddress") {
      row[key] = sanitizeDetailCsvPlain(normalizeOfficeAddressForCsv(v));
    } else {
      row[key] = sanitizeDetailCsvPlain(v);
    }
  }
  let fn = row.firstName || "";
  let ln = row.lastName || "";
  if (!fn && !ln) {
    const sp = splitCombinedFirstLastName(row.firstNameLastName);
    fn = sp.firstName;
    ln = sp.lastName;
  } else {
    const sp = splitCombinedFirstLastName(row.firstNameLastName);
    if (!fn) fn = sp.firstName;
    if (!ln) ln = sp.lastName;
  }
  row.firstName = sanitizeDetailCsvPlain(fn);
  row.lastName = sanitizeDetailCsvPlain(ln);
  return row;
}

export async function writeCsv(filePath, rows) {
  const headers = CSV_FLAT_COLUMN_ORDER;
  const lines = [headers.join(",")];
  for (const row of rows) {
    const flat = recordToFlatCsvRow(row);
    lines.push(headers.map((h) => escapeCsvField(flat[h] ?? "")).join(","));
  }
  await ensureDir(path.dirname(filePath));
  await fsWriteFile(filePath, lines.join("\n") + "\n", "utf8");
}

/**
 * Append flat CSV rows; writes header only when the file is missing or empty.
 * Skips only when recordDedupeFingerprint matches an existing row (true duplicate).
 * Mutates seenState.fingerprints and seenState.byStableKey.
 */
export async function appendCsvRecords(filePath, records, seenState) {
  const headers = CSV_FLAT_COLUMN_ORDER;
  const lines = [];
  let appended = 0;
  let deduped = 0;
  let collisionDetected = 0;
  const collisionKeys = new Set();
  let rowsPreviouslyDropped = 0;

  for (const record of records) {
    const sk = stableRecordKey(record);
    const fp = recordDedupeFingerprint(record);

    if (seenState.fingerprints.has(fp)) {
      deduped += 1;
      console.warn(
        `dedupe_skip fingerprint bar="${record?.selectedBar ?? ""}" name="${record?.fullName ?? ""}"`
      );
      continue;
    }

    const fpsForSk = seenState.byStableKey.get(sk);
    if (fpsForSk && fpsForSk.size > 0 && !fpsForSk.has(fp)) {
      collisionDetected += 1;
      collisionKeys.add(sk);
      rowsPreviouslyDropped += 1;
      const prov =
        Number.isFinite(record?.pageNumber) && Number.isFinite(record?.cardIndex)
          ? ` page=${record.pageNumber} card=${record.cardIndex}`
          : "";
      console.warn(
        `stable_key_collision collisionDetected=true collisionKey="${sk}" rowsKept+=1 rowsPreviouslyDropped+=1 (would have skipped under stable-key-only dedupe)${prov}`
      );
    }

    if (!seenState.byStableKey.has(sk)) seenState.byStableKey.set(sk, new Set());
    seenState.byStableKey.get(sk).add(fp);
    seenState.fingerprints.add(fp);

    const flat = recordToFlatCsvRow(record);
    lines.push(headers.map((h) => escapeCsvField(flat[h] ?? "")).join(","));
    appended += 1;
  }

  if (!lines.length) {
    return {
      appended: 0,
      deduped,
      collisionDetected,
      collisionKeys: [...collisionKeys],
      rowsKept: 0,
      rowsPreviouslyDropped,
    };
  }
  await ensureDir(path.dirname(filePath));
  let needHeader = true;
  try {
    const st = await fsStat(filePath);
    needHeader = st.size === 0;
  } catch {
    needHeader = true;
  }
  const chunk =
    (needHeader ? `${headers.join(",")}\n` : "") + `${lines.join("\n")}\n`;
  await fsAppendFile(filePath, chunk, "utf8");
  return {
    appended,
    deduped,
    collisionDetected,
    collisionKeys: [...collisionKeys],
    rowsKept: appended,
    rowsPreviouslyDropped,
  };
}

/**
 * One CSV row (RFC4180-style; our export never embeds raw newlines in a cell).
 */
function parseCsvDataLine(line) {
  const row = [];
  let field = "";
  let i = 0;
  while (i < line.length) {
    if (line[i] === '"') {
      i += 1;
      while (i < line.length) {
        if (line[i] === '"') {
          if (i + 1 < line.length && line[i + 1] === '"') {
            field += '"';
            i += 2;
          } else {
            i += 1;
            break;
          }
        } else {
          field += line[i];
          i += 1;
        }
      }
    } else {
      while (i < line.length && line[i] !== ",") {
        field += line[i];
        i += 1;
      }
    }
    row.push(field);
    field = "";
    if (i < line.length && line[i] === ",") i += 1;
  }
  return row;
}

/**
 * Parses full CSV text from disk (same shape as writeCsv output).
 * @returns {string[][]}
 */
export function parseCsvText(text) {
  const norm = text.replace(/^\uFEFF/, "");
  const lines = norm.split(/\r?\n/).filter((l) => l.length > 0);
  return lines.map(parseCsvDataLine);
}

function recordFromFlatCsvObject(obj) {
  const detailData = {};
  for (const group of DETAIL_LABELS_TO_FLAT_KEY) {
    const v = obj[group.key];
    if (v != null && String(v).trim() !== "") {
      detailData[group.labels[0]] = String(v);
    }
  }
  return {
    fullName: obj.fullName ?? "",
    profession: obj.profession ?? "",
    companyOrOffice: obj.companyOrOffice ?? "",
    street: obj.street ?? "",
    postalCode: obj.postalCode ?? "",
    city: obj.city ?? "",
    selectedBar: obj.selectedBar ?? "",
    detailData,
  };
}

/**
 * Load rows from a per-Bar CSV (same shape as writeCsv). Returns [] if missing/invalid.
 */
export async function loadBarRecordsFromCsvFile(csvPath) {
  let text;
  try {
    text = await fsReadFile(csvPath, "utf8");
  } catch {
    return [];
  }
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  const table = parseCsvText(text);
  if (!table.length) return [];
  const headers = table[0];
  const want = CSV_FLAT_COLUMN_ORDER;
  const idx = {};
  for (const h of want) {
    const j = headers.indexOf(h);
    idx[h] = j >= 0 ? j : -1;
  }
  if (idx.fullName < 0) {
    console.warn(`loadBarRecordsFromCsvFile: unexpected header in ${csvPath}`);
    return [];
  }
  const records = [];
  for (let r = 1; r < table.length; r++) {
    const cols = table[r];
    const obj = {};
    for (const h of want) {
      const j = idx[h];
      obj[h] = j >= 0 && j < cols.length ? cols[j] : "";
    }
    if (!Object.values(obj).some((v) => String(v ?? "").trim())) continue;
    records.push(recordFromFlatCsvObject(obj));
  }
  return records;
}

/**
 * Restore crawl counters from checkpoint (used when resuming the same Bar).
 */
export function applyCheckpointToCrawlStats(cp, crawlStats) {
  if (!cp || typeof cp !== "object") return;
  if (Number.isFinite(cp.detailOkCount)) crawlStats.detailOk = cp.detailOkCount;
  else if (Number.isFinite(cp.detailOk)) crawlStats.detailOk = cp.detailOk;
  if (Number.isFinite(cp.detailFailedCount)) crawlStats.failedDetail = cp.detailFailedCount;
  else if (Number.isFinite(cp.failedDetail)) crawlStats.failedDetail = cp.failedDetail;
  if (Number.isFinite(cp.summaryFailedCount))
    crawlStats.summaryFailed = cp.summaryFailedCount;
  if (Number.isFinite(cp.summaryRowsProcessed)) crawlStats.summaryRows = cp.summaryRowsProcessed;
  else if (Number.isFinite(cp.summaryRows)) crawlStats.summaryRows = cp.summaryRows;
}

/**
 * Stable row identity: selectedBar + fullName + street + postalCode + city.
 */
export function stableRecordKey(record) {
  const parts = [
    record.selectedBar,
    record.fullName,
    record.street,
    record.postalCode,
    record.city,
  ].map((p) => (p ?? "").trim());
  return parts.join("|");
}

function collapseFpWhitespace(value) {
  return String(value ?? "")
    .replace(/\s+/g, " ")
    .trim();
}

function detailDataSignature(detailData) {
  if (!detailData || typeof detailData !== "object") return "";
  return Object.keys(detailData)
    .sort()
    .map((k) => `${k}=${collapseFpWhitespace(detailData[k])}`)
    .join("|");
}

/**
 * Strong row identity for dedupe / resume. `stableRecordKey` is only a prefix
 * (grouping); several real records may share it — distinguish via content + provenance ids.
 */
export function recordDedupeFingerprint(record) {
  const sk = stableRecordKey(record);
  const flat = recordToFlatCsvRow(record);
  const flatSig = CSV_FLAT_COLUMN_ORDER.map((h) =>
    collapseFpWhitespace(flat[h])
  ).join("\x1f");
  const detailSig = detailDataSignature(record?.detailData);
  const rawBlock = [
    collapseFpWhitespace(record?.rawText),
    collapseFpWhitespace(record?.infoLinkText),
    collapseFpWhitespace(record?.infoSourceId),
    collapseFpWhitespace(record?.detailError),
  ].join("\x1f");
  const body = [flatSig, detailSig, rawBlock].join("\x1e");
  return `${sk}\x1e${body}`;
}

/** True if any committed fingerprint belongs to this summary stable key (audit vs CSV). */
export function fingerprintSetCoversStableKey(fingerprints, stableKey) {
  const prefix = `${stableKey}\x1e`;
  for (const fp of fingerprints) {
    if (fp.startsWith(prefix)) return true;
  }
  return false;
}

export function createEmptyDedupeState() {
  return {
    fingerprints: new Set(),
    byStableKey: new Map(),
  };
}

/**
 * Rebuild dedupe state from an existing per-Bar CSV (crash-safe resume).
 * Tracks full fingerprints and groups them by stableRecordKey for collision diagnostics.
 */
export async function loadSeenKeySetFromBarCsv(csvPath) {
  const rows = await loadBarRecordsFromCsvFile(csvPath);
  const state = createEmptyDedupeState();
  for (const r of rows) {
    const fp = recordDedupeFingerprint(r);
    const sk = stableRecordKey(r);
    state.fingerprints.add(fp);
    if (!state.byStableKey.has(sk)) state.byStableKey.set(sk, new Set());
    state.byStableKey.get(sk).add(fp);
  }
  return state;
}

export function dedupeRecords(records) {
  const seen = new Map();
  for (const r of records) {
    const fp = recordDedupeFingerprint(r);
    if (!seen.has(fp)) seen.set(fp, r);
  }
  return [...seen.values()];
}

export async function getSelectedBarInfo(page) {
  return await page.evaluate(
    (selectSel, labelSel) => {
      const labelEl = document.querySelector(labelSel);
      const labelText = labelEl ? labelEl.textContent.trim() : "";
      const sel = document.querySelector(selectSel);
      let value = "";
      let textFromSelect = "";
      if (sel && sel.tagName === "SELECT") {
        const idx = sel.selectedIndex;
        const opt = sel.options[idx];
        value = opt?.value ?? "";
        textFromSelect = opt ? opt.textContent.trim() : "";
      }
      const text = labelText || textFromSelect;
      if (!text && !value) return null;
      return { value, text, labelText, textFromSelect };
    },
    SEL_BAR_SELECT,
    SEL_BAR_LABEL
  );
}

export async function getTotalPages(page) {
  const total = await page.evaluate((pagSel) => {
    const root = document.querySelector(pagSel);
    if (!root) return 1;
    const pageLinks = [...root.querySelectorAll(".ui-paginator-page")].filter(
      (el) => el.offsetParent !== null
    );
    if (pageLinks.length) {
      const nums = pageLinks
        .map((p) => parseInt(p.textContent.trim(), 10))
        .filter((n) => Number.isFinite(n) && n > 0);
      if (nums.length) return Math.max(...nums);
    }
    const cur =
      root.querySelector(".ui-paginator-current") ||
      root.querySelector(".ui-paginator-page.ui-state-active");
    const txt = cur?.textContent?.trim() ?? "";
    const m = txt.match(
      /(?:\(|^|\s)(\d+)\s*(?:of|von|\/)\s*(\d+)|(\d+)\s*-\s*\d+\s*(?:of|von)\s*(\d+)/i
    );
    if (m) {
      const t = parseInt(m[2] || m[4], 10);
      if (Number.isFinite(t) && t > 0) return t;
    }
    return 1;
  }, SEL_PAGINATOR_BOTTOM);
  return Math.max(1, Number(total) || 1);
}

/** Parse "Entries 307 - 312 of 2838" / German-style paginator lines. */
export function parseEntriesReport(text) {
  if (!text || typeof text !== "string") return null;
  const n = text.replace(/\u00a0/g, " ").trim();
  const m = n.match(
    /(\d+)\s*[-–]\s*(\d+)\s+.*?(?:of|von)\s+([\d.]+)/i
  );
  if (!m) return null;
  const from = parseInt(m[1], 10);
  const to = parseInt(m[2], 10);
  const total = parseInt(String(m[3]).replace(/\./g, ""), 10);
  if (![from, to, total].every((x) => Number.isFinite(x))) return null;
  return { from, to, total };
}

/**
 * Reliable paginator + first-card snapshot (PrimeFaces sliding page buttons are NOT total pages).
 */
export async function getPaginatorSnapshot(page) {
  return await page.evaluate(
    (pagRoot, activeSel, currentSel, nextSel, cardSel) => {
      const root = document.querySelector(pagRoot);
      const activeEl = document.querySelector(activeSel);
      const currentEl = document.querySelector(currentSel);
      const nextEl = document.querySelector(nextSel);
      const firstCard = document.querySelector(cardSel);
      const activePage = activeEl?.textContent?.trim() ?? "";
      const currentReport = currentEl?.textContent?.replace(/\u00a0/g, " ").trim() ?? "";
      const firstCardPeek = firstCard
        ? (firstCard.innerText || "").replace(/\s+/g, " ").trim().slice(0, 160)
        : "";
      let nextDisabled = true;
      if (nextEl) {
        nextDisabled =
          nextEl.classList.contains("ui-state-disabled") ||
          nextEl.getAttribute("aria-disabled") === "true" ||
          nextEl.closest?.(".ui-state-disabled") != null;
      }
      return { activePage, currentReport, firstCardPeek, nextDisabled };
    },
    SEL_PAGINATOR_BOTTOM,
    SEL_PAGINATOR_ACTIVE_PAGE,
    SEL_PAGINATOR_CURRENT,
    SEL_PAGINATOR_NEXT,
    SEL_RESULT_CARDS
  );
}

export async function getActivePaginatorPageLabel(page) {
  return await page.evaluate(
    (activeSel, currentSel, pagRootSel) => {
      const a = document.querySelector(activeSel);
      if (a?.textContent?.trim()) return a.textContent.trim();
      const c = document.querySelector(currentSel);
      if (c?.textContent?.trim()) return c.textContent.trim();
      const root = document.querySelector(pagRootSel);
      if (!root) return "";
      const active = root.querySelector(".ui-paginator-page.ui-state-active");
      if (active) return active.textContent.trim();
      const cur = root.querySelector(".ui-paginator-current");
      return cur ? cur.textContent.trim() : "";
    },
    SEL_PAGINATOR_ACTIVE_PAGE,
    SEL_PAGINATOR_CURRENT,
    SEL_PAGINATOR_BOTTOM
  );
}

/**
 * Infer 1-based results page from paginator "Entries X – Y of Z" (or active page label).
 */
export async function inferCurrentResultPage1Based(page) {
  const snap = await getPaginatorSnapshot(page);
  const rep = parseEntriesReport(snap.currentReport);
  if (rep && Number.isFinite(rep.from) && Number.isFinite(rep.to)) {
    const span = Math.max(1, rep.to - rep.from + 1);
    return Math.max(1, Math.ceil(rep.from / span));
  }
  const n = parseInt(snap.activePage, 10);
  return Number.isFinite(n) && n > 0 ? n : 1;
}

/**
 * Clicks bottom "next" if not disabled.
 * Never keeps ElementHandles — always re-queries DOM in-page (avoids detached node crashes after AJAX).
 * @returns {Promise<boolean>} true if another page loaded
 */
export async function goToNextResultsPage(page, options = {}) {
  const pageDelayMs = options.pageDelayMs ?? 200;
  const actionDelayMs = options.actionDelayMs ?? 100;
  const maxRetries = Math.min(
    5,
    Math.max(1, options.maxRetries ?? 3)
  );

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    const snap = await getPaginatorSnapshot(page);
    if (snap.nextDisabled) return false;

    const prevActive = snap.activePage;
    const prevCurrent = snap.currentReport;
    const prevFirst = snap.firstCardPeek;

    const clicked = await page
      .evaluate((pagRoot) => {
        const root = document.querySelector(pagRoot);
        if (!root) return { ok: false, reason: "no paginator root" };
        const next = root.querySelector(".ui-paginator-next");
        if (!next) return { ok: false, reason: "no next" };
        const dis =
          next.classList.contains("ui-state-disabled") ||
          next.getAttribute("aria-disabled") === "true" ||
          Boolean(next.closest(".ui-state-disabled"));
        if (dis) return { ok: false, reason: "next disabled" };
        try {
          next.scrollIntoView({ block: "center", inline: "nearest" });
        } catch (_) {
          /* ignore scroll errors */
        }
        next.click();
        return { ok: true, reason: "" };
      }, SEL_PAGINATOR_BOTTOM)
      .catch((e) => ({ ok: false, reason: String(e?.message || e) }));

    if (!clicked?.ok) {
      if (clicked?.reason === "next disabled") return false;
      if (attempt === maxRetries) {
        console.warn(
          `goToNextResultsPage: could not click next after ${maxRetries} attempt(s): ${clicked?.reason ?? "?"}`
        );
        return false;
      }
      console.warn(
        `goToNextResultsPage: retry ${attempt}/${maxRetries} (paginator not ready — ${clicked?.reason ?? "?"})`
      );
      await delay(200 + 100 * attempt);
      continue;
    }

    try {
      await page.waitForFunction(
        (
          activeSel,
          currentSel,
          cardSel,
          pActive,
          pCurrent,
          pFirst
        ) => {
          const a = document.querySelector(activeSel)?.textContent?.trim() ?? "";
          const c = document
            .querySelector(currentSel)
            ?.textContent?.replace(/\u00a0/g, " ")
            .trim() ?? "";
          const card = document.querySelector(cardSel);
          const first = card
            ? (card.innerText || "").replace(/\s+/g, " ").trim().slice(0, 160)
            : "";
          const activeChanged =
            a !== "" && pActive !== "" && a !== pActive;
          const currentChanged =
            c !== "" && pCurrent !== "" && c !== pCurrent;
          const firstChanged =
            (first !== "" && pFirst !== "" && first !== pFirst) ||
            (pFirst === "" && first !== "");
          return activeChanged || currentChanged || firstChanged;
        },
        { timeout: 75000 },
        SEL_PAGINATOR_ACTIVE_PAGE,
        SEL_PAGINATOR_CURRENT,
        SEL_RESULT_CARDS,
        prevActive,
        prevCurrent,
        prevFirst
      );
    } catch {
      if (attempt === maxRetries) {
        console.warn(
          "goToNextResultsPage: paginator/content did not change after next — stale-node recovery exhausted."
        );
        throw new Error(
          "Pagination did not update after next (detached or AJAX stalled)"
        );
      }
      console.warn(
        `goToNextResultsPage: no UI change yet, retry ${attempt}/${maxRetries} (possible stale DOM)`
      );
      await delay(250 * attempt);
      const again = await getPaginatorSnapshot(page);
      if (again.nextDisabled) return false;
      continue;
    }

    await page
      .waitForSelector(SEL_RESULT_CARDS, { visible: true, timeout: 45000 })
      .catch(() => null);

    await delay(pageDelayMs);
    await delay(actionDelayMs);
    return true;
  }

  return false;
}

/**
 * Jump to the first results page (paginator "first" or page label "1").
 * Required after landing on the last page so forward-only resume logic can reach page 2+.
 */
export async function goToFirstResultsPage(page, options = {}) {
  const pageDelayMs = options.pageDelayMs ?? 200;
  const actionDelayMs = options.actionDelayMs ?? 100;
  const maxRetries = Math.min(
    5,
    Math.max(1, options.maxRetries ?? 3)
  );

  const snapBefore = await getPaginatorSnapshot(page);
  const rep0 = parseEntriesReport(snapBefore.currentReport);
  if (rep0 && rep0.from === 1) {
    console.log(
      `[paginator] goToFirstResultsPage: already first range (entries ${rep0.from}–${rep0.to})`
    );
    return { ok: true, via: "already_first", ms: 0 };
  }

  const prevActive = snapBefore.activePage;
  const prevCurrent = snapBefore.currentReport;
  const prevFirst = snapBefore.firstCardPeek;

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    const clicked = await page
      .evaluate((pagRoot) => {
        const root = document.querySelector(pagRoot);
        if (!root) return { ok: false, reason: "no paginator root" };
        const tryClick = (el) => {
          if (!el) return false;
          const dis =
            el.classList.contains("ui-state-disabled") ||
            el.getAttribute("aria-disabled") === "true" ||
            Boolean(el.closest?.(".ui-state-disabled"));
          if (dis) return false;
          try {
            el.scrollIntoView({ block: "center", inline: "nearest" });
          } catch (_) {}
          el.click();
          return true;
        };
        const firstBtn = root.querySelector(".ui-paginator-first");
        if (tryClick(firstBtn)) return { ok: true, via: "first_btn" };
        const pageLinks = [...root.querySelectorAll(".ui-paginator-page")].filter(
          (el) => el.offsetParent !== null
        );
        const one = pageLinks.find((el) => /^1$/.test(el.textContent.trim()));
        if (tryClick(one)) return { ok: true, via: "page_num_1" };
        return { ok: false, reason: "no_first_control" };
      }, SEL_PAGINATOR_BOTTOM)
      .catch((e) => ({ ok: false, reason: String(e?.message || e) }));

    if (!clicked?.ok) {
      if (attempt === maxRetries) {
        throw new Error(
          `goToFirstResultsPage failed after ${maxRetries} attempt(s): ${clicked?.reason ?? "?"}`
        );
      }
      await delay(200 + 80 * attempt);
      continue;
    }

    try {
      await page.waitForFunction(
        (
          activeSel,
          currentSel,
          cardSel,
          pActive,
          pCurrent,
          pFirst
        ) => {
          const a = document.querySelector(activeSel)?.textContent?.trim() ?? "";
          const c = document
            .querySelector(currentSel)
            ?.textContent?.replace(/\u00a0/g, " ")
            .trim() ?? "";
          const card = document.querySelector(cardSel);
          const first = card
            ? (card.innerText || "").replace(/\s+/g, " ").trim().slice(0, 160)
            : "";
          const activeChanged =
            a !== "" && pActive !== "" && a !== pActive;
          const currentChanged =
            c !== "" && pCurrent !== "" && c !== pCurrent;
          const firstChanged =
            (first !== "" && pFirst !== "" && first !== pFirst) ||
            (pFirst === "" && first !== "");
          return activeChanged || currentChanged || firstChanged;
        },
        { timeout: 75000 },
        SEL_PAGINATOR_ACTIVE_PAGE,
        SEL_PAGINATOR_CURRENT,
        SEL_RESULT_CARDS,
        prevActive,
        prevCurrent,
        prevFirst
      );
    } catch (e) {
      const again = await getPaginatorSnapshot(page);
      const rep = parseEntriesReport(again.currentReport);
      if (rep && rep.from === 1) {
        await delay(pageDelayMs);
        await delay(actionDelayMs);
        return { ok: true, via: `${clicked.via}_range_ok`, ms: 0 };
      }
      if (attempt === maxRetries) {
        throw new Error(
          `goToFirstResultsPage: paginator did not update (${e?.message || e})`
        );
      }
      await delay(250 * attempt);
      continue;
    }

    await page
      .waitForSelector(SEL_RESULT_CARDS, { visible: true, timeout: 45000 })
      .catch(() => null);
    await delay(pageDelayMs);
    await delay(actionDelayMs);
    return { ok: true, via: clicked.via, ms: 0 };
  }

  throw new Error("goToFirstResultsPage: exhausted retries");
}

/**
 * Reliable navigation for mismatch-audit / sparse recovery: never assumes we start from page 1.
 * Resets to page 1 first, then skips forward — safe when the session ended on the last results page.
 */
export async function navigateAuditToResultsPage(
  page,
  targetPage1Based,
  options = {}
) {
  const prefix = options.logPrefix ?? "[audit-nav]";
  const pageDelayMs = options.pageDelayMs ?? 200;
  const actionDelayMs = options.actionDelayMs ?? 100;

  let preApprox = null;
  try {
    preApprox = await inferCurrentResultPage1Based(page);
  } catch (_) {}
  const preSnap = await getPaginatorSnapshot(page);
  console.log(
    `${prefix} before: approxPage=${preApprox ?? "?"} active="${preSnap.activePage}" report="${(preSnap.currentReport || "").slice(0, 72)}…" → target=${targetPage1Based}`
  );

  if (targetPage1Based <= 1) {
    console.log(`${prefix} strategy: goToFirstResultsPage only`);
    const r = await goToFirstResultsPage(page, options);
    let postApprox = null;
    try {
      postApprox = await inferCurrentResultPage1Based(page);
    } catch (_) {}
    console.log(
      `${prefix} reached: approxPage=${postApprox ?? "?"} via=${r.via ?? "?"}`
    );
    return { strategy: "first_only", ...r };
  }

  console.log(
    `${prefix} strategy: goToFirstResultsPage + skipForward ${targetPage1Based - 1} step(s)`
  );
  await goToFirstResultsPage(page, options);
  const skip = await skipForwardToResultsStartingPage(
    page,
    targetPage1Based,
    options
  );
  let postApprox = null;
  try {
    postApprox = await inferCurrentResultPage1Based(page);
  } catch (_) {}
  console.log(
    `${prefix} reached: approxPage=${postApprox ?? "?"} steps=${skip.steps}`
  );
  return { strategy: "first_then_forward", ...skip };
}

/**
 * After search, results start on page 1. To resume at `resumeStartPage1Based`, advance (resumeStartPage - 1) times.
 * Uses shorter delays than normal scraping; every click uses fresh DOM (goToNextResultsPage).
 */
export async function skipForwardToResultsStartingPage(
  page,
  resumeStartPage1Based,
  options = {}
) {
  if (resumeStartPage1Based <= 1) {
    return { steps: 0, ms: 0 };
  }
  const clicksNeeded = resumeStartPage1Based - 1;
  const t0 = Date.now();
  const resumePageDelayMs =
    options.resumePageDelayMs ?? options.pageDelayMs ?? 55;
  const resumeActionDelayMs =
    options.resumeActionDelayMs ?? options.actionDelayMs ?? 35;
  const maxRetries = options.maxRetries ?? 3;

  console.log(
    `Resume: skipping forward ${clicksNeeded} paginator step(s) to reach start page ${resumeStartPage1Based} (compact delays).`
  );

  for (let s = 0; s < clicksNeeded; s += 1) {
    const ok = await goToNextResultsPage(page, {
      pageDelayMs: resumePageDelayMs,
      actionDelayMs: resumeActionDelayMs,
      maxRetries,
    });
    if (!ok) {
      throw new Error(
        `Resume skip failed at step ${s + 1}/${clicksNeeded} (cannot reach page ${resumeStartPage1Based})`
      );
    }
    const done = s + 1;
    if (done === 1 || done % 40 === 0 || done === clicksNeeded) {
      let approx = null;
      try {
        approx = await inferCurrentResultPage1Based(page);
      } catch (_) {
        /* ignore */
      }
      const sec = ((Date.now() - t0) / 1000).toFixed(1);
      console.log(
        `  resume progress: ${done}/${clicksNeeded} (~page ${approx ?? "?"}) — ${sec}s elapsed`
      );
    }
  }

  return { steps: clicksNeeded, ms: Date.now() - t0 };
}

/**
 * Collect lawyer cards from the current results view (PrimeFaces / JSF).
 */
export async function extractCardsFromPage(page, selectedBar) {
  return await page.evaluate((bar, cardSel) => {
    const norm = (s) => (s ?? "").replace(/\s+/g, " ").trim();
    function parseResultCard(el, b) {
      const rawText = norm(el.innerText);
      if (!rawText) return null;
      const link =
        el.querySelector(".resultCardDetailLink") || el.querySelector("a[href]");
      const infoLinkText = link ? norm(link.textContent) : "";
      const lines = (el.innerText || "")
        .split(/\r?\n/)
        .map((l) => norm(l))
        .filter((l) => l && l !== "Info");
      let postalCode = "";
      let city = "";
      let street = "";
      let fullName = "";
      let profession = "";
      let companyOrOffice = "";
      const plzIdx = lines.findIndex((l) => /^\d{5}\b/.test(l));
      if (plzIdx >= 0) {
        const m = lines[plzIdx].match(/^(\d{5})\s+(.+)$/);
        if (m) {
          postalCode = m[1];
          city = m[2].trim();
        }
        fullName = lines[0] || "";
        if (plzIdx >= 2) {
          profession = lines[1] || "";
          companyOrOffice =
            plzIdx > 3 ? lines.slice(2, plzIdx - 1).join(" ") : "";
          street = lines[plzIdx - 1] || "";
        }
      } else {
        fullName = lines[0] || "";
        profession = lines[1] || "";
        companyOrOffice = lines[2] || "";
        street = lines[3] || "";
      }
      return {
        fullName: norm(fullName),
        profession: norm(profession),
        companyOrOffice: norm(companyOrOffice),
        street: norm(street),
        postalCode: norm(postalCode),
        city: norm(city),
        rawText,
        infoLinkText,
        selectedBar: b,
      };
    }
    function visibleCard(el) {
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
    const roots = [...document.querySelectorAll(cardSel)].filter(visibleCard);
    const cards = [];
    for (const el of roots) {
      const row = parseResultCard(el, bar);
      if (row) cards.push(row);
    }
    return cards;
  }, selectedBar, SEL_RESULT_CARDS);
}

/**
 * Summary fields for one visible `.resultCard` by index (re-query each time; no stale handles).
 */
export async function extractSummaryFromResultCardByIndex(
  page,
  index,
  selectedBar
) {
  return await page.evaluate(
    (idx, bar, cardSel) => {
      const norm = (s) => (s ?? "").replace(/\s+/g, " ").trim();
      function parseResultCard(el, b) {
        const rawText = norm(el.innerText);
        if (!rawText) return null;
        const link =
          el.querySelector(".resultCardDetailLink") ||
          el.querySelector("a[href]");
        const infoLinkText = link ? norm(link.textContent) : "";
        const lines = (el.innerText || "")
          .split(/\r?\n/)
          .map((l) => norm(l))
          .filter((l) => l && l !== "Info");
        let postalCode = "";
        let city = "";
        let street = "";
        let fullName = "";
        let profession = "";
        let companyOrOffice = "";
        const plzIdx = lines.findIndex((l) => /^\d{5}\b/.test(l));
        if (plzIdx >= 0) {
          const m = lines[plzIdx].match(/^(\d{5})\s+(.+)$/);
          if (m) {
            postalCode = m[1];
            city = m[2].trim();
          }
          fullName = lines[0] || "";
          if (plzIdx >= 2) {
            profession = lines[1] || "";
            companyOrOffice =
              plzIdx > 3 ? lines.slice(2, plzIdx - 1).join(" ") : "";
            street = lines[plzIdx - 1] || "";
          }
        } else {
          fullName = lines[0] || "";
          profession = lines[1] || "";
          companyOrOffice = lines[2] || "";
          street = lines[3] || "";
        }
        return {
          fullName: norm(fullName),
          profession: norm(profession),
          companyOrOffice: norm(companyOrOffice),
          street: norm(street),
          postalCode: norm(postalCode),
          city: norm(city),
          rawText,
          infoLinkText,
          selectedBar: b,
        };
      }
      function visibleCard(el) {
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
      const roots = [...document.querySelectorAll(cardSel)].filter(visibleCard);
      const el = roots[idx];
      if (!el) return null;
      return parseResultCard(el, bar);
    },
    index,
    selectedBar,
    SEL_RESULT_CARDS
  );
}

/** Count visible `.resultCard` on the current list page. */
export async function getVisibleCardsCount(page) {
  return await page.evaluate((cardSel) => {
    function visibleCard(el) {
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
    return [...document.querySelectorAll(cardSel)].filter(visibleCard).length;
  }, SEL_RESULT_CARDS);
}

/**
 * Re-query visible cards, pick index, find `.resultCardDetailLink` inside that card only; Puppeteer click + evaluate fallback.
 */
export async function openCardDetailByIndex(page, index) {
  const handle = await page.evaluateHandle(
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
      return card.querySelector(linkSel);
    },
    index,
    SEL_RESULT_CARDS,
    SEL_RESULT_CARD_INFO_LINK
  );
  const linkEl = handle.asElement();
  if (!linkEl) {
    await handle.dispose();
    throw new Error(
      `openCardDetailByIndex: no card or .resultCardDetailLink at index ${index}`
    );
  }
  await linkEl.evaluate((el) =>
    el.scrollIntoView({ block: "center", inline: "nearest" })
  );
  try {
    await linkEl.click({ delay: 50 });
  } catch {
    await linkEl.evaluate((el) => el.click());
  }
  await linkEl.dispose();
  await delay(100);
}

const DETAIL_OPEN_TIMEOUT_MS = 45000;

/**
 * After Info click: wait for a visible `.ui-dialog` / `.ui-dialog-content` with enough text (>30).
 * Pick logic runs entirely in the page (visible box + title/text heuristics); not only `#resultDetailForm`.
 */
export async function waitForVisibleDetailDialog(page, { actionDelayMs, pageDelayMs } = {}) {
  await page.waitForFunction(
    () => {
      function boxVisible(el) {
        if (!el) return false;
        const st = window.getComputedStyle(el);
        if (st.display === "none" || st.visibility === "hidden") return false;
        if (parseFloat(st.opacity || "1") < 0.05) return false;
        const r = el.getBoundingClientRect();
        if (r.width < 20 || r.height < 20) return false;
        return true;
      }
      const dlgs = [...document.querySelectorAll(".ui-dialog")].filter(boxVisible);
      if (!dlgs.length) return false;
      const scored = dlgs.map((dlg) => {
        const titleEl = dlg.querySelector(".ui-dialog-title");
        const title = (titleEl && titleEl.innerText) || "";
        const content = dlg.querySelector(".ui-dialog-content") || dlg;
        const t = (content.innerText || dlg.innerText || "").replace(/\s+/g, " ").trim();
        let score = 0;
        if (/details|detail|angaben|information/i.test(title + " " + t.slice(0, 200)))
          score += 120;
        if (dlg.querySelector("#resultDetailForm")) score += 40;
        if (/telefon|e-?mail|straße|adresse|kammer|zulassung|anwalt|name|amt/i.test(t))
          score += 30;
        score += Math.min(t.length, 8000) / 50;
        return { dlg, content, score };
      });
      scored.sort((a, b) => b.score - a.score);
      const pick = scored[0];
      if (!pick) return false;
      const full = (pick.content.innerText || "").replace(/\s+/g, " ").trim();
      return full.length > 30;
    },
    { timeout: DETAIL_OPEN_TIMEOUT_MS }
  );

  if (pageDelayMs != null) await delay(pageDelayMs);
  if (actionDelayMs != null) await delay(actionDelayMs);
}

async function screenshotDetailOpenFailure(page, pageNumber, cardIndex) {
  const pg = pageNumber != null ? String(pageNumber) : "x";
  const fname = `debug-detail-fail-page-${pg}-card-${cardIndex}.png`;
  const filePath = path.join(process.cwd(), fname);
  try {
    await page.screenshot({ path: filePath, fullPage: false });
    console.warn(`  Screenshot: ${filePath}`);
  } catch (_) {}
}

/**
 * Extract label/value pairs from the visible detail modal (default Data tab content when present).
 */
export async function extractVisibleDetailDialog(page, debug) {
  return await page.evaluate((debugFlag) => {
    const norm = (s) => (s ?? "").replace(/\s+/g, " ").trim();
    function collectInto(root, detailData) {
      if (!root) return;
      root.querySelectorAll("tr").forEach((tr) => {
        const cells = tr.querySelectorAll("th, td");
        if (cells.length >= 2) {
          const k = norm(cells[0].innerText).replace(/:\s*$/u, "");
          const v = norm(cells[1].innerText);
          if (k && v) detailData[k] = v;
        }
      });
      root.querySelectorAll("dl").forEach((dl) => {
        dl.querySelectorAll("dt").forEach((dt) => {
          const dd = dt.nextElementSibling;
          if (dd && dd.tagName === "DD") {
            const k = norm(dt.innerText).replace(/:\s*$/u, "");
            const v = norm(dd.innerText);
            if (k && v) detailData[k] = v;
          }
        });
      });
      root.querySelectorAll("label").forEach((lab) => {
        const id = lab.getAttribute("for");
        if (id) {
          const inp = document.getElementById(id);
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
    }

    function boxVisible(el) {
      if (!el) return false;
      const st = window.getComputedStyle(el);
      if (st.display === "none" || st.visibility === "hidden") return false;
      if (parseFloat(st.opacity || "1") < 0.05) return false;
      const r = el.getBoundingClientRect();
      return r.width >= 20 && r.height >= 20;
    }
    const dlgs = [...document.querySelectorAll(".ui-dialog")].filter(boxVisible);
    if (!dlgs.length) return { detailData: {}, detailRawText: "" };

    const scored = dlgs.map((dlg) => {
      const titleEl = dlg.querySelector(".ui-dialog-title");
      const title = (titleEl && titleEl.innerText) || "";
      const content = dlg.querySelector(".ui-dialog-content") || dlg;
      const t = (content.innerText || dlg.innerText || "").replace(/\s+/g, " ").trim();
      let score = 0;
      if (/details|detail|angaben|information/i.test(title + " " + t.slice(0, 200)))
        score += 120;
      if (dlg.querySelector("#resultDetailForm")) score += 40;
      if (/telefon|e-?mail|straße|adresse|kammer|zulassung|anwalt|name/i.test(t))
        score += 30;
      score += Math.min(t.length, 8000) / 50;
      return { dlg, score, content };
    });
    scored.sort((a, b) => b.score - a.score);
    const best = scored[0];
    if (!best) return { detailData: {}, detailRawText: "" };

    const dlg = best.dlg;
    const detailData = {};
    const dataTabPanel =
      dlg.querySelector(
        ".ui-tabview-panels .ui-tabview-panel.ui-state-active"
      ) ||
      dlg.querySelector(".ui-tabs-panels .ui-tabs-panel.ui-state-active") ||
      dlg.querySelector(".ui-tabs-panels .ui-tabs-panel") ||
      dlg.querySelector(".ui-tabview-panels .ui-tabview-panel") ||
      dlg.querySelector("#resultDetailForm") ||
      dlg.querySelector('[id*="resultDetailForm"]');
    const extractRoot =
      dataTabPanel ||
      dlg.querySelector(".ui-dialog-content") ||
      dlg;

    collectInto(extractRoot, detailData);
    if (Object.keys(detailData).length < 2 && dataTabPanel && dataTabPanel !== extractRoot) {
      collectInto(dlg.querySelector(".ui-dialog-content") || dlg, detailData);
    }

    const rawInner = norm((extractRoot && extractRoot.innerText) || dlg.innerText);
    const detailRawText = debugFlag && rawInner.length > 0 ? rawInner : "";
    return { detailData, detailRawText };
  }, Boolean(debug));
}

/** Close the visible detail dialog via `.ui-dialog-titlebar-close`, then PF fallback. */
export async function closeVisibleDetailDialog(page) {
  return await page.evaluate(() => {
    function boxVisible(el) {
      if (!el) return false;
      const st = window.getComputedStyle(el);
      if (st.display === "none" || st.visibility === "hidden") return false;
      const r = el.getBoundingClientRect();
      return r.width > 8 && r.height > 8;
    }
    const dlgs = [...document.querySelectorAll(".ui-dialog")].filter(boxVisible);
    for (let i = 0; i < dlgs.length; i += 1) {
      const dlg = dlgs[i];
      const t = (dlg.innerText || "").trim();
      if (t.length < 15) continue;
      const btn = dlg.querySelector(".ui-dialog-titlebar-close");
      if (btn) {
        btn.click();
        return "titlebar_close";
      }
    }
    for (let i = 0; i < dlgs.length; i += 1) {
      const btn = dlgs[i].querySelector(
        ".ui-dialog-titlebar-close, .ui-dialog-titlebar-icon"
      );
      if (btn) {
        btn.click();
        return "titlebar_close";
      }
    }
    if (typeof PF !== "undefined" && PF("dlgResultDetail")) {
      try {
        PF("dlgResultDetail").hide();
        return "pf_hide";
      } catch (_) {}
    }
    return "none";
  });
}

/** Wait until the detail modal is no longer visibly open. */
export async function waitForVisibleDetailDialogHidden(page, { actionDelayMs }) {
  await page
    .waitForFunction(
      () => {
        function boxVisible(el) {
          if (!el) return false;
          const st = window.getComputedStyle(el);
          if (st.display === "none" || st.visibility === "hidden") return false;
          const r = el.getBoundingClientRect();
          return r.width > 10 && r.height > 10;
        }
        const dlgs = [...document.querySelectorAll(".ui-dialog")].filter(boxVisible);
        for (let i = 0; i < dlgs.length; i += 1) {
          const dlg = dlgs[i];
          const t = (dlg.innerText || "").replace(/\s+/g, " ").trim();
          if (t.length > 30 && /details|telefon|e-?mail|straße|anwalt|name|angaben/i.test(t)) {
            return false;
          }
        }
        return true;
      },
      { timeout: 45000 }
    )
    .catch(() => {});
  if (actionDelayMs != null) await delay(actionDelayMs);
}

/**
 * For each visible `.resultCard`, open Info dialog, merge summary + detail, close dialog.
 */
export async function scrapeResultPageCardsWithDetails(
  page,
  selectedBar,
  { actionDelayMs, pageDelayMs, debug, pageNumber }
) {
  const count = await getVisibleCardsCount(page);

  const records = [];
  for (let i = 0; i < count; i += 1) {
    let summary = null;
    try {
      summary = await extractSummaryFromResultCardByIndex(page, i, selectedBar);
    } catch (e) {
      console.warn(`  Card ${i} summary failed:`, e?.message || e);
      continue;
    }
    if (!summary) {
      console.warn(`  Card ${i}: no summary — skipped`);
      continue;
    }

    try {
      console.log(`  Opening detail for card ${i}`);
      await openCardDetailByIndex(page, i);
      await waitForVisibleDetailDialog(page, { actionDelayMs, pageDelayMs });
      console.log("  Detail dialog visible");

      const { detailData, detailRawText } = await extractVisibleDetailDialog(
        page,
        debug
      );
      const keyCount = detailData ? Object.keys(detailData).length : 0;
      console.log(`  Extracted detail keys: ${keyCount}`);

      await closeVisibleDetailDialog(page);
      await waitForVisibleDetailDialogHidden(page, { actionDelayMs });
      console.log("  Dialog closed");

      const merged = {
        ...summary,
        detailData: detailData && Object.keys(detailData).length ? detailData : {},
      };
      if (debug && detailRawText) merged.detailRawText = detailRawText;
      records.push(merged);
    } catch (err) {
      const msg = err?.message || err;
      console.warn(`  Detail failed: ${msg}`);
      await screenshotDetailOpenFailure(page, pageNumber, i);
      try {
        await closeVisibleDetailDialog(page);
      } catch (_) {}
      try {
        await waitForVisibleDetailDialogHidden(page, { actionDelayMs });
      } catch (_) {}
      records.push({
        ...summary,
        detailData: {},
        detailError: String(msg),
      });
    }
  }

  return records;
}
