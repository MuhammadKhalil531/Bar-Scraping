#!/usr/bin/env node
/**
 * Merge per-Bar CSV files into one complete.csv in dropdown order.
 * Reads extracted-data/meta/bar-scrape-order.json (or legacy extracted-data/bar-scrape-order.json).
 *
 *   npm run merge-csv
 *   node scripts/merge-csvs.js
 *
 * Output: extracted-data/complete.csv
 */
import { readFile, writeFile, access } from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import { sanitizeFilename } from "../src/utils.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXTRACTED = path.join(ROOT, "extracted-data");
const ORDER_FILE_META = path.join(EXTRACTED, "meta", "bar-scrape-order.json");
const ORDER_FILE_LEGACY = path.join(EXTRACTED, "bar-scrape-order.json");
const OUT_FILE = path.join(EXTRACTED, "complete.csv");

async function fileExists(p) {
  try {
    await access(p);
    return true;
  } catch {
    return false;
  }
}

async function main() {
  let orderFileUsed = ORDER_FILE_META;
  let order;
  try {
    let raw;
    try {
      raw = await readFile(ORDER_FILE_META, "utf8");
    } catch {
      orderFileUsed = ORDER_FILE_LEGACY;
      raw = await readFile(ORDER_FILE_LEGACY, "utf8");
    }
    const parsed = JSON.parse(raw);
    order = parsed.dropdownOrder;
  } catch (e) {
    console.error(
      `merge-csvs: missing or invalid bar-scrape-order.json (tried ${ORDER_FILE_META} and ${ORDER_FILE_LEGACY}). Run a full scrape first.`
    );
    console.error(e?.message || e);
    process.exit(1);
  }
  console.log(
    `merge-csvs: combining per-Bar CSVs → complete.csv (order from ${orderFileUsed})`
  );
  if (!Array.isArray(order) || !order.length) {
    console.error("merge-csvs: bar-scrape-order.json has no dropdownOrder array.");
    process.exit(1);
  }

  const chunks = [];
  let headerLine = null;

  for (const bar of order) {
    const name = sanitizeFilename(bar);
    const csvPath = path.join(EXTRACTED, `${name}.csv`);
    if (!(await fileExists(csvPath))) {
      console.warn(`merge-csvs: skip missing file for "${bar}" → ${name}.csv`);
      continue;
    }
    let text = await readFile(csvPath, "utf8");
    if (text.charCodeAt(0) === 0xfeff) {
      text = text.slice(1);
    }
    const lines = text
      .replace(/\r\n/g, "\n")
      .replace(/\r/g, "\n")
      .split("\n")
      .filter((line) => line.length > 0);

    if (!lines.length) {
      console.warn(`merge-csvs: empty CSV for "${bar}"`);
      continue;
    }

    if (headerLine === null) {
      headerLine = lines[0];
      for (const line of lines) {
        chunks.push(line);
      }
    } else {
      for (let i = 0; i < lines.length; i += 1) {
        if (i === 0 && lines[i] === headerLine) continue;
        if (i === 0 && lines[i] !== headerLine) {
          console.warn(
            `merge-csvs: unexpected header in ${name}.csv — appending all lines including first (check column alignment).`
          );
        }
        chunks.push(lines[i]);
      }
    }
  }

  if (headerLine === null) {
    console.error("merge-csvs: no CSV data found.");
    process.exit(1);
  }

  const body = `${chunks.join("\n")}\n`;
  await writeFile(OUT_FILE, body, "utf8");
  const dataRows = chunks.length - 1;
  console.log(
    `merge-csvs: wrote ${OUT_FILE} (header + ${dataRows} data row(s)).`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
