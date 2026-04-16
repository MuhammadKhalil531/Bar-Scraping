# Bravsearch scraper (Puppeteer)

Node.js tool that drives the public **Bravsearch** form in a real Chromium window (**normal Puppeteer only** — no stealth plugins, no custom anti-bot bypass). It picks the first non-empty **Bar** option, runs **Start search!**, walks every results page via the PrimeFaces **paginator**, deduplicates rows, and writes **JSON** and **CSV** under `extracted-data/`.

## Prerequisites

- **Node.js** 18+
- Network access to `https://bravsearch.bea-brak.de/bravsearch/`
- On some Linux distributions (e.g. Ubuntu with restricted user namespaces), Chromium may require the sandbox flags already included in `src/scraper.js` (`--no-sandbox` / `--disable-setuid-sandbox` on Linux only).

## Install

```bash
npm install
```

## Run

```bash
npm run dev
```

This executes `node src/index.js`, which runs `runScrape()` in `src/scraper.js`.

## Configuration

Edit the exported constants at the top of `src/scraper.js`:

| Constant | Default | Purpose |
|----------|---------|---------|
| `HEADLESS` | `false` | Set `true` for headless runs later |
| `ACTION_DELAY_MS` | `1200` | Pause between form actions / after AJAX |
| `PAGE_DELAY_MS` | `1500` | Pause after load / pagination |
| `DEBUG` | `true` | On **fatal** errors, leave the browser open; otherwise it is closed |

## Output

- **Directory:** `extracted-data/` (created automatically if missing).
- **Names:** Derived from the **visible text** of the selected Bar option, passed through `sanitizeFilename()` (e.g. `Bamberg` → `extracted-data/Bamberg.json` and `extracted-data/Bamberg.csv`).
- **On error:** If any rows were collected, a partial export is written as `extracted-data/<Bar>_partial.json` and `.csv`.

## Behaviour notes

- Waits include `networkidle2` on first navigation, explicit waits for the results list / paginator, and configurable delays to accommodate **JSF / PrimeFaces** partial page updates.
- Pagination follows the **Next** control until it is disabled (with a UI-reported page count as a hint and a safeguard cap).
- Records are **deduplicated** using `fullName|street|postalCode|city|selectedBar`.

## Project layout

- `src/index.js` — entry point
- `src/scraper.js` — browser launch, navigation, pagination loop, logging, persistence
- `src/utils.js` — delays, filename sanitization, DOM extraction, CSV/JSON helpers
