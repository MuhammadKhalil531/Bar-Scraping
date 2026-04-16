import { runScrape } from "./scraper.js";

runScrape().catch((err) => {
  console.error(err);
  process.exit(1);
});
