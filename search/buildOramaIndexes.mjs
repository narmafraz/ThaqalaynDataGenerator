/**
 * Build Orama search indexes from pre-generated search document JSON files.
 *
 * Reads the document files produced by app/search_index.py and creates
 * persisted Orama indexes that the Angular app can load directly.
 *
 * Output:
 *   ThaqalaynData/index/search/titles-orama.json   (~small, loaded immediately)
 *   ThaqalaynData/index/search/quran-orama.json     (lazy-loaded on demand)
 *   ThaqalaynData/index/search/al-kafi-orama.json   (lazy-loaded on demand)
 *
 * Usage:
 *   node search/buildOramaIndexes.mjs
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { create, insertMultiple, save, load, search } from "@orama/orama";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = join(__dirname, "..", "..", "ThaqalaynData");
const SEARCH_DIR = join(DATA_DIR, "index", "search");

/**
 * Load a JSON document file.
 */
function loadDocs(filename) {
  const filepath = join(SEARCH_DIR, filename);
  if (!existsSync(filepath)) {
    console.error(`Document file not found: ${filepath}`);
    console.error("Run the Python generator first: python -m app.search_index");
    process.exit(1);
  }
  return JSON.parse(readFileSync(filepath, "utf-8"));
}

/**
 * Schema for the titles index.
 * Uses compact keys: p=path, pt=part_type, en=title_en, ar=title_ar, arn=title_ar_normalized
 */
const TITLES_SCHEMA = {
  p: "string",
  pt: "string",
  en: "string",
  ar: "string",
  arn: "string",
};

/**
 * Build and save the titles index.
 */
function buildTitlesIndex() {
  console.log("Building titles index...");
  const docs = loadDocs("titles.json");

  const db = create({ schema: TITLES_SCHEMA });
  insertMultiple(db, docs);

  const saved = save(db);
  const json = JSON.stringify(saved);
  const outPath = join(SEARCH_DIR, "titles-orama.json");
  writeFileSync(outPath, json, "utf-8");

  // Verify
  const db2 = create({ schema: TITLES_SCHEMA });
  load(db2, saved);
  const testResult = search(db2, { term: "Opening", properties: ["en"] });
  console.log(
    `  titles-orama.json: ${(json.length / 1024).toFixed(1)} KB, ` +
      `${docs.length} docs, verification: ${testResult.count} hits for "Opening"`
  );

  return { filename: "titles-orama.json", size: json.length, docs: docs.length };
}

/**
 * Schema for per-book full-text indexes.
 * Uses compact keys: p=path, t=chapter_title_en, ar=text_ar_normalized, en=text_en, i=local_index
 */
const BOOK_SCHEMA = {
  p: "string",
  t: "string",
  ar: "string",
  en: "string",
  i: "number",
};

/**
 * Build and save a per-book full-text index.
 */
function buildBookIndex(bookSlug) {
  console.log(`Building ${bookSlug} index...`);
  const docs = loadDocs(`${bookSlug}-docs.json`);

  const db = create({ schema: BOOK_SCHEMA });

  // Insert in batches for memory efficiency
  const BATCH_SIZE = 5000;
  for (let i = 0; i < docs.length; i += BATCH_SIZE) {
    const batch = docs.slice(i, i + BATCH_SIZE);
    insertMultiple(db, batch);
    if (docs.length > BATCH_SIZE) {
      console.log(
        `  Inserted ${Math.min(i + BATCH_SIZE, docs.length)}/${docs.length} docs`
      );
    }
  }

  const saved = save(db);
  const json = JSON.stringify(saved);
  const outPath = join(SEARCH_DIR, `${bookSlug}-orama.json`);
  writeFileSync(outPath, json, "utf-8");

  // Verify with a test search
  const db2 = create({ schema: BOOK_SCHEMA });
  load(db2, saved);

  const testTerm = bookSlug === "quran" ? "merciful" : "intellect";
  const testResult = search(db2, { term: testTerm, properties: ["en"], limit: 5 });
  console.log(
    `  ${bookSlug}-orama.json: ${(json.length / 1024 / 1024).toFixed(1)} MB, ` +
      `${docs.length} docs, verification: ${testResult.count} hits for "${testTerm}"`
  );

  return { filename: `${bookSlug}-orama.json`, size: json.length, docs: docs.length };
}

/**
 * Main build pipeline.
 */
function main() {
  console.log(`Search index directory: ${SEARCH_DIR}\n`);

  if (!existsSync(SEARCH_DIR)) {
    mkdirSync(SEARCH_DIR, { recursive: true });
  }

  const results = [];
  results.push(buildTitlesIndex());
  results.push(buildBookIndex("quran"));
  results.push(buildBookIndex("al-kafi"));

  console.log("\nOrama index build complete:");
  for (const r of results) {
    const sizeStr =
      r.size > 1024 * 1024
        ? `${(r.size / 1024 / 1024).toFixed(1)} MB`
        : `${(r.size / 1024).toFixed(1)} KB`;
    console.log(`  ${r.filename}: ${sizeStr}, ${r.docs} documents`);
  }
}

main();
