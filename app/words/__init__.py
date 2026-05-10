"""ThaqalaynWords project — per-word Arabic dictionary generator.

This module is the generator-side implementation of the per-word pages
documented in `Thaqalayn/docs/WORDS_PROJECT_PLAN.md`. It produces lean
JSON files served at `ThaqalaynWords/surfaces/{surface}.json` and
`ThaqalaynWords/lemmas/{lemma}.json`, plus index files for browse/search.

Sub-modules:
- `normalize` — NFC + Arabic-letter normalization. The single canonical
  slug-derivation function used by both generator and UI. A 1000-form
  fixture locks parity with the TypeScript twin in
  `Thaqalayn/src/app/services/word-normalize.ts`.
- `morphology` — CAMeL Tools wrapper (analyzer + generator).
- `corpus_extract` — Walks v4 chunks, NFC-normalizes, produces the
  corpus surface-form set with counts and occurrence paths.
- `build_pages` — Page builders for surface and lemma JSONs.
- `build_indexes` — Browse-list builders.
- `validate` — Schema validation gates.
"""
