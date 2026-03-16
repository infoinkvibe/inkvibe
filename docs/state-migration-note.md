# State migration note

The pipeline now normalizes legacy state rows at read time using `state_store.migrate_legacy_row`.

- Existing `state.json` files continue to load.
- Resume behavior is preserved:
  - `dry-run-only` rows are resumable.
  - `real-completed` rows are skipped on `--resume`.
- Publish verification fields (`publish_attempted`, `publish_verified`, verification metadata) continue to persist in row `result.printify`.
