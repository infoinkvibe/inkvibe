# Production certification smoke (tiny live matrix)

This recipe is a small, repeatable live certification run for cross-family production confidence without broad rollout risk.

## Target matrix

Use exactly one template from each bucket:

- `poster_basic`
- `hoodie_gildan` **or** `tshirt_gildan`
- `tote_basic`
- `mug_new` **or** `accent_mug_basic` (if enabled in your template config)

## Why this matrix

It provides a low-volume cross-family check for:

1. family routing behavior
2. placement behavior
3. metadata provenance
4. collection/family scope
5. publish + verify success

## Recommended command sequence

> Replace paths/artwork count to match your environment.

### 1) Optional read-only storefront QA baseline

```bash
python printify_shopify_sync_pipeline.py \
  --storefront-qa \
  --template-key poster_basic \
  --template-key hoodie_gildan \
  --template-key tote_basic \
  --template-key mug_new \
  --max-artworks 4 \
  --export-storefront-qa-report reports/certification/storefront_qa.csv \
  --export-storefront-qa-json reports/certification/storefront_qa.json \
  --export-certification-summary-json reports/certification/storefront_qa_summary.json
```

### 2) Live smoke create/update + publish + verify

```bash
python printify_shopify_sync_pipeline.py \
  --template-key poster_basic \
  --template-key hoodie_gildan \
  --template-key tote_basic \
  --template-key mug_new \
  --max-artworks 4 \
  --publish \
  --verify-publish \
  --sync-collections \
  --verify-collections \
  --enforce-family-collection-membership \
  --export-run-report reports/certification/run_report.csv \
  --export-failure-report reports/certification/failure_report.csv \
  --export-certification-summary-json reports/certification/certification_summary.json
```

### 3) If publish queue was deferred/rate-limited, resume and refresh summary

```bash
python printify_shopify_sync_pipeline.py \
  --resume-publish-only \
  --publish-batch-size 5 \
  --pause-between-publish-batches-seconds 3 \
  --export-run-report reports/certification/resume_run_report.csv \
  --export-certification-summary-json reports/certification/resume_certification_summary.json
```

## Evidence to audit

Primary artifacts:

- `reports/certification/run_report.csv`
- `reports/certification/storefront_qa.csv` (optional but recommended)
- `reports/certification/failure_report.csv` (should be empty or narrowly explain failures)
- `reports/certification/certification_summary.json`

The certification summary JSON aggregates these proof points:

- **family_routing**
  - `rows_with_template_family`
  - `rows_with_routed_asset_family`
  - `mismatch_rows`
- **placement_behavior**
  - `rows_with_source_size`
  - `rows_with_required_placement_size`
  - `rows_with_placement_scale_used`
- **metadata_provenance**
  - `run_rows_with_metadata_resolution_source`
  - `run_rows_with_final_title_source`
  - `qa_rows_with_metadata_resolution_source`
  - `qa_copy_provenance_counts`
- **collection_family_scope**
  - `rows_with_family_collection_handle`
  - `rows_with_collection_membership_verified`
  - `rows_with_collection_sync_attempted`
- **publish_verify**
  - `rows_with_publish_attempted`
  - `rows_with_publish_verified`
  - `publish_outcome_counts`
  - `publish_queue_status_counts`

## Pass/attention guide

- Pass signal: all expected matrix templates appear in `observed_template_keys` and `missing_template_keys` is empty.
- Attention signal: any `mismatch_rows` in family routing, or zero `rows_with_publish_verified` in live run.
- Attention signal: non-empty failure report or publish queue statuses stuck in `pending_*`.
