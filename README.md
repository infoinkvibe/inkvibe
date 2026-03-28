# InkVibeAuto

InkVibeAuto is a Python automation pipeline for print-on-demand stores that converts local artwork into fully configured products for **Printify** and optionally **Shopify**.

## Architecture snapshot

- **Pipeline entrypoint:** `printify_shopify_sync_pipeline.py`
- **Source processing:** reads images from `IMAGE_DIR`, validates placement size constraints, and exports print-ready PNGs.
- **Printify flow:** catalog discovery (shops/blueprints/providers/variants), uploads, product creation, publish.
- **Shopify flow (isolated section):** `productSet` GraphQL sync path that can be disabled simply by omitting `SHOPIFY_ADMIN_TOKEN`.
- **State/idempotency:** `state.json` caches processed artworks and upload IDs.

## Known risks / manual review points

- Provider-specific `print_areas` transforms can differ by blueprint/provider; generated placement transforms should be validated against manually created products per template.
- Printify variant prices must be sent as integer minor units (for example `2499` for $24.99).
- Large assets can use Cloudflare R2 URL upload flow to avoid direct base64 upload limits.
- Variant/color/size filtering depends on provider catalog option naming consistency.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Optional tests:

```bash
pip install pytest
pytest
```

## Environment

Copy `.env.example` to `.env` and set values.

```bash
cp .env.example .env
```

Required:
- `PRINTIFY_API_TOKEN`

Common optional:
- `PRINTIFY_SHOP_ID`
- `SHOPIFY_ADMIN_TOKEN`
- `IMAGE_DIR`, `EXPORT_DIR`, `STATE_PATH`, `TEMPLATES_CONFIG`
- `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET`, `R2_PUBLIC_BASE_URL` (required only for `--upload-strategy r2_url`, and for `auto` when assets exceed 5MB)

## CLI usage

```bash
python printify_shopify_sync_pipeline.py --help
```

Common examples:

```bash
# Dry run with audit
python printify_shopify_sync_pipeline.py --dry-run

# Force reprocessing and debug logs
python printify_shopify_sync_pipeline.py --force --log-level DEBUG

# Custom paths and process first 5 artworks
python printify_shopify_sync_pipeline.py \
  --templates ./product_templates.json \
  --image-dir ./images \
  --export-dir ./exports \
  --state-path ./state.json \
  --max-artworks 5

# List template keys
python printify_shopify_sync_pipeline.py --templates ./product_templates.json --list-templates

# Process one artwork with selected templates
python printify_shopify_sync_pipeline.py --max-artworks 1 --template-key hoodie_gildan --template-key mug_new

# Keep only first selected template after filtering
python printify_shopify_sync_pipeline.py --template-key hoodie_gildan --template-key mug_new --limit-templates 1

# Regenerate a valid mug snippet (blueprint 68 + auto-selected provider)
python printify_shopify_sync_pipeline.py --generate-template-snippet --blueprint-id 68 --auto-provider --key mug_new --template-output-file ./mug_template.json
```

## Local-image-first (cheap batch) workflow

This is now a first-class primary workflow when you want to avoid AI image generation cost.

Use existing files in `--image-dir` and run only the first `N` local images:

```bash
# First 3 local images across all active product templates (including tshirt_gildan)
python printify_shopify_sync_pipeline.py --local-image-batch 3

# Equivalent legacy form (kept for compatibility)
python printify_shopify_sync_pipeline.py --max-artworks 3
```

Create/publish/verify directly from local images (no `--generate-artwork-from-prompt`):

```bash
python printify_shopify_sync_pipeline.py --local-image-batch 3 --publish --verify-publish
```

Run storefront QA from local images only:

```bash
python printify_shopify_sync_pipeline.py --local-image-batch 3 --storefront-qa --export-storefront-qa-report ./exports/storefront_qa_local.csv
```

Run a 3-image all-family batch with collection sync + family enforcement:

```bash
python printify_shopify_sync_pipeline.py \
  --local-image-batch 3 \
  --publish \
  --verify-publish \
  --sync-collections \
  --enforce-family-collection-membership \
  --collection-removal-mode conservative \
  --export-run-report ./exports/run_report_local3.csv
```

Current supported template families include:
- `hoodie_gildan`
- `longsleeve_gildan`
- `tshirt_gildan` (new short-sleeve family)
- `sweatshirt_gildan`
- `mug_new`
- `poster_basic`
- `tote_basic`

### Storefront merchandising controls

- **Strict family collections:** family routing is deterministic (`tshirt_gildan -> t-shirts`, `sweatshirt_gildan -> sweatshirts`, etc.) and can be enforced via `--enforce-family-collection-membership`.
- **Collection visuals:** launch-plan rows can now include `collection_image_src` and `collection_sort_order` so collection tiles/sorting are intentionally merchandised.
- **Default mockup diversity:** templates now support `preferred_mockup_colors`, `preferred_default_variant_color`, `preferred_mockup_types`, and `preferred_featured_image_strategy` to avoid white-dominant defaults when alternatives exist.
- **Tote merchandising:** tote front remains primary/front-only while a modest tote front fill boost is applied for stronger collection-card presence.

## Catalog exploration workflows

Use the new read-only catalog tooling to discover blueprint/provider ids and bootstrap template entries.

```bash
# Find candidate blueprints (filter by keyword)
python printify_shopify_sync_pipeline.py --list-blueprints --search-blueprints "heavy cotton tee" --limit-blueprints 15

# List providers for a blueprint with variant/color/size summary
python printify_shopify_sync_pipeline.py --list-providers --blueprint-id 6 --limit-providers 10

# Inspect a specific provider's variants
python printify_shopify_sync_pipeline.py --inspect-variants --blueprint-id 6 --provider-id 99

# Recommend best provider for a blueprint (optionally use template key constraints)
python printify_shopify_sync_pipeline.py --recommend-provider --blueprint-id 6 --template-file ./product_templates.json --key hoodie_gildan

# Generate starter snippet JSON for product_templates.json
python printify_shopify_sync_pipeline.py --generate-template-snippet --blueprint-id 6 --provider-id 99 --key tshirt_new


# Generate starter snippet JSON with automatic provider selection
python printify_shopify_sync_pipeline.py --generate-template-snippet --blueprint-id 6 --auto-provider --key tshirt_new

# Write snippet to a file for direct editing
python printify_shopify_sync_pipeline.py --generate-template-snippet --blueprint-id 6 --provider-id 99 --key tshirt_new --template-output-file ./snippet.json
```

Output is compact by design and includes blueprint/provider identifiers, catalog titles, variant counts, and summarized color/size support.


## Catalog troubleshooting

- If you see `Provider <id> is not available for blueprint <id>`, the provider/blueprint pair is invalid.
- List valid providers for a blueprint:
  - `python printify_shopify_sync_pipeline.py --list-providers --blueprint-id <blueprint_id>`
- Ask InkVibeAuto to rank providers:
  - `python printify_shopify_sync_pipeline.py --recommend-provider --blueprint-id <blueprint_id>`
- Auto-select the top provider when generating a snippet:
  - `python printify_shopify_sync_pipeline.py --generate-template-snippet --blueprint-id <blueprint_id> --auto-provider --key <template_key>`


## Product update/rebuild workflows

By default, InkVibeAuto checks `state.json` for each `artwork_slug:template_key` pair:
- if a Printify product id exists, it updates that product,
- if no id exists, it creates a new product.

```bash
# Default behavior: update existing products when state has ids
python printify_shopify_sync_pipeline.py --template-key hoodie_gildan

# Create-only mode: skip rows that already have a product id
python printify_shopify_sync_pipeline.py --template-key hoodie_gildan --create-only

# Update-only mode: skip rows with no existing product id in state
python printify_shopify_sync_pipeline.py --template-key hoodie_gildan --update-only

# Rebuild mode: create fresh product and replace active state references on next run
python printify_shopify_sync_pipeline.py --template-key hoodie_gildan --rebuild-product

# Auto-rebuild mode: attempt update first, rebuild only on compatibility failures
python printify_shopify_sync_pipeline.py --template-key hoodie_gildan --auto-rebuild-on-incompatible-update
```

Per-template logs now include action (`create/update/rebuild/skip`), product id, provider id, and blueprint id. Run summary now includes created/updated/rebuilt/skipped totals.

### Troubleshooting stale product ids in state

- Symptom: update path fails because the stored id now returns `404` on `GET /shops/{shop_id}/products/{product_id}.json`.
- InkVibeAuto now recovers by treating that id as stale, logging:
  - `Stored product_id not found in Printify; treating as missing and creating a new product`
  and continuing with create for that artwork/template.
- State is updated by the successful run row with the new `printify_product_id`.
- Optional manual reset/rebuild path:
  - `python printify_shopify_sync_pipeline.py --template-key <template_key> --rebuild-product`

### Troubleshooting too many enabled variants

- Symptom: Printify rejects create/update with `Too many variants enabled. Maximum allowed: 100`.
- Controls:
  - tighten `enabled_colors` / `enabled_sizes`,
  - add `enabled_variant_option_filters`,
  - set `max_enabled_variants` on the template.
- InkVibeAuto enforces the max locally before create/update to avoid unnecessary API calls.

## Example: add a new mug or hoodie template from scratch

```bash
# 1) Find blueprint candidates
python printify_shopify_sync_pipeline.py --list-blueprints --search-blueprints mug --limit-blueprints 10

# 2) Rank providers for chosen blueprint
python printify_shopify_sync_pipeline.py --recommend-provider --blueprint-id <blueprint_id>

# 3) Inspect variants/placements for top provider
python printify_shopify_sync_pipeline.py --inspect-variants --blueprint-id <blueprint_id> --provider-id <provider_id>

# 4) Bootstrap snippet and paste into product_templates.json
python printify_shopify_sync_pipeline.py --generate-template-snippet --blueprint-id <blueprint_id> --provider-id <provider_id> --key mug_new --template-output-file ./mug_template.json

# Example known-good mug pair
python printify_shopify_sync_pipeline.py --inspect-variants --blueprint-id 68 --provider-id 1

# 5) Test with one artwork in dry-run
python printify_shopify_sync_pipeline.py --dry-run --template-key mug_new --max-artworks 1
```

## Template schema

`product_templates.json` expects a top-level array of templates with required keys:
- `key`
- `printify_blueprint_id`
- `printify_print_provider_id`
- `placements` (non-empty)

Each placement requires:
- `placement_name`
- `width_px`
- `height_px`

Placement fields (optional):
- `artwork_fit_mode` (`contain` or `cover`, default `contain`)
- `allow_upscale` (`true`/`false`, default `false`; per-placement override that allows contain/cover upscale for undersized art)
- `max_upscale_factor` (number > 0, optional; shirt-only safety cap for contain/cover upscale at placement level)
- `trim_artwork_bounds` (`true`/`false`, default `false`; trims transparent margins before fit)

Pricing fields (optional):
- `base_price`
- `markup_type` (`fixed` or `percent`)
- `markup_value`
- `rounding_mode` (`none`, `whole_dollar`, `x_99`)
- `compare_at_price`

Variant-control fields (optional):
- `max_enabled_variants`
- `enabled_variant_option_filters`

Note: some Printify blueprint/provider combinations expose only one option dimension (for example, size without color). When `enabled_colors` or `enabled_sizes` are configured but that option dimension does not exist in the catalog response, InkVibeAuto ignores the missing dimension filter and logs a warning; existing dimensions remain strictly filtered.

SEO/listing fields (optional):
- `seo_keywords`
- `audience`
- `product_type_label`
- `style_keywords`

Pricing notes:
- Final variant `price` and optional `compare_at_price` are normalized to integer minor units for Printify.
- Existing integer price behavior is preserved, while decimal template values are converted safely.

## Artwork sizing behavior

Many Printify placements require large source files (for example, a front print area of `4500x5400`). Small source images may be rejected by strict validation because they cannot satisfy print-area requirements without interpolation.

By default, InkVibeAuto keeps strict validation. You can now choose controlled fallback behavior:
- `--allow-upscale`: allow enlarging undersized artwork before placement fitting.
- `--upscale-method nearest|lanczos`: choose the upscaling resampling method (default: `lanczos`).
- `--skip-undersized`: skip undersized artwork/template placements with a warning instead of stopping the run.
- `--force`: rerun artworks even if previously marked completed in `state.json`.

Each placement can also set `artwork_fit_mode`:
- `contain` (default): safer mode; preserves full artwork, keeps aspect ratio, and centers on a transparent canvas at placement size (no cropping).
- `cover`: fills the full print area while preserving aspect ratio, which may crop edges.

Template-level optional preprocessing:
- `trim_artwork_bounds: true` trims transparent bounds before contain/cover fitting.
- Default is `false` (no behavior change from previous runs).
- This helps when source images include large transparent margins that make visible artwork appear too small.

Recommendation:
- Shirts and mugs should usually use `artwork_fit_mode: contain` unless you intentionally want a full-bleed/cropped look.
- Current defaults are intentionally split: `hoodie_gildan` front placement enables `allow_upscale: true` with a conservative `max_upscale_factor` cap, while `mug_new` keeps `allow_upscale: false` (conservative/no interpolation by default).
- `poster_basic` now uses a stronger but bounded poster-only fallback: it still prefers `cover` when source resolution is eligible, and for moderately undersized sources it can apply safe bounded enhancement (`poster_safe_max_upscale_factor`, `poster_safe_min_source_ratio`) plus optional poster trim/fill optimization (`poster_trim_fill_optimization`, `poster_fill_target_pct`). If limits are exceeded, it stays on plain `contain` without upscale.
- `tote_basic` supports deterministic placement controls via `active_placements`, `preferred_primary_placement`, and `publish_only_primary_placement` (default front-primary/front-only publish behavior).

Example commands:

```bash
python printify_shopify_sync_pipeline.py --dry-run --skip-undersized
python printify_shopify_sync_pipeline.py --dry-run --allow-upscale
python printify_shopify_sync_pipeline.py --max-artworks 1 --force --allow-upscale
python printify_shopify_sync_pipeline.py --dry-run --allow-upscale --force --upload-strategy auto
python printify_shopify_sync_pipeline.py --allow-upscale --force --max-artworks 1 --upload-strategy r2_url
```

> Upscaling low-resolution source art can reduce final print quality, even when the pipeline completes successfully.



## Launch plan CSV workflow

Use a launch-plan CSV when you want explicit artwork/template pair control and per-row overrides.

```bash
# 1) Export a starter file with headers + sample tee/mug rows
python printify_shopify_sync_pipeline.py --export-launch-plan-template ./launch_plan.csv

# 2) Or export a real launch plan directly from files in images/
python printify_shopify_sync_pipeline.py --export-launch-plan-from-images ./launch_plan.csv

#    You can combine with template filtering and defaults
python printify_shopify_sync_pipeline.py --template-key hoodie_gildan --template-key mug_new \
  --export-launch-plan-from-images ./launch_plan.csv \
  --launch-plan-default-enabled true \
  --include-disabled-template-rows

# 3) Run with launch plan (replaces folder cross-product behavior for this run)
python printify_shopify_sync_pipeline.py --launch-plan ./launch_plan.csv --dry-run
```

Supported CSV columns:
- `artwork_file`, `template_key`, `enabled`
- `title_override`, `description_override`, `tags_override`
- `audience_override`, `style_keywords_override`, `seo_keywords_override`
- `base_price_override`, `markup_type_override`, `markup_value_override`, `compare_at_price_override`
- `publish_after_create_override`
- optional `row_id` (propagates to run/failure reports)

Example rows:

```csv
artwork_file,template_key,enabled,title_override,tags_override,row_id
tee-artwork.png,hoodie_gildan,true,{artwork_title} T-Shirt,"shirt,graphic,launch",tee-001
mug-artwork.png,mug_new,true,,"mug,coffee,launch",mug-001
```

## Placement transform tuning (mockup sizing)

Each placement supports optional transform fields:
- `placement_scale` (visual size)
- `placement_x`, `placement_y` (anchor position)
- `placement_angle` (rotation)

Defaults remain centered (`x=0.5`, `y=0.5`) and unrotated (`angle=0`).
`placement_scale` template caps remain:
- `hoodie_gildan` front: `0.9`
- `mug_new` front: `0.78`

Use dry-run logs to inspect the exact transform values applied per template/placement.

InkVibeAuto now also applies orientation-aware mockup scaling automatically at payload build time:
- orientation buckets: `portrait`, `square`, `landscape`
- template families: mugs use smaller caps than shirts
- shirt orientation presets: portrait=`0.72`, square=`0.70`, landscape=`0.66`
- mug orientation presets (unchanged): portrait=`0.60`, square=`0.58`, landscape=`0.54`
- the runtime scale is `min(template placement_scale, orientation preset)` so manual template overrides are preserved as an upper bound.

## Verifying a successful run

After a non-dry-run execution, verify success by checking logs for:
- upload success lines (placement, strategy, Printify upload image id, and R2 public URL when used),
- product creation lines (Printify product id, product title, enabled variant count),
- explicit state persistence messages after upload/template/artwork completion,
- final per-template summary lines with success/failure, product id, and upload strategy.

Also review `state.json` for updated `processed` and `uploads` records for the latest artwork/template run.

## Title and description generation

- Artwork names are cleaned from filenames/slugs into readable titles (for example converting separators and stripping noisy numeric/version fragments).
- Templates still control title output via `title_pattern`, now with cleaner fallback title values when source titles are filename-like.
- Descriptions still support template override via `description_pattern`.
- If a template uses the basic default description shape, InkVibeAuto now generates a polished generic ecommerce description with:
  - a concise opening hook,
  - short product summary copy,
  - simple feature bullets suitable for POD apparel.

## Current mockup behavior and limitations

- Product create flow continues to use placement image uploads in `print_areas` (existing stable path).
- Publish flow uses Printify publish flags (`title`, `description`, `images`, `variants`, `tags`) and now supports an optional template-level `publish_mockups` override for image/mockup publish control.
- Printify mockup/image selection can vary by channel/provider and is not fully deterministic through this path today.
- TODOs are in code where provider/channel-specific mockup controls can be safely expanded once stable API behavior is confirmed.

## Troubleshooting

- Duplicate title wording (for example, `Signature T-Shirt T-Shirt`) is now de-duplicated automatically when rendered title context already semantically includes the product label (`tee/shirt`, `mug/cup` families).
- Templates using `printify_blueprint_id` 6 must use a valid `printify_print_provider_id` (for example, `99` for Printify Choice).
- Default mug sample now uses blueprint `68` with provider `1` (validate in your region/account with `--list-providers --blueprint-id 68`).
- Printify variant responses can arrive as either a raw list or a `{"variants": [...]}` wrapper; the pipeline normalizes both, but malformed shapes now raise a clear error that includes the top-level type/keys.
- If update calls fail with a payload consistency error (for example, missing `print_areas[*].variant_ids` coverage), InkVibeAuto now validates locally before API calls and reports missing ids directly.
- If update preflight detects incompatibility (blueprint/provider/variant/print-area position mismatch), default behavior is conservative: fail with a recommendation to rerun using `--rebuild-product`.
- Use `--auto-rebuild-on-incompatible-update` only when you explicitly want automatic delete/recreate behavior after failed compatibility checks.

## Backward compatibility

`printful_shopify_sync_pipeline.py` remains a wrapper to preserve legacy invocation.

## Upload strategy

Use `--upload-strategy auto|direct|r2_url` (default: `auto`).

- `auto`: files up to 5MB upload directly to Printify; larger files use R2 URL upload when R2 is configured.
- `direct`: always use direct Printify upload (base64 payload).
- `r2_url`: always upload to R2 and send Printify the public URL (requires all `R2_*` env vars).

`R2_PUBLIC_BASE_URL` should be your public bucket/domain prefix; `r2.dev` URLs are acceptable for development/testing, while a custom domain is preferred for production reliability and branding.

## Batch and multi-template runs

- The pipeline can process multiple product templates per artwork in one run.
- State tracking now records `state_key` as `artwork_slug:template_key` for clearer artwork/template idempotency behavior.
- Final logs include a concise run summary: artworks scanned, templates processed, products created, skipped, failures.


## Product lifecycle and publish verification

InkVibeAuto lifecycle for each `artwork_slug:template_key` pair:
- `create`: no previous product id found in state.
- `update`: existing product id found in state.
- `rebuild`: delete and recreate when `--rebuild-product` is used.
- `publish`: optional post-create/update publish step.
- `verify`: optional post-action readback check of product id/title/variants/print areas and storefront indicators.

Publish controls:
- default behavior stays backward compatible (`template.publish_after_create` controls publish).
- `--publish`: force publish after create/update/rebuild.
- `--skip-publish`: skip publish after create/update/rebuild.
- `--verify-publish`: read product back and log concise verification warnings/success signals.
- `--auto-rebuild-on-incompatible-update`: on update-only incompatibility, automatically switch to rebuild (delete+recreate) for that product.

State helpers:
- `--list-state-keys`: list tracked `artwork_slug:template_key` keys with completion status (`dry-run-only` or `real-completed`).
- `--inspect-state-key <artwork_slug:template_key>`: show the matching state entry as JSON (including completion status).
- `--list-failures`: concise list of failed combinations needing attention.
- `--list-pending`: concise list of combinations not yet successful.

Bulk safety controls:
- `--max-artworks <n>`: cap input artwork files scanned in this run.
- `--local-image-batch <n>`: alias for local-image-first runs (first N discovered local images).
- `--batch-size <n>`: cap processed artwork/template combinations in this run.
- `--stop-after-failures <n>`: stop when N failures are reached.
- `--fail-fast`: stop on first failure.
- `--resume`: skip combinations already successful in **real** (non-dry-run) state and continue pending rows.

Reporting exports:
- `--export-failure-report <path>`: CSV report for failed combinations.
- `--export-run-report <path>`: CSV report for all processed combinations (success/failure/skipped).
- Run report now includes `effective_upscale_factor`, `requested_upscale_factor`, `applied_upscale_factor`, and `upscale_capped`.
- Poster run-report fields now include `poster_cover_eligible`, `poster_enhancement_status`, `poster_requested_upscale_factor`, `poster_applied_upscale_factor`, and `poster_fill_optimization_used`.
- Tote run-report fields now include `tote_primary_placement` and `tote_active_placements`.
- Merch routing report fields now include `template_family` and `product_family_label` so long-sleeve and t-shirt labels remain distinct in diagnostics.

Examples:

```bash
# Create/update without publish
python printify_shopify_sync_pipeline.py --template-key hoodie_gildan --skip-publish

# Create/update and force publish
python printify_shopify_sync_pipeline.py --template-key hoodie_gildan --publish

# Create/update + publish + verification readback
python printify_shopify_sync_pipeline.py --template-key hoodie_gildan --publish --verify-publish

# Inspect state entries
python printify_shopify_sync_pipeline.py --list-state-keys
python printify_shopify_sync_pipeline.py --inspect-state-key cool-cat:hoodie_gildan

# Batch-size examples
python printify_shopify_sync_pipeline.py --batch-size 10 --resume
python printify_shopify_sync_pipeline.py --max-artworks 5 --batch-size 20

# Failure controls
python printify_shopify_sync_pipeline.py --fail-fast
python printify_shopify_sync_pipeline.py --stop-after-failures 3

# Reporting exports
python printify_shopify_sync_pipeline.py --export-failure-report reports/failures.csv
python printify_shopify_sync_pipeline.py --export-run-report reports/run.csv

# Pending/failure inspection
python printify_shopify_sync_pipeline.py --list-failures
python printify_shopify_sync_pipeline.py --list-pending
```

Safer bulk rollout workflow:
1. Dry run first (`--dry-run`) to validate templates/placements. Dry-run entries are recorded for diagnostics but do **not** count as completed for `--resume`.
2. Real run for one artwork (`--max-artworks 1 --publish --verify-publish`).
3. Confirm state + verification fields in `state.json`.
4. Run a small real batch (`--batch-size 5 --resume --export-failure-report reports/failures.csv`).
5. Review failures (`--list-failures`) and fix templates/artwork/provider issues.
6. Resume (`--resume`) until `--list-pending` is empty, then run the full rollout.

## Artwork sidecar metadata (optional)

InkVibeAuto now supports optional per-artwork sidecar metadata files that sit beside image files:

```text
images/my-design.png
images/my-design.json
```

Supported sidecar JSON fields:
- `title`
- `subtitle`
- `description`
- `tags`
- `seo_keywords`
- `audience`
- `style_keywords`
- `theme`
- `collection`
- `color_story`
- `occasion`
- `artist_note`

Behavior:
- If sidecar metadata exists, listing copy prefers metadata values.
- If metadata is missing, filename-based behavior remains in place.
- Low-quality filename stems (UUID/hash/noisy strings) now trigger safer fallback titles.
- Launch-plan row overrides still take precedence over sidecar metadata during listing rendering.

Example sidecar files:
- `examples/tee-artwork.sidecar.example.json`
- `examples/mug-artwork.sidecar.example.json`

### Reviewable artwork metadata generation workflow

Use the metadata generator to create candidate sidecars from image analysis without changing create/update/publish flows.

Safe defaults:
- `--generate-artwork-metadata` with `--metadata-preview` previews only (no writes).
- `--write-sidecars` writes generated sidecars.
- Existing sidecars are preserved by default (`--metadata-only-missing` safe mode).
- `--overwrite-sidecars` is required to replace existing sidecars.
- `--metadata-auto-approve` adds confidence/quality gating so low-confidence results are queued for review instead of written by default.

Common commands:

```bash
# Preview candidate metadata only (no file writes)
python printify_shopify_sync_pipeline.py \
  --generate-artwork-metadata \
  --metadata-preview \
  --image-dir ./images

# Write sidecars for images missing .json files
python printify_shopify_sync_pipeline.py \
  --generate-artwork-metadata \
  --write-sidecars \
  --metadata-max-artworks 20 \
  --image-dir ./images

# Auto-approve only high-confidence metadata and export review queue
python printify_shopify_sync_pipeline.py \
  --generate-artwork-metadata \
  --write-sidecars \
  --metadata-auto-approve \
  --metadata-min-confidence 0.90 \
  --metadata-review-report ./exports/metadata_review.csv \
  --metadata-review-json ./exports/metadata_review.json \
  --image-dir ./images

# Explicitly overwrite existing sidecars (opt-in only)
python printify_shopify_sync_pipeline.py \
  --generate-artwork-metadata \
  --write-sidecars \
  --overwrite-sidecars \
  --image-dir ./images
```

Optional controls:
- `--metadata-max-artworks <n>` limit batch size.
- `--metadata-output-dir <path>` write generated sidecars to a review folder instead of beside source images.
- `--metadata-generator heuristic|vision|openai|auto` chooses generator mode.
- `--metadata-openai-model <model>` overrides model for OpenAI mode.
- `--metadata-openai-timeout <seconds>` sets API timeout for OpenAI mode.
- `--metadata-auto-approve` enables confidence/quality routing (`auto_approved`, `needs_review`, `rejected`).
- `--metadata-min-confidence <float>` sets minimum confidence for auto-approval (default `0.90`).
- `--metadata-review-report <path>` exports spreadsheet-friendly review CSV with reasons/flags and write intent.
- `--metadata-review-json <path>` exports optional JSON review payload.
- `--metadata-write-auto-approved-only` enforces approved-only writes in write mode.
- `--metadata-allow-review-writes` allows writing `needs_review` candidates (off by default).

OpenAI mode notes:
- Set `OPENAI_API_KEY` in your environment (or `.env`) to enable OpenAI-backed vision metadata.
- Optional `OPENAI_MODEL` env var sets the default model (unless overridden by `--metadata-openai-model`).
- `auto` mode now prefers `openai`, then local `vision`, then `heuristic` fallback.
- Missing key / API errors gracefully fall back and are surfaced in preview `sources:` output.

Preview listing copy without creating/updating products:

```bash
python printify_shopify_sync_pipeline.py --preview-listing-copy --template-key hoodie_gildan --max-artworks 5
```

Run metadata-backed artwork in dry-run mode:

```bash
python printify_shopify_sync_pipeline.py --dry-run --force --template-key mug_new --max-artworks 3
```

## Phase 8: Printify UI automation helper

Use `printify_ui_automation.py` for semi-automated Printify Product Creator actions driven by setup packets/checklists.

Key behaviors:
- Requires explicit targeting (`--listing-slug`, `--row-id`, or `--synced-manual-only`).
- Reads `shopify_personalization_setup_checklist.csv`, queue CSV metadata, and setup packet JSON files.
- Supports `--dry-run` and `--screenshot-only` safety modes.
- Captures before/after screenshots, action logs, and a `ui_automation_report.(json|csv)` containing:
  - `ui_automation_status`
  - `ui_automation_last_run_at`
  - `ui_automation_last_result`
  - `ui_automation_screenshot_paths`
- Stops with diagnostics if required selectors cannot be found.
- Optionally generates a one-time Shopify checklist with `--generate-shopify-theme-checklist`.

Example dry-run for two synced/manual setup products:

```bash
python printify_ui_automation.py \
  --checklist-csv examples/ui_automation/shopify_personalization_setup_checklist.sample.csv \
  --queue-csv examples/ui_automation/launch_queue.sample.csv \
  --setup-packet-dir examples/setup_packets \
  --synced-manual-only \
  --dry-run \
  --headless \
  --output-dir ./exports/ui_automation
```

Example live-mode single-target run:

```bash
python printify_ui_automation.py \
  --checklist-csv ./exports/shopify_personalization_setup_checklist.csv \
  --queue-csv ./exports/launch_queue.csv \
  --setup-packet-dir ./exports/setup_packets \
  --listing-slug ocean-bloom-text-tee \
  --pause-per-product \
  --generate-shopify-theme-checklist \
  --output-dir ./exports/ui_automation
```

> Live mode requires Playwright (`pip install playwright` and browser install) and an authenticated Printify session context.

## Known-good baseline commands

- `python printify_shopify_sync_pipeline.py --dry-run --template-key hoodie_gildan`
- `python printify_shopify_sync_pipeline.py --dry-run --template-key mug_new`
- `python printify_shopify_sync_pipeline.py --publish --verify-publish`
- `python printify_shopify_sync_pipeline.py --rebuild-product --resume`
- `python printify_shopify_sync_pipeline.py --export-launch-plan-from-images ./exports/launch_plan.csv`

## Reporting and launch-plan metadata

Run report rows now include launch-plan row context (`launch_plan_row`, `launch_plan_row_id`) for successful rows and can also carry optional collection metadata columns (`collection_handle`, `collection_title`, `collection_description`, `launch_name`, `campaign`, `merch_theme`) when provided in launch-plan CSV mode.

Collection sync is optional and explicit:

- `--sync-collections`: enable Shopify custom/manual collection resolve/create/update + product membership attach for launch-plan rows that include collection metadata.
- `--skip-collections`: explicitly disable collection sync even if `--sync-collections` is present.
- `--verify-collections`: read-only membership verification after collection sync.

Collection behavior is idempotent by design: handle-first resolution (title fallback), reuse existing collections on reruns, and no-op membership checks before creating a collect row.

Example:

```bash
python printify_shopify_sync_pipeline.py \
  --launch-plan ./launch_plan.csv \
  --sync-collections \
  --verify-collections \
  --export-run-report ./exports/run.csv
```

## Compact state behavior and migration

State handling is centralized in `state_store.py`. Legacy `state.json` rows still load through migration helpers, and status semantics for resume are preserved (`dry-run-only` remains resumable, `real-completed` remains non-resumable).

See migration note: `docs/state-migration-note.md`.

## When to use --skip-audit

Use `--skip-audit` when iterating quickly in local dry-run/testing loops where Printify catalog/shop reachability is already known and you want faster startup. Keep audits enabled for production launches.

## Placement QA preview mode

Use `--placement-preview` to emit local preview composites to `exports/previews/` before upload. This mode is read-only and optional.

## Storefront QA mode (read-only)

Use `--storefront-qa` to run a non-mutating listing-quality audit across rendered titles, descriptions, tags, pricing/compare-at summaries, variant option structure, and publish image/mockup intent. This mode does **not** create, update, rebuild, publish, or delete products.

Examples:

```bash
# Spreadsheet-ready CSV report
python printify_shopify_sync_pipeline.py \
  --storefront-qa \
  --export-storefront-qa-report ./exports/storefront_qa.csv

# Optional JSON export + strict exit behavior
python printify_shopify_sync_pipeline.py \
  --storefront-qa \
  --strict-storefront-qa \
  --export-storefront-qa-report ./exports/storefront_qa.csv \
  --export-storefront-qa-json ./exports/storefront_qa.json
```

Notes:
- QA rows include launch-plan metadata when run with `--launch-plan`.
- Mockup/image selection is still partially provider/channel-dependent in Printify, so this report flags publish intent and placement context, but cannot guarantee exact storefront mockup ordering.
