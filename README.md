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
python printify_shopify_sync_pipeline.py --max-artworks 1 --template-key tshirt_gildan --template-key mug_11oz

# Keep only first selected template after filtering
python printify_shopify_sync_pipeline.py --template-key tshirt_gildan --template-key mug_11oz --limit-templates 1
```

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
python printify_shopify_sync_pipeline.py --recommend-provider --blueprint-id 6 --template-file ./product_templates.json --key tshirt_gildan

# Generate starter snippet JSON for product_templates.json
python printify_shopify_sync_pipeline.py --generate-template-snippet --blueprint-id 6 --provider-id 99 --key tshirt_new

# Write snippet to a file for direct editing
python printify_shopify_sync_pipeline.py --generate-template-snippet --blueprint-id 6 --provider-id 99 --key tshirt_new --template-output-file ./snippet.json
```

Output is compact by design and includes blueprint/provider identifiers, catalog titles, variant counts, and summarized color/size support.


## Product update/rebuild workflows

By default, InkVibeAuto checks `state.json` for each `artwork_slug:template_key` pair:
- if a Printify product id exists, it updates that product,
- if no id exists, it creates a new product.

```bash
# Default behavior: update existing products when state has ids
python printify_shopify_sync_pipeline.py --template-key tshirt_gildan

# Create-only mode: skip rows that already have a product id
python printify_shopify_sync_pipeline.py --template-key tshirt_gildan --create-only

# Update-only mode: skip rows with no existing product id in state
python printify_shopify_sync_pipeline.py --template-key tshirt_gildan --update-only

# Rebuild mode: create fresh product and replace active state references on next run
python printify_shopify_sync_pipeline.py --template-key tshirt_gildan --rebuild-product
```

Per-template logs now include action (`create/update/rebuild/skip`), product id, provider id, and blueprint id. Run summary now includes created/updated/rebuilt/skipped totals.

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

Pricing fields (optional):
- `base_price`
- `markup_type` (`fixed` or `percent`)
- `markup_value`
- `rounding_mode` (`none`, `whole_dollar`, `x_99`)
- `compare_at_price`

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
- `--allow-upscale`: upscale undersized artwork proportionally to cover the required placement area, then crop/fit to exact dimensions.
- `--upscale-method nearest|lanczos`: choose the upscaling resampling method (default: `lanczos`).
- `--skip-undersized`: skip undersized artwork/template placements with a warning instead of stopping the run.
- `--force`: rerun artworks even if previously marked completed in `state.json`.

Example commands:

```bash
python printify_shopify_sync_pipeline.py --dry-run --skip-undersized
python printify_shopify_sync_pipeline.py --dry-run --allow-upscale
python printify_shopify_sync_pipeline.py --max-artworks 1 --force --allow-upscale
python printify_shopify_sync_pipeline.py --dry-run --allow-upscale --force --upload-strategy auto
python printify_shopify_sync_pipeline.py --allow-upscale --force --max-artworks 1 --upload-strategy r2_url
```

> Upscaling low-resolution source art can reduce final print quality, even when the pipeline completes successfully.


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

- Templates using `printify_blueprint_id` 6 must use a valid `printify_print_provider_id` (for example, `99` for Printify Choice).
- Printify variant responses can arrive as either a raw list or a `{"variants": [...]}` wrapper; the pipeline normalizes both, but malformed shapes now raise a clear error that includes the top-level type/keys.

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

