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
- Image files >5MB are uploaded as base64 in the current implementation; URL uploads are recommended where possible.
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

## Troubleshooting

- Templates using `printify_blueprint_id` 6 must use a valid `printify_print_provider_id` (for example, `99` for Printify Choice).

## Backward compatibility

`printful_shopify_sync_pipeline.py` remains a wrapper to preserve legacy invocation.
