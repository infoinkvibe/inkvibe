# InkVibeAuto

InkVibeAuto is a Python automation pipeline for print-on-demand stores that converts local artwork into fully configured products for **Printify** and **Shopify**.

It scans a local image folder, validates artwork against product placement requirements, generates print-ready exports, uploads assets to the Printify media library, resolves valid provider-specific variants, creates products in Printify, optionally publishes them, and can also sync product data into Shopify.

## Current workflow

1. Discover artwork from `./images`
2. Load product templates from `product_templates.json`
3. Validate image dimensions against template placement requirements
4. Export print-ready PNG assets into `./exports`
5. Upload those exports to Printify media library
6. Resolve valid variants from Printify blueprint + print provider catalog endpoints
7. Create Printify product payloads with `blueprint_id`, `print_provider_id`, `variants`, and `print_areas`
8. Publish products in Printify
9. Optionally create/update matching products in Shopify through Admin GraphQL
10. Persist state in `state.json` for idempotent reruns

## Tech stack

- Python
- `requests`
- `Pillow`
- `dataclasses`
- `python-dotenv`
- Printify REST API
- Shopify Admin GraphQL API

## Files

```text
InkVibeAuto/
├─ printify_shopify_sync_pipeline.py
├─ printful_shopify_sync_pipeline.py   # compatibility wrapper
├─ product_templates.json
├─ images/
├─ exports/
├─ state.json
├─ .env
├─ .env.example
├─ .gitignore
└─ README.md
```

## Environment variables

```env
PRINTIFY_API_TOKEN=your_printify_personal_access_token
PRINTIFY_SHOP_ID=your_printify_shop_id
SHOPIFY_ADMIN_TOKEN=your_shopify_admin_token
SHOPIFY_STORE_DOMAIN=your-store.myshopify.com
SHOPIFY_API_VERSION=2025-10
IMAGE_DIR=./images
EXPORT_DIR=./exports
STATE_PATH=./state.json
TEMPLATES_CONFIG=./product_templates.json
PRINTIFY_USER_AGENT=InkVibeAuto/1.0
DEFAULT_PRICE_FALLBACK=29.99
RETRY_MAX_ATTEMPTS=5
RETRY_BACKOFF_SECONDS=1.5
```

## Install

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install requests pillow python-dotenv
```

## Run

Normal execution:

```bash
python printify_shopify_sync_pipeline.py
```

Dry run:

```bash
python printify_shopify_sync_pipeline.py --dry-run
```

Force reprocessing even if `state.json` already contains the artwork slug:

```bash
python printify_shopify_sync_pipeline.py --force
```

## Template schema

Each template should use Printify blueprint/provider metadata instead of Printful catalog IDs:

```json
{
  "key": "tshirt_gildan",
  "printify_blueprint_id": 6,
  "printify_print_provider_id": 1,
  "title_pattern": "{artwork_title} T-Shirt",
  "description_pattern": "<p>{artwork_title} printed on premium cotton.</p>",
  "enabled_colors": ["Black", "White"],
  "enabled_sizes": ["S", "M", "L", "XL"],
  "tags": ["shirt", "graphic"],
  "placements": [
    {
      "placement_name": "front",
      "width_px": 4500,
      "height_px": 5400
    }
  ]
}
```

## Notes

- Printify uses bearer-token authentication with a Personal Access Token for single-account use. Tokens are valid for one year. citeturn0search0
- Printify’s catalog is organized around blueprints, print providers, and provider-specific variants. Variants are fetched from `/v1/catalog/blueprints/{blueprint_id}/print_providers/{print_provider_id}/variants.json`. citeturn1search0turn2search0
- Printify uploads support either a file URL or base64-encoded contents at `POST /v1/uploads/images.json`, and Printify recommends URL-based uploads for files larger than 5 MB. citeturn3search0
- Shopify’s modern admin flow uses GraphQL. `productCreate` is limited to the initial variant, while `productVariantsBulkCreate` and `productSet` are the better fit for multi-variant sync flows. citeturn0search1turn0search2turn0search3
- The pipeline includes TODO markers where Printify placement transform details may need adjustment per blueprint/provider combination.

## Suggested follow-up after migration

1. Create one product manually in Printify for each product family you want to automate.
2. Fetch that product through the Printify API and compare its `print_areas` / placeholder structure to the generated payload.
3. Adjust placement transforms if a provider expects different image positioning, scaling, or extra placeholder fields.
4. Test with `--dry-run` first, then run against a development shop.
