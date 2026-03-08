from __future__ import annotations

import argparse
import base64
import json
import logging
import math
import os
import pathlib
import random
import re
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from PIL import Image, ImageOps

load_dotenv()

# -----------------------------
# Configuration
# -----------------------------

PRINTIFY_API_BASE = os.getenv("PRINTIFY_API_BASE", "https://api.printify.com/v1")
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "your-store.myshopify.com")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-10")
PRINTIFY_API_TOKEN = os.getenv("PRINTIFY_API_TOKEN", "")
PRINTIFY_SHOP_ID = os.getenv("PRINTIFY_SHOP_ID", "")
SHOPIFY_ADMIN_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN", "")

IMAGE_DIR = pathlib.Path(os.getenv("IMAGE_DIR", "./images"))
EXPORT_DIR = pathlib.Path(os.getenv("EXPORT_DIR", "./exports"))
STATE_PATH = pathlib.Path(os.getenv("STATE_PATH", "./state.json"))
TEMPLATES_CONFIG = pathlib.Path(os.getenv("TEMPLATES_CONFIG", "./product_templates.json"))

DEFAULT_TAGS = ["print-on-demand", "printify"]
DEFAULT_VENDOR = "Printify"
DEFAULT_PRODUCT_STATUS = os.getenv("SHOPIFY_PRODUCT_STATUS", "DRAFT")
DEFAULT_PRICE_FALLBACK = os.getenv("DEFAULT_PRICE_FALLBACK", "29.99")
USER_AGENT = os.getenv("PRINTIFY_USER_AGENT", "InkVibeAuto/1.1")
RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "5"))
RETRY_BACKOFF_SECONDS = float(os.getenv("RETRY_BACKOFF_SECONDS", "1.5"))

logger = logging.getLogger("inkvibeauto")


# -----------------------------
# Models / exceptions
# -----------------------------


class DryRunMutationSkipped(RuntimeError):
    pass


class TemplateValidationError(ValueError):
    pass


class StateFileError(RuntimeError):
    pass


@dataclass
class Artwork:
    slug: str
    src_path: pathlib.Path
    title: str
    description_html: str
    tags: List[str]
    image_width: int
    image_height: int
    dpi_hint: Optional[int] = None


@dataclass
class PlacementRequirement:
    placement_name: str
    width_px: int
    height_px: int
    file_type: str = "png"
    allow_upscale: bool = False
    transparent_background_required: bool = False
    padding_pct: float = 0.0


@dataclass
class ProductTemplate:
    key: str
    printify_blueprint_id: int
    printify_print_provider_id: int
    title_pattern: str
    description_pattern: str
    enabled_colors: List[str] = field(default_factory=list)
    enabled_sizes: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    placements: List[PlacementRequirement] = field(default_factory=list)
    shopify_product_type: str = "Apparel"
    publish_to_shopify: bool = False
    push_via_printify: bool = True
    publish_after_create: bool = True
    publish_title: bool = True
    publish_description: bool = True
    publish_images: bool = True
    publish_variants: bool = True
    publish_tags: bool = True
    default_price: str = DEFAULT_PRICE_FALLBACK


@dataclass
class PreparedArtwork:
    artwork: Artwork
    template: ProductTemplate
    placement: PlacementRequirement
    export_path: pathlib.Path
    width_px: int
    height_px: int


# -----------------------------
# Logging / helpers
# -----------------------------


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def _compute_backoff(attempt: int) -> float:
    base = RETRY_BACKOFF_SECONDS * (2 ** max(0, attempt - 1))
    jitter = random.uniform(0.0, RETRY_BACKOFF_SECONDS / 2)
    return base + jitter


def _retry_after_seconds(value: Optional[str], attempt: int) -> float:
    if value:
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                dt = parsedate_to_datetime(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
            except Exception:
                pass
    return _compute_backoff(attempt)


def load_json(path: pathlib.Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        backup = path.with_suffix(path.suffix + ".corrupt")
        path.rename(backup)
        logger.error("Invalid JSON in %s; moved to %s", path, backup)
        raise StateFileError(f"Invalid JSON in {path}: {exc}") from exc


def save_json_atomic(path: pathlib.Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=path.parent, prefix=f".{path.name}.", suffix=".tmp") as tmp:
        json.dump(data, tmp, indent=2, ensure_ascii=False)
        tmp.write("\n")
        temp_name = tmp.name
    os.replace(temp_name, path)


def ensure_state_shape(state: Dict[str, Any]) -> Dict[str, Any]:
    state.setdefault("processed", {})
    state.setdefault("uploads", {})
    state.setdefault("shopify", {})
    state.setdefault("printify", {})
    return state


# -----------------------------
# HTTP clients
# -----------------------------


class BaseApiClient:
    def __init__(self, base_url: str, headers: Dict[str, str], dry_run: bool = False):
        self.base_url = base_url.rstrip("/")
        self.dry_run = dry_run
        self.session = requests.Session()
        self.session.headers.update(headers)

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        expected_statuses: Iterable[int] = (200, 201, 202),
        mutating: bool = False,
    ) -> Any:
        if mutating and self.dry_run:
            logger.info("[dry-run] %s %s", method.upper(), path)
            raise DryRunMutationSkipped(f"dry-run skipped {method.upper()} {path}")

        url = f"{self.base_url}{path}"
        last_exc: Optional[Exception] = None

        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                response = self.session.request(method=method.upper(), url=url, params=params, json=payload, timeout=120)
                if response.status_code in expected_statuses:
                    return response.json() if response.content else {}

                if response.status_code in {429, 500, 502, 503, 504}:
                    sleep_seconds = _retry_after_seconds(response.headers.get("Retry-After"), attempt)
                    logger.warning(
                        "Request %s %s failed with %s (%s/%s), retrying in %.2fs",
                        method.upper(),
                        path,
                        response.status_code,
                        attempt,
                        RETRY_MAX_ATTEMPTS,
                        sleep_seconds,
                    )
                    time.sleep(sleep_seconds)
                    continue

                body = response.text
                try:
                    body = response.json()
                except Exception:
                    pass
                raise RuntimeError(f"HTTP {response.status_code} for {method.upper()} {path}: {body}")
            except DryRunMutationSkipped:
                raise
            except Exception as exc:
                last_exc = exc
                if attempt >= RETRY_MAX_ATTEMPTS:
                    break
                sleep_seconds = _compute_backoff(attempt)
                logger.warning("Request exception for %s %s (%s/%s): %s", method.upper(), path, attempt, RETRY_MAX_ATTEMPTS, exc)
                time.sleep(sleep_seconds)

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"Request failed for {method.upper()} {path}")

    def get(self, path: str, **params: Any) -> Any:
        return self._request("GET", path, params=params or None, mutating=False)

    def post(self, path: str, payload: Dict[str, Any]) -> Any:
        return self._request("POST", path, payload=payload, mutating=True)

    def put(self, path: str, payload: Dict[str, Any]) -> Any:
        return self._request("PUT", path, payload=payload, mutating=True)


class PrintifyClient(BaseApiClient):
    def __init__(self, api_token: str, dry_run: bool = False):
        super().__init__(
            base_url=PRINTIFY_API_BASE,
            headers={
                "Authorization": f"Bearer {api_token}",
                "User-Agent": USER_AGENT,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            dry_run=dry_run,
        )

    def list_shops(self) -> List[Dict[str, Any]]:
        return self.get("/shops.json")

    def list_blueprints(self) -> List[Dict[str, Any]]:
        return self.get("/catalog/blueprints.json")

    def list_print_providers(self, blueprint_id: int) -> List[Dict[str, Any]]:
        return self.get(f"/catalog/blueprints/{blueprint_id}/print_providers.json")

    def list_variants(self, blueprint_id: int, print_provider_id: int, show_out_of_stock: bool = True) -> List[Dict[str, Any]]:
        response = self.get(
            f"/catalog/blueprints/{blueprint_id}/print_providers/{print_provider_id}/variants.json",
            **{"show-out-of-stock": 1 if show_out_of_stock else 0},
        )
        return normalize_catalog_variants_response(response)

    def upload_image(self, *, file_path: Optional[pathlib.Path] = None, image_url: Optional[str] = None) -> Dict[str, Any]:
        if file_path is None and image_url is None:
            raise ValueError("upload_image requires file_path or image_url")

        payload: Dict[str, Any] = {}
        if file_path is not None:
            payload["file_name"] = file_path.name
            if file_path.stat().st_size > 5 * 1024 * 1024:
                logger.warning("Large file detected (>5MB): %s. Printify recommends URL uploads.", file_path.name)
            payload["contents"] = base64.b64encode(file_path.read_bytes()).decode("ascii")
        else:
            payload["file_name"] = pathlib.Path(image_url or "image.png").name or "image.png"
            payload["url"] = image_url

        return self.post("/uploads/images.json", payload)

    def create_product(self, shop_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.post(f"/shops/{shop_id}/products.json", payload)

    def publish_product(self, shop_id: int, product_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.post(f"/shops/{shop_id}/products/{product_id}/publish.json", payload)


# -----------------------------
# Shopify integration isolation
# -----------------------------


class ShopifyClient(BaseApiClient):
    def __init__(self, admin_token: str, dry_run: bool = False):
        super().__init__(
            base_url=f"https://{SHOPIFY_STORE_DOMAIN}",
            headers={
                "X-Shopify-Access-Token": admin_token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            dry_run=dry_run,
        )

    def graphql(self, query: str, variables: Dict[str, Any], *, mutating: bool = True) -> Dict[str, Any]:
        if mutating and self.dry_run:
            logger.info("[dry-run] Shopify GraphQL mutation skipped")
            raise DryRunMutationSkipped("dry-run skipped Shopify GraphQL mutation")

        path = f"/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            response = self.session.post(
                f"{self.base_url}{path}",
                json={"query": query, "variables": variables},
                timeout=120,
            )
            if response.status_code in {429, 500, 502, 503, 504}:
                time.sleep(_retry_after_seconds(response.headers.get("Retry-After"), attempt))
                continue
            response.raise_for_status()
            data = response.json()
            if data.get("errors"):
                raise RuntimeError(f"Shopify GraphQL top-level errors: {data['errors']}")
            return data["data"]

        raise RuntimeError("Shopify GraphQL request failed")

    def product_set(self, identifier: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
        mutation = """
        mutation productSet($identifier: ProductSetIdentifiers!, $input: ProductSetInput!) {
          productSet(identifier: $identifier, input: $input, synchronous: true) {
            product {
              id
              title
              variants(first: 100) { nodes { id title } }
            }
            userErrors { field message code }
          }
        }
        """
        data = self.graphql(mutation, {"identifier": identifier, "input": payload})["productSet"]
        if data.get("userErrors"):
            raise RuntimeError(f"Shopify productSet errors: {data['userErrors']}")
        return data["product"]


def _variant_option_value(variant: Dict[str, Any], key: str) -> str:
    options = variant.get("options") or {}
    if isinstance(options, dict):
        return str(options.get(key, "")).strip()
    return str(variant.get(key, "")).strip()


def build_shopify_product_options(variant_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    colors = sorted({c for c in (_variant_option_value(v, "color") for v in variant_rows) if c})
    sizes = sorted({s for s in (_variant_option_value(v, "size") for v in variant_rows) if s})

    product_options: List[Dict[str, Any]] = []
    if colors:
        product_options.append({"name": "Color", "values": [{"name": value} for value in colors]})
    if sizes:
        product_options.append({"name": "Size", "values": [{"name": value} for value in sizes]})
    if not product_options:
        product_options.append({"name": "Title", "values": [{"name": "Default Title"}]})

    variants: List[Dict[str, Any]] = []
    for variant in variant_rows:
        option_values: List[Dict[str, str]] = []
        color = _variant_option_value(variant, "color")
        size = _variant_option_value(variant, "size")
        if colors and color:
            option_values.append({"optionName": "Color", "name": color})
        if sizes and size:
            option_values.append({"optionName": "Size", "name": size})
        if not option_values:
            option_values.append({"optionName": "Title", "name": "Default Title"})

        price_cents = variant.get("price") or variant.get("cost") or variant.get("price_cents")
        if isinstance(price_cents, str) and price_cents.isdigit():
            price_value = f"{int(price_cents) / 100:.2f}"
        elif isinstance(price_cents, int):
            price_value = f"{price_cents / 100:.2f}"
        elif price_cents is not None:
            price_value = str(price_cents)
        else:
            price_value = DEFAULT_PRICE_FALLBACK

        variants.append({
            "optionValues": option_values,
            "price": price_value,
            "inventoryPolicy": "CONTINUE",
            "taxable": True,
            "inventoryItem": {"tracked": False},
        })

    return product_options, variants


def create_in_shopify_only(
    shopify: ShopifyClient,
    artwork: Artwork,
    template: ProductTemplate,
    variant_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    title = template.title_pattern.format(artwork_title=artwork.title).strip()
    description_html = template.description_pattern.format(artwork_title=artwork.title).strip()
    tags = list(dict.fromkeys(DEFAULT_TAGS + artwork.tags + template.tags))
    product_options, variants = build_shopify_product_options(variant_rows)
    handle = slugify(f"{artwork.slug}-{template.key}")

    payload = {
        "title": title,
        "descriptionHtml": description_html,
        "vendor": DEFAULT_VENDOR,
        "productType": template.shopify_product_type,
        "tags": tags,
        "status": DEFAULT_PRODUCT_STATUS,
        "productOptions": product_options,
        "variants": variants,
    }

    try:
        product = shopify.product_set(identifier={"handle": handle}, payload=payload)
    except DryRunMutationSkipped:
        return {"status": "dry-run", "shopify_handle": handle}

    return {
        "shopify_product_id": product["id"],
        "shopify_variant_ids": [node["id"] for node in product.get("variants", {}).get("nodes", [])],
        "shopify_handle": handle,
    }


# -----------------------------
# Core image prep
# -----------------------------


def discover_artworks(image_dir: pathlib.Path) -> List[Artwork]:
    supported = {".png", ".jpg", ".jpeg", ".webp"}
    artworks: List[Artwork] = []

    for path in sorted(image_dir.glob("**/*")):
        if path.suffix.lower() not in supported or not path.is_file():
            continue

        with Image.open(path) as im:
            width, height = im.size

        stem = path.stem.replace("_", " ").replace("-", " ").strip()
        title = stem.title()
        artworks.append(
            Artwork(
                slug=slugify(path.stem),
                src_path=path,
                title=title,
                description_html=f"<p>{title}</p>",
                tags=[],
                image_width=width,
                image_height=height,
            )
        )

    return artworks


def validate_artwork_for_placement(artwork: Artwork, placement: PlacementRequirement) -> Tuple[bool, str]:
    too_small = artwork.image_width < placement.width_px or artwork.image_height < placement.height_px
    if too_small and not placement.allow_upscale:
        return False, (
            f"image too small ({artwork.image_width}x{artwork.image_height}) for "
            f"placement {placement.placement_name} ({placement.width_px}x{placement.height_px})"
        )
    return True, "ok"


def prepare_artwork_export(artwork: Artwork, template: ProductTemplate, placement: PlacementRequirement, export_dir: pathlib.Path) -> PreparedArtwork:
    export_path = export_dir / template.key / f"{artwork.slug}-{placement.placement_name}.png"
    export_path.parent.mkdir(parents=True, exist_ok=True)

    valid, reason = validate_artwork_for_placement(artwork, placement)
    if not valid:
        raise ValueError(reason)

    with Image.open(artwork.src_path) as im:
        im = ImageOps.exif_transpose(im).convert("RGBA")
        scale = max(placement.width_px / im.width, placement.height_px / im.height)
        if scale > 1 and not placement.allow_upscale:
            raise ValueError("upscale required but disabled")
        resized = im.resize((math.ceil(im.width * scale), math.ceil(im.height * scale)), Image.LANCZOS)
        left = max(0, (resized.width - placement.width_px) // 2)
        top = max(0, (resized.height - placement.height_px) // 2)
        resized.crop((left, top, left + placement.width_px, top + placement.height_px)).save(export_path, "PNG")

    return PreparedArtwork(artwork=artwork, template=template, placement=placement, export_path=export_path, width_px=placement.width_px, height_px=placement.height_px)


# -----------------------------
# Product mapping and validation
# -----------------------------


def _validate_template_row(row: Dict[str, Any], index: int) -> None:
    required = ["key", "printify_blueprint_id", "printify_print_provider_id", "placements"]
    for field_name in required:
        if field_name not in row:
            raise TemplateValidationError(f"Template[{index}] missing required field '{field_name}'")
    if not isinstance(row["placements"], list) or not row["placements"]:
        raise TemplateValidationError(f"Template[{index}] placements must be a non-empty list")
    for pidx, placement in enumerate(row["placements"]):
        for field_name in ["placement_name", "width_px", "height_px"]:
            if field_name not in placement:
                raise TemplateValidationError(f"Template[{index}] placement[{pidx}] missing '{field_name}'")


def load_templates(config_path: pathlib.Path) -> List[ProductTemplate]:
    raw = load_json(config_path, [])
    if not isinstance(raw, list):
        raise TemplateValidationError("product_templates.json must contain a top-level JSON array")

    keys: set[str] = set()
    templates: List[ProductTemplate] = []
    for idx, row in enumerate(raw):
        if not isinstance(row, dict):
            raise TemplateValidationError(f"Template[{idx}] must be a JSON object")
        _validate_template_row(row, idx)
        key = str(row["key"])
        if key in keys:
            raise TemplateValidationError(f"Duplicate template key '{key}'")
        keys.add(key)

        templates.append(
            ProductTemplate(
                key=key,
                printify_blueprint_id=int(row["printify_blueprint_id"]),
                printify_print_provider_id=int(row["printify_print_provider_id"]),
                title_pattern=row.get("title_pattern", "{artwork_title}"),
                description_pattern=row.get("description_pattern", "<p>{artwork_title}</p>"),
                enabled_colors=row.get("enabled_colors", []),
                enabled_sizes=row.get("enabled_sizes", []),
                tags=row.get("tags", []),
                shopify_product_type=row.get("shopify_product_type", "Apparel"),
                publish_to_shopify=bool(row.get("publish_to_shopify", False)),
                push_via_printify=bool(row.get("push_via_printify", True)),
                publish_after_create=bool(row.get("publish_after_create", True)),
                publish_title=bool(row.get("publish_title", True)),
                publish_description=bool(row.get("publish_description", True)),
                publish_images=bool(row.get("publish_images", True)),
                publish_variants=bool(row.get("publish_variants", True)),
                publish_tags=bool(row.get("publish_tags", True)),
                default_price=str(row.get("default_price", DEFAULT_PRICE_FALLBACK)),
                placements=[PlacementRequirement(**p) for p in row.get("placements", [])],
            )
        )

    return templates


def choose_variants_from_catalog(catalog_variants: Any, template: ProductTemplate) -> List[Dict[str, Any]]:
    catalog_variants = normalize_catalog_variants_response(catalog_variants)
    chosen: List[Dict[str, Any]] = []
    for variant in catalog_variants:
        color = _variant_option_value(variant, "color")
        size = _variant_option_value(variant, "size")
        is_available = variant.get("is_available", True)
        if (not template.enabled_colors or color in template.enabled_colors) and (not template.enabled_sizes or size in template.enabled_sizes) and is_available:
            chosen.append(variant)
    return chosen


def normalize_catalog_variants_response(raw_variants: Any) -> List[Dict[str, Any]]:
    response_type = type(raw_variants).__name__
    logger.debug("Printify variants response top-level type: %s", response_type)

    if isinstance(raw_variants, list):
        variants = raw_variants
    elif isinstance(raw_variants, dict):
        if "variants" not in raw_variants:
            raise ValueError(
                "Malformed Printify variants response: expected list or dict with 'variants' key; "
                f"got dict keys={sorted(raw_variants.keys())}"
            )
        variants = raw_variants["variants"]
    else:
        raise ValueError(
            "Malformed Printify variants response: expected list or dict with 'variants' key; "
            f"got type={response_type}"
        )

    if not isinstance(variants, list):
        raise ValueError(
            "Malformed Printify variants response: 'variants' must be a list; "
            f"got type={type(variants).__name__}"
        )

    non_dict_index = next((idx for idx, row in enumerate(variants) if not isinstance(row, dict)), None)
    if non_dict_index is not None:
        raise ValueError(
            "Malformed Printify variants response: each variant must be an object; "
            f"got type={type(variants[non_dict_index]).__name__} at index={non_dict_index}"
        )

    logger.debug("Printify variants found: %s", len(variants))
    return variants


def build_printify_product_payload(artwork: Artwork, template: ProductTemplate, variant_rows: List[Dict[str, Any]], upload_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    title = template.title_pattern.format(artwork_title=artwork.title).strip()
    description_html = template.description_pattern.format(artwork_title=artwork.title).strip()
    tags = list(dict.fromkeys(DEFAULT_TAGS + artwork.tags + template.tags))

    variants_payload: List[Dict[str, Any]] = []
    enabled_variant_ids: List[int] = []
    for variant in variant_rows:
        variant_id = int(variant["id"])
        enabled_variant_ids.append(variant_id)
        price_cents = variant.get("price") or variant.get("cost") or variant.get("price_cents")
        if isinstance(price_cents, str) and price_cents.isdigit():
            price_value = f"{int(price_cents) / 100:.2f}"
        elif isinstance(price_cents, int):
            price_value = f"{price_cents / 100:.2f}"
        elif price_cents is not None:
            price_value = str(price_cents)
        else:
            price_value = template.default_price
        variants_payload.append({"id": variant_id, "price": price_value, "is_enabled": True})

    print_areas: List[Dict[str, Any]] = []
    for placement in template.placements:
        upload_info = upload_map[placement.placement_name]
        print_areas.append({
            "variant_ids": enabled_variant_ids,
            "placeholders": [{
                "position": placement.placement_name,
                "images": [{"id": upload_info["id"], "x": 0.5, "y": 0.5, "scale": 1, "angle": 0}],
            }],
        })
        # TODO: Provider-specific print_areas can differ by blueprint/provider. Validate generated transforms
        # TODO: against a manually created product for each provider and adjust placement fields as needed.

    return {
        "title": title,
        "description": description_html,
        "blueprint_id": template.printify_blueprint_id,
        "print_provider_id": template.printify_print_provider_id,
        "variants": variants_payload,
        "print_areas": print_areas,
        "tags": tags,
    }


def build_printify_publish_payload(template: ProductTemplate) -> Dict[str, Any]:
    return {
        "title": template.publish_title,
        "description": template.publish_description,
        "images": template.publish_images,
        "variants": template.publish_variants,
        "tags": template.publish_tags,
    }


# -----------------------------
# Sync flows
# -----------------------------


def upload_assets_to_printify(printify: PrintifyClient, state: Dict[str, Any], artwork: Artwork, template: ProductTemplate, prepared_assets: List[PreparedArtwork], state_path: pathlib.Path) -> Dict[str, Dict[str, Any]]:
    uploaded: Dict[str, Dict[str, Any]] = {}
    uploads_state = state.setdefault("uploads", {})

    for asset in prepared_assets:
        cache_key = f"{artwork.slug}:{template.key}:{asset.placement.placement_name}:{asset.export_path.stat().st_size}"
        cached = uploads_state.get(cache_key)
        if cached and cached.get("id"):
            uploaded[asset.placement.placement_name] = cached
            continue

        try:
            response = printify.upload_image(file_path=asset.export_path)
            uploads_state[cache_key] = response
            uploaded[asset.placement.placement_name] = response
            save_json_atomic(state_path, state)
        except DryRunMutationSkipped:
            uploaded[asset.placement.placement_name] = {"id": f"dry-run-{artwork.slug}-{template.key}-{asset.placement.placement_name}"}

    return uploaded


def create_in_printify(printify: PrintifyClient, shop_id: int, artwork: Artwork, template: ProductTemplate, variant_rows: List[Dict[str, Any]], upload_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    payload = build_printify_product_payload(artwork, template, variant_rows, upload_map)
    try:
        created = printify.create_product(shop_id, payload)
    except DryRunMutationSkipped:
        return {"status": "dry-run", "payload_preview": payload}

    product_id = str(created.get("id") or created.get("data", {}).get("id") or "")
    result: Dict[str, Any] = {"printify_product_id": product_id, "created": created}
    if template.publish_after_create and product_id:
        try:
            result["published"] = printify.publish_product(shop_id, product_id, build_printify_publish_payload(template))
        except DryRunMutationSkipped:
            result["published"] = {"status": "dry-run"}
    return result


def process_artwork(*, printify: PrintifyClient, shopify: Optional[ShopifyClient], shop_id: Optional[int], artwork: Artwork, templates: List[ProductTemplate], state: Dict[str, Any], force: bool, export_dir: pathlib.Path, state_path: pathlib.Path) -> None:
    processed = state.setdefault("processed", {})
    if artwork.slug in processed and not force:
        logger.info("Skipping already processed artwork: %s", artwork.slug)
        return

    logger.info("Processing artwork: %s", artwork.src_path.name)
    processed.setdefault(artwork.slug, {"products": []})

    for template in templates:
        try:
            catalog_variants = printify.list_variants(template.printify_blueprint_id, template.printify_print_provider_id)
            variant_rows = choose_variants_from_catalog(catalog_variants, template)
            if not variant_rows:
                processed[artwork.slug]["products"].append({"template": template.key, "result": {"status": "no_matching_variants"}})
                save_json_atomic(state_path, state)
                continue

            prepared_assets = [prepare_artwork_export(artwork, template, placement, export_dir) for placement in template.placements]
            upload_map = upload_assets_to_printify(printify, state, artwork, template, prepared_assets, state_path)

            result: Dict[str, Any] = {}
            result["printify"] = create_in_printify(printify, shop_id, artwork, template, variant_rows, upload_map) if (template.push_via_printify and shop_id is not None) else {"status": "prepared_only"}
            if template.publish_to_shopify and shopify is not None:
                result["shopify"] = create_in_shopify_only(shopify, artwork, template, variant_rows)

            processed[artwork.slug]["products"].append({
                "template": template.key,
                "blueprint_id": template.printify_blueprint_id,
                "print_provider_id": template.printify_print_provider_id,
                "result": result,
            })
        except Exception as exc:
            logger.exception("Sync failed for artwork=%s template=%s", artwork.slug, template.key)
            processed[artwork.slug]["products"].append({"template": template.key, "result": {"error": str(exc)}})
        save_json_atomic(state_path, state)
        time.sleep(0.25)


def resolve_shop_id(printify: PrintifyClient, env_value: str) -> Optional[int]:
    if env_value:
        return int(env_value)
    if printify.dry_run:
        logger.info("[dry-run] PRINTIFY_SHOP_ID not set; skipping remote shop discovery")
        return None
    shops = printify.list_shops()
    return int(shops[0]["id"]) if shops else None


def audit_printify_integration(printify: PrintifyClient, templates: List[ProductTemplate], shop_id: Optional[int]) -> None:
    if printify.dry_run:
        logger.info("[dry-run] Skipping remote Printify audit")
        return

    shops = printify.list_shops()
    logger.info("Printify audit: shops discovered=%s", len(shops))
    if shop_id and not any(str(s.get("id")) == str(shop_id) for s in shops):
        logger.warning("Configured PRINTIFY_SHOP_ID=%s not found in shop list", shop_id)

    blueprints = {int(b["id"]): b for b in printify.list_blueprints() if "id" in b}
    for template in templates:
        if template.printify_blueprint_id not in blueprints:
            logger.warning("Template %s uses missing blueprint_id=%s", template.key, template.printify_blueprint_id)
            continue
        providers = printify.list_print_providers(template.printify_blueprint_id)
        if not any(int(p["id"]) == template.printify_print_provider_id for p in providers if "id" in p):
            logger.warning("Template %s provider_id=%s not available for blueprint=%s", template.key, template.printify_print_provider_id, template.printify_blueprint_id)
            continue
        variants = printify.list_variants(template.printify_blueprint_id, template.printify_print_provider_id)
        if not variants:
            logger.warning("Template %s returned zero variants from Printify catalog", template.key)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="InkVibeAuto Printify + Shopify sync pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Prepare and log payloads without mutating remote APIs")
    parser.add_argument("--force", action="store_true", help="Reprocess artworks already present in state.json")
    parser.add_argument("--templates", default=str(TEMPLATES_CONFIG), help="Path to product_templates.json")
    parser.add_argument("--image-dir", default=str(IMAGE_DIR), help="Image source directory")
    parser.add_argument("--export-dir", default=str(EXPORT_DIR), help="Export output directory")
    parser.add_argument("--state-path", default=str(STATE_PATH), help="State JSON path")
    parser.add_argument("--log-level", default="INFO", help="Logging level: DEBUG/INFO/WARNING/ERROR")
    parser.add_argument("--skip-audit", action="store_true", help="Skip Printify catalog/shop preflight audit")
    parser.add_argument("--max-artworks", type=int, default=0, help="Limit number of discovered artworks (0 = no limit)")
    return parser.parse_args()


def run(config_path: pathlib.Path, *, dry_run: bool = False, force: bool = False, image_dir: pathlib.Path = IMAGE_DIR, export_dir: pathlib.Path = EXPORT_DIR, state_path: pathlib.Path = STATE_PATH, skip_audit: bool = False, max_artworks: int = 0) -> None:
    if not PRINTIFY_API_TOKEN:
        raise RuntimeError("Missing PRINTIFY_API_TOKEN")
    if not image_dir.exists():
        raise RuntimeError(f"Missing image directory: {image_dir}")

    templates = load_templates(config_path)
    artworks = discover_artworks(image_dir)
    if max_artworks > 0:
        artworks = artworks[:max_artworks]

    state = ensure_state_shape(load_json(state_path, {}))
    printify = PrintifyClient(PRINTIFY_API_TOKEN, dry_run=dry_run)
    shop_id = resolve_shop_id(printify, PRINTIFY_SHOP_ID)
    shopify = ShopifyClient(SHOPIFY_ADMIN_TOKEN, dry_run=dry_run) if SHOPIFY_ADMIN_TOKEN else None

    if not skip_audit:
        audit_printify_integration(printify, templates, shop_id)

    logger.info("Loaded %s template(s) and %s artwork file(s)", len(templates), len(artworks))
    for artwork in artworks:
        process_artwork(
            printify=printify,
            shopify=shopify,
            shop_id=shop_id,
            artwork=artwork,
            templates=templates,
            state=state,
            force=force,
            export_dir=export_dir,
            state_path=state_path,
        )

    save_json_atomic(state_path, state)
    logger.info("Done")


if __name__ == "__main__":
    args = parse_args()
    configure_logging(args.log_level)
    run(
        pathlib.Path(args.templates),
        dry_run=args.dry_run,
        force=args.force,
        image_dir=pathlib.Path(args.image_dir),
        export_dir=pathlib.Path(args.export_dir),
        state_path=pathlib.Path(args.state_path),
        skip_audit=args.skip_audit,
        max_artworks=args.max_artworks,
    )
