from __future__ import annotations

import argparse
import base64
import json
import logging
import math
import os
import pathlib
import re
import time
from dataclasses import dataclass, field
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
SHOPIFY_GRAPHQL_URL = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

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
USER_AGENT = os.getenv("PRINTIFY_USER_AGENT", "InkVibeAuto/1.0")
RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "5"))
RETRY_BACKOFF_SECONDS = float(os.getenv("RETRY_BACKOFF_SECONDS", "1.5"))


# -----------------------------
# Models
# -----------------------------

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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("inkvibeauto")


class DryRunMutationSkipped(RuntimeError):
    pass


def slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def load_json(path: pathlib.Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: pathlib.Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


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
                response = self.session.request(
                    method=method.upper(),
                    url=url,
                    params=params,
                    json=payload,
                    timeout=120,
                )

                if response.status_code in expected_statuses:
                    if response.content:
                        return response.json()
                    return {}

                if response.status_code in {429, 500, 502, 503, 504}:
                    retry_after = response.headers.get("Retry-After")
                    sleep_seconds = float(retry_after) if retry_after else RETRY_BACKOFF_SECONDS * attempt
                    logger.warning(
                        "Request %s %s failed with %s on attempt %s/%s; retrying in %.1fs",
                        method.upper(),
                        path,
                        response.status_code,
                        attempt,
                        RETRY_MAX_ATTEMPTS,
                        sleep_seconds,
                    )
                    time.sleep(sleep_seconds)
                    continue

                try:
                    body = response.json()
                except Exception:
                    body = response.text
                raise RuntimeError(f"HTTP {response.status_code} for {method.upper()} {path}: {body}")
            except DryRunMutationSkipped:
                raise
            except Exception as exc:
                last_exc = exc
                if attempt >= RETRY_MAX_ATTEMPTS:
                    break
                sleep_seconds = RETRY_BACKOFF_SECONDS * attempt
                logger.warning(
                    "Request exception on %s %s attempt %s/%s: %s; retrying in %.1fs",
                    method.upper(),
                    path,
                    attempt,
                    RETRY_MAX_ATTEMPTS,
                    exc,
                    sleep_seconds,
                )
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
        return self.get(
            f"/catalog/blueprints/{blueprint_id}/print_providers/{print_provider_id}/variants.json",
            **{"show-out-of-stock": 1 if show_out_of_stock else 0},
        )

    def upload_image(self, *, file_path: Optional[pathlib.Path] = None, image_url: Optional[str] = None) -> Dict[str, Any]:
        if file_path is None and image_url is None:
            raise ValueError("upload_image requires file_path or image_url")

        payload: Dict[str, Any] = {}
        if file_path is not None:
            payload["file_name"] = file_path.name
            if file_path.stat().st_size > 5 * 1024 * 1024:
                # TODO: For large local files, upload to your own durable URL first and call upload_image(image_url=...).
                logger.warning("Large file detected (>5MB): %s. URL uploads are recommended by Printify.", file_path.name)
            payload["contents"] = base64.b64encode(file_path.read_bytes()).decode("ascii")
        else:
            payload["file_name"] = pathlib.Path(image_url or "image.png").name or "image.png"
            payload["url"] = image_url

        return self.post("/uploads/images.json", payload)

    def create_product(self, shop_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.post(f"/shops/{shop_id}/products.json", payload)

    def update_product(self, shop_id: int, product_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.put(f"/shops/{shop_id}/products/{product_id}.json", payload)

    def publish_product(self, shop_id: int, product_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.post(f"/shops/{shop_id}/products/{product_id}/publish.json", payload)


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

        last_exc: Optional[Exception] = None
        path = f"/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                response = self.session.post(
                    f"{self.base_url}{path}",
                    json={"query": query, "variables": variables},
                    timeout=120,
                )
                if response.status_code in {429, 500, 502, 503, 504}:
                    retry_after = response.headers.get("Retry-After")
                    sleep_seconds = float(retry_after) if retry_after else RETRY_BACKOFF_SECONDS * attempt
                    logger.warning(
                        "Shopify GraphQL failed with %s on attempt %s/%s; retrying in %.1fs",
                        response.status_code,
                        attempt,
                        RETRY_MAX_ATTEMPTS,
                        sleep_seconds,
                    )
                    time.sleep(sleep_seconds)
                    continue

                response.raise_for_status()
                data = response.json()
                if data.get("errors"):
                    raise RuntimeError(f"Shopify GraphQL top-level errors: {data['errors']}")
                return data["data"]
            except DryRunMutationSkipped:
                raise
            except Exception as exc:
                last_exc = exc
                if attempt >= RETRY_MAX_ATTEMPTS:
                    break
                sleep_seconds = RETRY_BACKOFF_SECONDS * attempt
                logger.warning(
                    "Shopify GraphQL exception attempt %s/%s: %s; retrying in %.1fs",
                    attempt,
                    RETRY_MAX_ATTEMPTS,
                    exc,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Shopify GraphQL request failed")

    def staged_upload_image(self, file_path: pathlib.Path, mime_type: str = "image/png") -> str:
        mutation = """
        mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
          stagedUploadsCreate(input: $input) {
            stagedTargets {
              url
              resourceUrl
              parameters { name value }
            }
            userErrors { field message }
          }
        }
        """
        variables = {
            "input": [{
                "filename": file_path.name,
                "mimeType": mime_type,
                "resource": "IMAGE",
                "httpMethod": "POST",
                "fileSize": str(file_path.stat().st_size),
            }]
        }
        data = self.graphql(mutation, variables)["stagedUploadsCreate"]
        if data.get("userErrors"):
            raise RuntimeError(f"Shopify stagedUploadsCreate errors: {data['userErrors']}")
        target = data["stagedTargets"][0]

        files = {"file": (file_path.name, file_path.read_bytes(), mime_type)}
        form_data = {p["name"]: p["value"] for p in target["parameters"]}
        response = requests.post(target["url"], data=form_data, files=files, timeout=120)
        response.raise_for_status()
        return target["resourceUrl"]

    def create_product(self, title: str, description_html: str, vendor: str, tags: List[str], product_type: str, status: str) -> str:
        mutation = """
        mutation productCreate($product: ProductCreateInput!) {
          productCreate(product: $product) {
            product { id title }
            userErrors { field message }
          }
        }
        """
        variables = {
            "product": {
                "title": title,
                "descriptionHtml": description_html,
                "vendor": vendor,
                "productType": product_type,
                "tags": tags,
                "status": status,
                "productOptions": [
                    {"name": "Color", "values": [{"name": "Default"}]},
                    {"name": "Size", "values": [{"name": "Default"}]},
                ],
            }
        }
        data = self.graphql(mutation, variables)["productCreate"]
        if data.get("userErrors"):
            raise RuntimeError(f"Shopify productCreate errors: {data['userErrors']}")
        return data["product"]["id"]

    def product_set(self, identifier: Dict[str, Any], *, title: str, description_html: str, vendor: str, tags: List[str], product_type: str, status: str, product_options: List[Dict[str, Any]], variants: List[Dict[str, Any]]) -> Dict[str, Any]:
        mutation = """
        mutation productSet($identifier: ProductSetIdentifiers!, $input: ProductSetInput!) {
          productSet(identifier: $identifier, input: $input, synchronous: true) {
            product {
              id
              title
              variants(first: 100) {
                nodes { id title }
              }
            }
            userErrors { field message code }
          }
        }
        """
        variables = {
            "identifier": identifier,
            "input": {
                "title": title,
                "descriptionHtml": description_html,
                "vendor": vendor,
                "productType": product_type,
                "tags": tags,
                "status": status,
                "productOptions": product_options,
                "variants": variants,
            },
        }
        data = self.graphql(mutation, variables)["productSet"]
        if data.get("userErrors"):
            raise RuntimeError(f"Shopify productSet errors: {data['userErrors']}")
        return data["product"]


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
    export_dir.mkdir(parents=True, exist_ok=True)
    valid, reason = validate_artwork_for_placement(artwork, placement)
    if not valid:
        raise ValueError(reason)

    export_path = export_dir / template.key / f"{artwork.slug}-{placement.placement_name}.png"
    export_path.parent.mkdir(parents=True, exist_ok=True)

    with Image.open(artwork.src_path) as im:
        im = ImageOps.exif_transpose(im)
        im = im.convert("RGBA")

        target_w = placement.width_px
        target_h = placement.height_px

        scale = max(target_w / im.width, target_h / im.height)
        if scale > 1 and not placement.allow_upscale:
            raise ValueError("upscale required but disabled")

        resized = im.resize((math.ceil(im.width * scale), math.ceil(im.height * scale)), Image.LANCZOS)
        left = max(0, (resized.width - target_w) // 2)
        top = max(0, (resized.height - target_h) // 2)
        cropped = resized.crop((left, top, left + target_w, top + target_h))
        cropped.save(export_path, "PNG")

    return PreparedArtwork(
        artwork=artwork,
        template=template,
        placement=placement,
        export_path=export_path,
        width_px=target_w,
        height_px=target_h,
    )


# -----------------------------
# Product mapping
# -----------------------------

def load_templates(config_path: pathlib.Path) -> List[ProductTemplate]:
    raw = load_json(config_path, [])
    templates: List[ProductTemplate] = []
    for row in raw:
        templates.append(
            ProductTemplate(
                key=row["key"],
                printify_blueprint_id=row["printify_blueprint_id"],
                printify_print_provider_id=row["printify_print_provider_id"],
                title_pattern=row.get("title_pattern", "{artwork_title}"),
                description_pattern=row.get("description_pattern", "<p>{artwork_title}</p>"),
                enabled_colors=row.get("enabled_colors", []),
                enabled_sizes=row.get("enabled_sizes", []),
                tags=row.get("tags", []),
                shopify_product_type=row.get("shopify_product_type", "Apparel"),
                publish_to_shopify=row.get("publish_to_shopify", False),
                push_via_printify=row.get("push_via_printify", True),
                publish_after_create=row.get("publish_after_create", True),
                publish_title=row.get("publish_title", True),
                publish_description=row.get("publish_description", True),
                publish_images=row.get("publish_images", True),
                publish_variants=row.get("publish_variants", True),
                publish_tags=row.get("publish_tags", True),
                default_price=str(row.get("default_price", DEFAULT_PRICE_FALLBACK)),
                placements=[PlacementRequirement(**p) for p in row.get("placements", [])],
            )
        )
    return templates


def build_title(template: ProductTemplate, artwork: Artwork) -> str:
    return template.title_pattern.format(artwork_title=artwork.title).strip()


def build_description(template: ProductTemplate, artwork: Artwork) -> str:
    return template.description_pattern.format(artwork_title=artwork.title).strip()


def _variant_option_value(variant: Dict[str, Any], key: str) -> str:
    options = variant.get("options") or {}
    if isinstance(options, dict):
        return str(options.get(key, "")).strip()
    return str(variant.get(key, "")).strip()


def choose_variants_from_catalog(catalog_variants: List[Dict[str, Any]], template: ProductTemplate) -> List[Dict[str, Any]]:
    chosen: List[Dict[str, Any]] = []

    for variant in catalog_variants:
        color = _variant_option_value(variant, "color")
        size = _variant_option_value(variant, "size")
        is_available = variant.get("is_available", True)

        color_ok = not template.enabled_colors or color in template.enabled_colors
        size_ok = not template.enabled_sizes or size in template.enabled_sizes
        if color_ok and size_ok and is_available:
            chosen.append(variant)

    return chosen


def _placement_by_name(template: ProductTemplate, placement_name: str) -> PlacementRequirement:
    for placement in template.placements:
        if placement.placement_name == placement_name:
            return placement
    raise KeyError(f"Placement {placement_name!r} not found for template {template.key}")


def build_printify_product_payload(
    artwork: Artwork,
    template: ProductTemplate,
    variant_rows: List[Dict[str, Any]],
    upload_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    title = build_title(template, artwork)
    description_html = build_description(template, artwork)
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

        variants_payload.append({
            "id": variant_id,
            "price": price_value,
            "is_enabled": True,
        })

    print_areas: List[Dict[str, Any]] = []
    for placement in template.placements:
        upload_info = upload_map[placement.placement_name]
        print_areas.append({
            "variant_ids": enabled_variant_ids,
            "placeholders": [
                {
                    "position": placement.placement_name,
                    "images": [
                        {
                            "id": upload_info["id"],
                            "x": 0.5,
                            "y": 0.5,
                            "scale": 1,
                            "angle": 0,
                        }
                    ],
                }
            ],
        })
        # TODO: Verify the exact placeholder image transform for each blueprint/provider combination by comparing
        # TODO: against a manually created Printify product. Some products need per-placement scaling or extra fields.

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


# -----------------------------
# Sync flows
# -----------------------------

def upload_assets_to_printify(
    printify: PrintifyClient,
    state: Dict[str, Any],
    artwork: Artwork,
    template: ProductTemplate,
    prepared_assets: List[PreparedArtwork],
) -> Dict[str, Dict[str, Any]]:
    uploaded: Dict[str, Dict[str, Any]] = {}
    uploads_state = state.setdefault("uploads", {})

    for asset in prepared_assets:
        cache_key = f"{artwork.slug}:{template.key}:{asset.placement.placement_name}:{asset.export_path.stat().st_size}"
        cached = uploads_state.get(cache_key)
        if cached and cached.get("id"):
            logger.info("    Reusing cached Printify upload for %s", asset.placement.placement_name)
            uploaded[asset.placement.placement_name] = cached
            continue

        logger.info("    Uploading asset to Printify: %s", asset.export_path.name)
        try:
            response = printify.upload_image(file_path=asset.export_path)
            uploads_state[cache_key] = response
            uploaded[asset.placement.placement_name] = response
            save_json(STATE_PATH, state)
        except DryRunMutationSkipped:
            uploaded[asset.placement.placement_name] = {
                "id": f"dry-run-{artwork.slug}-{template.key}-{asset.placement.placement_name}",
                "file_name": asset.export_path.name,
            }

    return uploaded


def create_in_shopify_only(
    shopify: ShopifyClient,
    artwork: Artwork,
    template: ProductTemplate,
    variant_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    title = build_title(template, artwork)
    description_html = build_description(template, artwork)
    tags = list(dict.fromkeys(DEFAULT_TAGS + artwork.tags + template.tags))
    product_options, variants = build_shopify_product_options(variant_rows)

    handle = slugify(f"{artwork.slug}-{template.key}")
    try:
        product = shopify.product_set(
            identifier={"handle": handle},
            title=title,
            description_html=description_html,
            vendor=DEFAULT_VENDOR,
            tags=tags,
            product_type=template.shopify_product_type,
            status=DEFAULT_PRODUCT_STATUS,
            product_options=product_options,
            variants=variants,
        )
    except DryRunMutationSkipped:
        return {"status": "dry-run", "shopify_handle": handle}

    return {
        "shopify_product_id": product["id"],
        "shopify_variant_ids": [node["id"] for node in product.get("variants", {}).get("nodes", [])],
        "shopify_handle": handle,
    }


def create_in_printify(
    printify: PrintifyClient,
    shop_id: int,
    artwork: Artwork,
    template: ProductTemplate,
    variant_rows: List[Dict[str, Any]],
    upload_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    payload = build_printify_product_payload(
        artwork=artwork,
        template=template,
        variant_rows=variant_rows,
        upload_map=upload_map,
    )

    try:
        created = printify.create_product(shop_id, payload)
    except DryRunMutationSkipped:
        return {"status": "dry-run", "payload_preview": payload}

    product_id = str(created.get("id") or created.get("data", {}).get("id") or "")
    result: Dict[str, Any] = {
        "printify_product_id": product_id,
        "created": created,
    }

    if template.publish_after_create and product_id:
        publish_payload = build_printify_publish_payload(template)
        try:
            published = printify.publish_product(shop_id, product_id, publish_payload)
            result["published"] = published
        except DryRunMutationSkipped:
            result["published"] = {"status": "dry-run"}

    return result


# -----------------------------
# Runner
# -----------------------------

def process_artwork(
    *,
    printify: PrintifyClient,
    shopify: Optional[ShopifyClient],
    shop_id: Optional[int],
    artwork: Artwork,
    templates: List[ProductTemplate],
    state: Dict[str, Any],
    force: bool,
) -> None:
    processed = state.setdefault("processed", {})
    if artwork.slug in processed and not force:
        logger.info("Skipping already processed artwork: %s", artwork.slug)
        return

    logger.info("Processing artwork: %s", artwork.src_path.name)
    processed.setdefault(artwork.slug, {"products": []})

    for template in templates:
        logger.info("  Template: %s", template.key)

        try:
            catalog_variants = printify.list_variants(
                template.printify_blueprint_id,
                template.printify_print_provider_id,
            )
        except Exception as exc:
            logger.exception("    Failed to load Printify variants: %s", exc)
            processed[artwork.slug]["products"].append({
                "template": template.key,
                "result": {"error": str(exc)},
            })
            save_json(STATE_PATH, state)
            continue

        variant_rows = choose_variants_from_catalog(catalog_variants, template)
        if not variant_rows:
            logger.info("    No matching variants after color/size filters")
            processed[artwork.slug]["products"].append({
                "template": template.key,
                "result": {"status": "no_matching_variants"},
            })
            save_json(STATE_PATH, state)
            continue

        prepared_assets: List[PreparedArtwork] = []
        placement_failed = False
        for placement in template.placements:
            try:
                prepared_assets.append(prepare_artwork_export(artwork, template, placement, EXPORT_DIR))
            except Exception as exc:
                placement_failed = True
                logger.warning("    Placement failed (%s): %s", placement.placement_name, exc)
                break

        if placement_failed:
            processed[artwork.slug]["products"].append({
                "template": template.key,
                "result": {"status": "placement_failed"},
            })
            save_json(STATE_PATH, state)
            continue

        result: Dict[str, Any] = {}
        try:
            upload_map = upload_assets_to_printify(
                printify=printify,
                state=state,
                artwork=artwork,
                template=template,
                prepared_assets=prepared_assets,
            )

            if template.push_via_printify and shop_id is not None:
                result["printify"] = create_in_printify(
                    printify=printify,
                    shop_id=shop_id,
                    artwork=artwork,
                    template=template,
                    variant_rows=variant_rows,
                    upload_map=upload_map,
                )
            else:
                result["printify"] = {"status": "prepared_only"}

            if template.publish_to_shopify and shopify is not None:
                result["shopify"] = create_in_shopify_only(
                    shopify=shopify,
                    artwork=artwork,
                    template=template,
                    variant_rows=variant_rows,
                )
        except Exception as exc:
            logger.exception("    Sync failed: %s", exc)
            result = {"error": str(exc)}

        processed[artwork.slug]["products"].append({
            "template": template.key,
            "blueprint_id": template.printify_blueprint_id,
            "print_provider_id": template.printify_print_provider_id,
            "result": result,
        })
        save_json(STATE_PATH, state)
        time.sleep(0.25)


def resolve_shop_id(printify: PrintifyClient, env_value: str) -> Optional[int]:
    if env_value:
        return int(env_value)
    if printify.dry_run:
        logger.info("[dry-run] PRINTIFY_SHOP_ID not set; skipping remote shop discovery")
        return None
    shops = printify.list_shops()
    if not shops:
        return None
    return int(shops[0]["id"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="InkVibeAuto Printify + Shopify sync pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Prepare and log payloads without creating/updating remote records")
    parser.add_argument("--force", action="store_true", help="Reprocess artworks already present in state.json")
    parser.add_argument("--templates", default=str(TEMPLATES_CONFIG), help="Path to product_templates.json")
    return parser.parse_args()


def run(config_path: pathlib.Path, *, dry_run: bool = False, force: bool = False) -> None:
    if not PRINTIFY_API_TOKEN:
        raise RuntimeError("Missing PRINTIFY_API_TOKEN")
    if not IMAGE_DIR.exists():
        raise RuntimeError(f"Missing image directory: {IMAGE_DIR}")

    templates = load_templates(config_path)
    artworks = discover_artworks(IMAGE_DIR)
    state = ensure_state_shape(load_json(STATE_PATH, {}))

    printify = PrintifyClient(PRINTIFY_API_TOKEN, dry_run=dry_run)
    shop_id = resolve_shop_id(printify, PRINTIFY_SHOP_ID)
    shopify = ShopifyClient(SHOPIFY_ADMIN_TOKEN, dry_run=dry_run) if SHOPIFY_ADMIN_TOKEN else None

    if not shop_id:
        logger.warning("No PRINTIFY_SHOP_ID configured and no shops discovered. Printify product creation disabled.")
    if not SHOPIFY_ADMIN_TOKEN:
        logger.info("SHOPIFY_ADMIN_TOKEN missing. Shopify sync disabled.")

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
        )

    save_json(STATE_PATH, state)
    logger.info("Done")


if __name__ == "__main__":
    args = parse_args()
    run(pathlib.Path(args.templates), dry_run=args.dry_run, force=args.force)
