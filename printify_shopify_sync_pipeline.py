from __future__ import annotations

import argparse
import base64
import hashlib
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
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from PIL import Image, ImageOps
from r2_uploader import R2Config, build_r2_public_url, load_r2_config_from_env, upload_file_to_r2

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
PRINTIFY_DIRECT_UPLOAD_LIMIT_BYTES = 5 * 1024 * 1024

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


class NonRetryableRequestError(RuntimeError):
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
    publish_mockups: Optional[bool] = None
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


@dataclass
class ArtworkProcessingOptions:
    allow_upscale: bool = False
    upscale_method: str = "lanczos"
    skip_undersized: bool = False


@dataclass
class ArtworkResolution:
    image: Image.Image
    action: str
    upscaled: bool
    original_size: Tuple[int, int]
    final_size: Tuple[int, int]


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


def _normalize_title_tokens(value: str) -> List[str]:
    cleaned = re.sub(r"\.[a-zA-Z0-9]{2,5}$", "", value).strip()
    cleaned = re.sub(r"[_\-]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return [token for token in cleaned.split(" ") if token]


def filename_slug_to_title(value: str) -> str:
    tokens = _normalize_title_tokens(value)
    if not tokens:
        return "Untitled Design"

    normalized_tokens: List[str] = []
    for token in tokens:
        if re.fullmatch(r"\d{8,}", token):
            continue
        if re.fullmatch(r"v\d+", token.lower()):
            continue
        normalized_tokens.append(token)

    chosen = normalized_tokens or tokens
    return " ".join(chosen).title().strip()


def looks_like_slug(value: str) -> bool:
    lowered = value.strip().lower()
    if not lowered:
        return True
    if "_" in lowered or "-" in lowered:
        return True
    if re.search(r"\d{5,}", lowered):
        return True
    return False


def choose_artwork_display_title(artwork: Artwork) -> str:
    derived = filename_slug_to_title(artwork.src_path.stem or artwork.slug)
    if not artwork.title.strip() or looks_like_slug(artwork.title):
        return derived
    return artwork.title.strip()


def build_generic_description_html(artwork_title: str) -> str:
    return (
        f"<p><strong>{artwork_title}</strong> adds an easy style upgrade to your everyday wardrobe.</p>"
        f"<p>This print-on-demand apparel design is made for casual wear, gifting, and year-round outfits. "
        "Pair it with your favorite layers for a clean, wearable look.</p>"
        "<ul>"
        "<li>Comfort-focused fit for daily wear</li>"
        "<li>High-quality print designed to stay vibrant</li>"
        "<li>Great for gifting or building a themed collection</li>"
        "</ul>"
    )


def render_product_title(template: ProductTemplate, artwork: Artwork) -> str:
    display_title = choose_artwork_display_title(artwork)
    return template.title_pattern.format(artwork_title=display_title, clean_artwork_title=display_title).strip()


def render_product_description(template: ProductTemplate, artwork: Artwork) -> str:
    display_title = choose_artwork_display_title(artwork)
    generated = build_generic_description_html(display_title)
    pattern = (template.description_pattern or "").strip()

    if not pattern or pattern in {"{artwork_title}", "<p>{artwork_title}</p>"}:
        return generated

    return template.description_pattern.format(
        artwork_title=display_title,
        clean_artwork_title=display_title,
        generated_description=generated,
    ).strip()


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


def normalize_printify_price(value: Any) -> int:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Invalid Printify price value: {value!r}")

    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"Printify price must be non-negative: {value!r}")
        return value

    decimal_value: Decimal
    if isinstance(value, Decimal):
        decimal_value = value
    elif isinstance(value, float):
        decimal_value = Decimal(str(value))
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise ValueError("Printify price cannot be empty")
        try:
            decimal_value = Decimal(stripped)
        except InvalidOperation as exc:
            raise ValueError(f"Invalid Printify price string: {value!r}") from exc
    else:
        raise ValueError(f"Unsupported Printify price type: {type(value).__name__}")

    if decimal_value < 0:
        raise ValueError(f"Printify price must be non-negative: {value!r}")

    minor_units = decimal_value * Decimal("100")
    normalized = minor_units.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(normalized)


def file_fingerprint(path: pathlib.Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


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

                if response.status_code == 400:
                    logger.error("Validation failed for %s %s: %s", method.upper(), path, body)
                    raise NonRetryableRequestError(f"HTTP 400 for {method.upper()} {path}: {body}")

                raise RuntimeError(f"HTTP {response.status_code} for {method.upper()} {path}: {body}")
            except (DryRunMutationSkipped, NonRetryableRequestError):
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
            if file_path.stat().st_size > PRINTIFY_DIRECT_UPLOAD_LIMIT_BYTES:
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
    title = render_product_title(template, artwork)
    description_html = render_product_description(template, artwork)
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

        title = filename_slug_to_title(path.stem)
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


def _upscale_filter(method: str) -> int:
    return Image.NEAREST if method == "nearest" else Image.LANCZOS


def resolve_artwork_for_placement(
    artwork: Artwork,
    placement: PlacementRequirement,
    *,
    allow_upscale: bool,
    upscale_method: str,
    skip_undersized: bool,
) -> ArtworkResolution:
    with Image.open(artwork.src_path) as opened:
        image = ImageOps.exif_transpose(opened).convert("RGBA")
    original_size = (image.width, image.height)
    required_size = (placement.width_px, placement.height_px)
    too_small = image.width < placement.width_px or image.height < placement.height_px

    logger.info(
        "Artwork %s placement=%s original=%sx%s required=%sx%s",
        artwork.src_path.name,
        placement.placement_name,
        original_size[0],
        original_size[1],
        required_size[0],
        required_size[1],
    )

    if too_small and not allow_upscale:
        if skip_undersized:
            logger.warning(
                "Skipping undersized artwork %s for placement %s: %sx%s < %sx%s (action=skip)",
                artwork.src_path.name,
                placement.placement_name,
                original_size[0],
                original_size[1],
                required_size[0],
                required_size[1],
            )
            return ArtworkResolution(
                image=image,
                action="skip",
                upscaled=False,
                original_size=original_size,
                final_size=original_size,
            )
        logger.error(
            "Undersized artwork action=fail artwork=%s placement=%s original=%sx%s required=%sx%s",
            artwork.src_path.name,
            placement.placement_name,
            original_size[0],
            original_size[1],
            required_size[0],
            required_size[1],
        )
        raise ValueError(
            f"image too small ({original_size[0]}x{original_size[1]}) for "
            f"placement {placement.placement_name} ({required_size[0]}x{required_size[1]})"
        )

    scale = max(placement.width_px / image.width, placement.height_px / image.height)
    upscaled = scale > 1
    if upscaled and allow_upscale:
        resized = image.resize((math.ceil(image.width * scale), math.ceil(image.height * scale)), _upscale_filter(upscale_method))
        left = max(0, (resized.width - placement.width_px) // 2)
        top = max(0, (resized.height - placement.height_px) // 2)
        final = resized.crop((left, top, left + placement.width_px, top + placement.height_px))
        logger.info(
            "Upscaled artwork from %sx%s to %sx%s for placement %s",
            original_size[0],
            original_size[1],
            placement.width_px,
            placement.height_px,
            placement.placement_name,
        )
        return ArtworkResolution(
            image=final,
            action="upscale",
            upscaled=True,
            original_size=original_size,
            final_size=(final.width, final.height),
        )

    resized = image.resize((math.ceil(image.width * scale), math.ceil(image.height * scale)), Image.LANCZOS)
    left = max(0, (resized.width - placement.width_px) // 2)
    top = max(0, (resized.height - placement.height_px) // 2)
    final = resized.crop((left, top, left + placement.width_px, top + placement.height_px))
    logger.info("Artwork resolution action=fit placement=%s final=%sx%s", placement.placement_name, final.width, final.height)
    return ArtworkResolution(
        image=final,
        action="fit",
        upscaled=False,
        original_size=original_size,
        final_size=(final.width, final.height),
    )


def prepare_artwork_export(
    artwork: Artwork,
    template: ProductTemplate,
    placement: PlacementRequirement,
    export_dir: pathlib.Path,
    options: ArtworkProcessingOptions,
) -> Optional[PreparedArtwork]:
    export_path = export_dir / template.key / f"{artwork.slug}-{placement.placement_name}.png"
    export_path.parent.mkdir(parents=True, exist_ok=True)

    resolution = resolve_artwork_for_placement(
        artwork,
        placement,
        allow_upscale=options.allow_upscale or placement.allow_upscale,
        upscale_method=options.upscale_method,
        skip_undersized=options.skip_undersized,
    )
    if resolution.action == "skip":
        return None

    resolution.image.save(export_path, "PNG")
    resolution.image.close()
    return PreparedArtwork(
        artwork=artwork,
        template=template,
        placement=placement,
        export_path=export_path,
        width_px=placement.width_px,
        height_px=placement.height_px,
    )


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
                publish_mockups=bool(row["publish_mockups"]) if "publish_mockups" in row else None,
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
    title = render_product_title(template, artwork)
    description_html = render_product_description(template, artwork)
    tags = list(dict.fromkeys(DEFAULT_TAGS + artwork.tags + template.tags))

    variants_payload: List[Dict[str, Any]] = []
    enabled_variant_ids: List[int] = []
    for variant in variant_rows:
        variant_id = int(variant["id"])
        enabled_variant_ids.append(variant_id)
        raw_price = variant.get("price")
        if raw_price is None:
            raw_price = variant.get("cost")
        if raw_price is None:
            raw_price = variant.get("price_cents")
        if raw_price is None:
            raw_price = template.default_price

        normalized_price = normalize_printify_price(raw_price)
        variants_payload.append({"id": variant_id, "price": normalized_price, "is_enabled": True})

    if variants_payload:
        logger.debug("Printify variants sample (normalized): %s", variants_payload[0])

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
    # Printify publish image controls can govern storefront mockups/images for supported channels.
    # TODO: Add provider/channel-specific mockup selection support if Printify API exposes stable controls.
    images_flag = template.publish_images if template.publish_mockups is None else bool(template.publish_mockups)
    return {
        "title": template.publish_title,
        "description": template.publish_description,
        "images": images_flag,
        "variants": template.publish_variants,
        "tags": template.publish_tags,
    }


# -----------------------------
# Sync flows
# -----------------------------




def log_upload_result(placement_name: str, metadata: Dict[str, Any]) -> None:
    logger.info(
        "Upload successful placement=%s strategy=%s printify_image_id=%s r2_public_url=%s",
        placement_name,
        metadata.get("upload_strategy", "unknown"),
        metadata.get("id", ""),
        metadata.get("r2_public_url", "n/a"),
    )


def summarize_upload_strategy(upload_map: Dict[str, Dict[str, Any]]) -> str:
    strategies = sorted({v.get("upload_strategy", "unknown") for v in upload_map.values()})
    return "+".join(strategies) if strategies else "none"


def log_template_summary(*, artwork_slug: str, template_key: str, success: bool, result: Dict[str, Any], upload_map: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
    printify_result = result.get("printify", {}) if isinstance(result, dict) else {}
    product_id = printify_result.get("printify_product_id") or "n/a"
    upload_strategy = summarize_upload_strategy(upload_map or {})
    status = "success" if success else "failure"
    logger.info(
        "Template summary artwork=%s template=%s status=%s product_id=%s upload_strategy=%s",
        artwork_slug,
        template_key,
        status,
        product_id,
        upload_strategy,
    )

def choose_upload_strategy(file_size: int, requested_strategy: str, r2_config: Optional[R2Config]) -> str:
    if requested_strategy == "direct":
        return "direct"
    if requested_strategy == "r2_url":
        if r2_config is None:
            raise RuntimeError("Upload strategy 'r2_url' requires R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET, and R2_PUBLIC_BASE_URL")
        return "r2_url"

    if file_size <= PRINTIFY_DIRECT_UPLOAD_LIMIT_BYTES:
        return "direct"
    if r2_config is not None:
        return "r2_url"
    raise RuntimeError(
        "Large exported asset detected (>5MB) but Cloudflare R2 is not configured. "
        "Configure R2_* env vars or run with --upload-strategy direct to force direct uploads."
    )


def upload_assets_to_printify(
    printify: PrintifyClient,
    state: Dict[str, Any],
    artwork: Artwork,
    template: ProductTemplate,
    prepared_assets: List[PreparedArtwork],
    state_path: pathlib.Path,
    upload_strategy: str,
    r2_config: Optional[R2Config],
) -> Dict[str, Dict[str, Any]]:
    uploaded: Dict[str, Dict[str, Any]] = {}
    uploads_state = state.setdefault("uploads", {})

    for asset in prepared_assets:
        file_size = asset.export_path.stat().st_size
        fingerprint = file_fingerprint(asset.export_path)
        strategy_used = choose_upload_strategy(file_size, upload_strategy, r2_config)
        object_key = f"inkvibe/{slugify(artwork.slug)}/{slugify(template.key)}/{slugify(asset.placement.placement_name)}-{fingerprint[:12]}{asset.export_path.suffix.lower()}"
        cache_key = f"{artwork.slug}:{template.key}:{asset.placement.placement_name}:{fingerprint}:{strategy_used}"

        cached = uploads_state.get(cache_key)
        if cached and cached.get("id"):
            uploaded[asset.placement.placement_name] = cached
            log_upload_result(asset.placement.placement_name, cached)
            continue

        try:
            if strategy_used == "direct":
                response = printify.upload_image(file_path=asset.export_path)
                metadata = {
                    "id": response.get("id"),
                    "upload_strategy": "direct",
                    "source_fingerprint": fingerprint,
                    "file_size": file_size,
                    "source_file": str(asset.export_path),
                }
            else:
                if r2_config is None:
                    raise RuntimeError("R2 config is required for r2_url upload strategy")
                public_url = build_r2_public_url(r2_config.public_base_url, object_key)
                if not printify.dry_run:
                    public_url = upload_file_to_r2(asset.export_path, object_key, r2_config)
                response = printify.upload_image(image_url=public_url)
                metadata = {
                    "id": response.get("id"),
                    "upload_strategy": "r2_url",
                    "r2_object_key": object_key,
                    "r2_public_url": public_url,
                    "source_fingerprint": fingerprint,
                    "file_size": file_size,
                    "source_file": str(asset.export_path),
                }

            uploads_state[cache_key] = metadata
            uploaded[asset.placement.placement_name] = metadata
            log_upload_result(asset.placement.placement_name, metadata)
            save_json_atomic(state_path, state)
            logger.info("State persisted after upload state_path=%s", state_path)
        except DryRunMutationSkipped:
            dry = {
                "id": f"dry-run-{artwork.slug}-{template.key}-{asset.placement.placement_name}",
                "upload_strategy": strategy_used,
                "source_fingerprint": fingerprint,
            }
            if strategy_used == "r2_url":
                dry["r2_object_key"] = object_key
                dry["r2_public_url"] = build_r2_public_url(r2_config.public_base_url if r2_config else "", object_key)
            uploaded[asset.placement.placement_name] = dry
            log_upload_result(asset.placement.placement_name, dry)

    return uploaded


def create_in_printify(printify: PrintifyClient, shop_id: int, artwork: Artwork, template: ProductTemplate, variant_rows: List[Dict[str, Any]], upload_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    payload = build_printify_product_payload(artwork, template, variant_rows, upload_map)
    logger.info("Mockup/image publish behavior template=%s publish_images=%s publish_mockups_override=%s", template.key, template.publish_images, template.publish_mockups)
    try:
        created = printify.create_product(shop_id, payload)
    except DryRunMutationSkipped:
        return {"status": "dry-run", "payload_preview": payload}

    product_id = str(created.get("id") or created.get("data", {}).get("id") or "")
    result: Dict[str, Any] = {"printify_product_id": product_id, "created": created}
    logger.info("Printify product created product_id=%s title=%s enabled_variants=%s", product_id, payload.get("title", ""), len(payload.get("variants", [])))
    if template.publish_after_create and product_id:
        try:
            result["published"] = printify.publish_product(shop_id, product_id, build_printify_publish_payload(template))
            logger.info("Printify publish completed product_id=%s result=%s", product_id, result["published"])
        except DryRunMutationSkipped:
            result["published"] = {"status": "dry-run"}
    return result


def process_artwork(*, printify: PrintifyClient, shopify: Optional[ShopifyClient], shop_id: Optional[int], artwork: Artwork, templates: List[ProductTemplate], state: Dict[str, Any], force: bool, export_dir: pathlib.Path, state_path: pathlib.Path, artwork_options: ArtworkProcessingOptions, upload_strategy: str, r2_config: Optional[R2Config]) -> None:
    processed = state.setdefault("processed", {})
    existing = processed.get(artwork.slug, {})
    if existing.get("completed") and not force:
        logger.info("Skipping already processed artwork: %s", artwork.slug)
        return

    logger.info("Processing artwork: %s", artwork.src_path.name)
    record: Dict[str, Any] = {
        "products": [],
        "completed": False,
        "last_run": datetime.now(timezone.utc).isoformat(),
    }

    all_templates_successful = True
    for template in templates:
        upload_map: Dict[str, Dict[str, Any]] = {}
        try:
            catalog_variants = printify.list_variants(template.printify_blueprint_id, template.printify_print_provider_id)
            variant_rows = choose_variants_from_catalog(catalog_variants, template)
            if not variant_rows:
                all_templates_successful = False
                result = {"status": "no_matching_variants"}
                record["products"].append({"template": template.key, "result": result})
                log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": result}, upload_map=upload_map)
                continue

            prepared_assets: List[PreparedArtwork] = []
            skipped_placements: List[str] = []
            for placement in template.placements:
                prepared = prepare_artwork_export(artwork, template, placement, export_dir, artwork_options)
                if prepared is None:
                    skipped_placements.append(placement.placement_name)
                    continue
                prepared_assets.append(prepared)

            if skipped_placements:
                all_templates_successful = False
                result = {"status": "skipped_undersized", "placements": skipped_placements}
                record["products"].append({
                    "template": template.key,
                    "result": result,
                })
                log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": result}, upload_map=upload_map)
                continue

            upload_map = upload_assets_to_printify(printify, state, artwork, template, prepared_assets, state_path, upload_strategy, r2_config)

            result: Dict[str, Any] = {}
            result["printify"] = create_in_printify(printify, shop_id, artwork, template, variant_rows, upload_map) if (template.push_via_printify and shop_id is not None) else {"status": "prepared_only"}
            if template.publish_to_shopify and shopify is not None:
                result["shopify"] = create_in_shopify_only(shopify, artwork, template, variant_rows)

            record["products"].append({
                "template": template.key,
                "blueprint_id": template.printify_blueprint_id,
                "print_provider_id": template.printify_print_provider_id,
                "result": result,
            })
            log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=True, result=result, upload_map=upload_map)
        except Exception as exc:
            all_templates_successful = False
            logger.exception("Sync failed for artwork=%s template=%s", artwork.slug, template.key)
            error_result = {"error": str(exc)}
            record["products"].append({"template": template.key, "result": error_result})
            log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": error_result}, upload_map=upload_map)
        save_json_atomic(state_path, state)
        logger.info("State persisted after template processing state_path=%s", state_path)
        time.sleep(0.25)

    record["completed"] = all_templates_successful
    processed[artwork.slug] = record
    save_json_atomic(state_path, state)
    logger.info("State persisted after artwork completion state_path=%s", state_path)


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
    parser.add_argument("--allow-upscale", action="store_true", help="Allow undersized artwork to be upscaled to placement dimensions")
    parser.add_argument("--upscale-method", choices=["nearest", "lanczos"], default="lanczos", help="Resampling method used when upscaling")
    parser.add_argument("--skip-undersized", action="store_true", help="Skip undersized artwork/template placements instead of failing")
    parser.add_argument("--templates", default=str(TEMPLATES_CONFIG), help="Path to product_templates.json")
    parser.add_argument("--image-dir", default=str(IMAGE_DIR), help="Image source directory")
    parser.add_argument("--export-dir", default=str(EXPORT_DIR), help="Export output directory")
    parser.add_argument("--state-path", default=str(STATE_PATH), help="State JSON path")
    parser.add_argument("--log-level", default="INFO", help="Logging level: DEBUG/INFO/WARNING/ERROR")
    parser.add_argument("--skip-audit", action="store_true", help="Skip Printify catalog/shop preflight audit")
    parser.add_argument("--max-artworks", type=int, default=0, help="Limit number of discovered artworks (0 = no limit)")
    parser.add_argument("--upload-strategy", choices=["auto", "direct", "r2_url"], default="auto", help="Asset upload strategy: auto (default), direct, or r2_url")
    return parser.parse_args()


def run(config_path: pathlib.Path, *, dry_run: bool = False, force: bool = False, allow_upscale: bool = False, upscale_method: str = "lanczos", skip_undersized: bool = False, image_dir: pathlib.Path = IMAGE_DIR, export_dir: pathlib.Path = EXPORT_DIR, state_path: pathlib.Path = STATE_PATH, skip_audit: bool = False, max_artworks: int = 0, upload_strategy: str = "auto") -> None:
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
    r2_config = load_r2_config_from_env()

    if not skip_audit:
        audit_printify_integration(printify, templates, shop_id)

    logger.info("Loaded %s template(s) and %s artwork file(s)", len(templates), len(artworks))
    artwork_options = ArtworkProcessingOptions(allow_upscale=allow_upscale, upscale_method=upscale_method, skip_undersized=skip_undersized)
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
            artwork_options=artwork_options,
            upload_strategy=upload_strategy,
            r2_config=r2_config,
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
        allow_upscale=args.allow_upscale,
        upscale_method=args.upscale_method,
        skip_undersized=args.skip_undersized,
        image_dir=pathlib.Path(args.image_dir),
        export_dir=pathlib.Path(args.export_dir),
        state_path=pathlib.Path(args.state_path),
        skip_audit=args.skip_audit,
        max_artworks=args.max_artworks,
        upload_strategy=args.upload_strategy,
    )
