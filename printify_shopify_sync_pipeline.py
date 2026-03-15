from __future__ import annotations

import argparse
import base64
import csv
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

DEFAULT_TEMPLATE_PRICE = "29.99"
DEFAULT_MAX_ENABLED_VARIANTS = int(os.getenv("MAX_ENABLED_VARIANTS_SAFETY_LIMIT", "100"))


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


class CatalogCliUsageError(RuntimeError):
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
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TitleResolution:
    raw_title_source: str
    cleaned_display_title: str
    title_source: str
    quality_reason: str


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
    base_price: Optional[str] = None
    markup_type: str = "fixed"
    markup_value: str = "0"
    rounding_mode: str = "none"
    compare_at_price: Optional[str] = None
    seo_keywords: List[str] = field(default_factory=list)
    audience: Optional[str] = None
    product_type_label: Optional[str] = None
    style_keywords: List[str] = field(default_factory=list)
    max_enabled_variants: Optional[int] = None
    enabled_variant_option_filters: Dict[str, List[str]] = field(default_factory=dict)


@dataclass
class RunSummary:
    artworks_scanned: int = 0
    templates_processed: int = 0
    products_created: int = 0
    products_updated: int = 0
    products_rebuilt: int = 0
    products_skipped: int = 0
    failures: int = 0
    publish_attempts: int = 0
    publish_verified: int = 0
    verification_warnings: int = 0
    combinations_processed: int = 0
    combinations_success: int = 0
    combinations_failed: int = 0
    combinations_skipped: int = 0


@dataclass
class FailureReportRow:
    timestamp: str
    artwork_filename: str
    artwork_slug: str
    template_key: str
    action_attempted: str
    blueprint_id: int
    provider_id: int
    upload_strategy: str
    error_type: str
    error_message: str
    suggested_next_action: str


@dataclass
class RunReportRow:
    timestamp: str
    artwork_filename: str
    artwork_slug: str
    template_key: str
    status: str
    action: str
    blueprint_id: int
    provider_id: int
    upload_strategy: str
    product_id: str
    publish_attempted: bool
    publish_verified: bool
    rendered_title: str


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




def _semantic_product_tokens(value: str) -> set[str]:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    words = [w for w in normalized.split() if w]
    tokens: set[str] = set()
    for word in words:
        if word in {"tee", "tees", "tshirt", "tshirts", "shirt", "shirts", "t", "ts"}:
            tokens.add("shirt")
            continue
        if word in {"mug", "mugs", "cup", "cups"}:
            tokens.add("mug")
            continue
        tokens.add(word)
    return tokens


def title_semantically_includes_product_label(cleaned_title: str, product_label: str) -> bool:
    title_tokens = _semantic_product_tokens(cleaned_title)
    label_tokens = _semantic_product_tokens(product_label)
    if not title_tokens or not label_tokens:
        return False
    return label_tokens.issubset(title_tokens)


def _dedupe_rendered_title(title: str) -> str:
    title = re.sub(r"\s+", " ", title).strip()
    patterns = [
        (r"\b(t-?shirt)\s+\1\b", r"\1"),
        (r"\b(tee)\s+\1\b", r"\1"),
        (r"\b(mug)\s+\1\b", r"\1"),
    ]
    for pattern, repl in patterns:
        title = re.sub(pattern, repl, title, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", title).strip()

def _split_keywords(value: Any) -> List[str]:
    if isinstance(value, list):
        rows = value
    elif isinstance(value, str):
        rows = re.split(r"[,;]", value)
    else:
        return []
    return [str(row).strip() for row in rows if str(row).strip()]


def load_artwork_metadata(sidecar_path: pathlib.Path) -> Dict[str, Any]:
    if not sidecar_path.exists() or not sidecar_path.is_file():
        return {}

    try:
        payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Unable to parse metadata sidecar %s: %s", sidecar_path, exc)
        return {}

    if not isinstance(payload, dict):
        logger.warning("Ignoring non-object metadata sidecar %s", sidecar_path)
        return {}

    fields = {
        "title": "",
        "subtitle": "",
        "description": "",
        "tags": [],
        "seo_keywords": [],
        "audience": "",
        "style_keywords": [],
        "theme": "",
        "collection": "",
        "color_story": "",
        "occasion": "",
        "artist_note": "",
    }
    for field_name in fields:
        value = payload.get(field_name)
        if field_name in {"tags", "seo_keywords", "style_keywords"}:
            fields[field_name] = _split_keywords(value)
        elif isinstance(value, str):
            fields[field_name] = value.strip()
        elif value is None:
            fields[field_name] = "" if isinstance(fields[field_name], str) else []
        else:
            fields[field_name] = str(value).strip()
    return fields


def filename_title_quality_reason(value: str) -> str:
    lowered = value.strip().lower()
    if not lowered:
        return "empty"
    if re.fullmatch(r"[a-f0-9]{24,64}", lowered):
        return "hex_like"
    if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", lowered):
        return "uuid_like"
    tokens = _normalize_title_tokens(lowered)
    alpha_tokens = [token for token in tokens if re.search(r"[a-z]", token)]
    if re.search(r"[0-9]{8,}", lowered) and len(alpha_tokens) < 2:
        return "long_numeric"
    normalized = re.sub(r"[_\-]+", "", lowered)
    if len(normalized) >= 18 and re.fullmatch(r"[a-z0-9]+", normalized) and len(alpha_tokens) < 2:
        return "hashy_slug"
    return "ok"


def _best_metadata_phrase(artwork: Artwork, template: ProductTemplate) -> str:
    for key in ("theme", "style_keywords", "collection", "subtitle"):
        value = artwork.metadata.get(key)
        if isinstance(value, list) and value:
            return str(value[0]).strip().title()
        if isinstance(value, str) and value.strip():
            return value.strip().title()
    return ""


def resolve_artwork_title(template: ProductTemplate, artwork: Artwork) -> TitleResolution:
    metadata_title = str(artwork.metadata.get("title", "")).strip()
    if metadata_title:
        return TitleResolution(metadata_title, metadata_title, "metadata", "metadata_title")

    filename_stem = artwork.src_path.stem or artwork.slug
    quality_reason = filename_title_quality_reason(filename_stem)
    cleaned = filename_slug_to_title(filename_stem)
    if quality_reason == "ok":
        return TitleResolution(filename_stem, cleaned, "filename", "filename_clean")

    product_label = (template.product_type_label or template.shopify_product_type or "Product").strip()
    phrase = _best_metadata_phrase(artwork, template)
    fallback = f"{phrase} {product_label}".strip() if phrase else f"Signature {product_label}".strip()
    return TitleResolution(filename_stem, fallback, "fallback", quality_reason)


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
    metadata_title = str(artwork.metadata.get("title", "")).strip()
    if metadata_title:
        return metadata_title
    derived = filename_slug_to_title(artwork.src_path.stem or artwork.slug)
    if not artwork.title.strip() or looks_like_slug(artwork.title):
        return derived
    return artwork.title.strip()


def _extract_blueprint_brand(blueprint: Dict[str, Any]) -> str:
    for key in ("brand", "brand_name"):
        value = blueprint.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            nested = value.get("title") or value.get("name")
            if isinstance(nested, str) and nested.strip():
                return nested.strip()
    return ""


def _extract_blueprint_model(blueprint: Dict[str, Any]) -> str:
    for key in ("model", "model_name"):
        value = blueprint.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def search_blueprints(blueprints: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
    terms = [term for term in re.split(r"\s+", query.lower().strip()) if term]
    if not terms:
        return blueprints

    def _matches(blueprint: Dict[str, Any]) -> bool:
        haystack = " ".join(
            [
                str(blueprint.get("title", "")).lower(),
                str(_extract_blueprint_brand(blueprint)).lower(),
                str(_extract_blueprint_model(blueprint)).lower(),
                str(blueprint.get("description", "")).lower(),
            ]
        )
        return all(term in haystack for term in terms)

    return [blueprint for blueprint in blueprints if _matches(blueprint)]


def summarize_variant_options(variants: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    colors = sorted({c for c in (_variant_option_value(variant, "color") for variant in variants) if c})
    sizes = sorted({s for s in (_variant_option_value(variant, "size") for variant in variants) if s})
    placements: set[str] = set()

    for variant in variants:
        placeholders = variant.get("placeholders") or variant.get("placeholder_options") or []
        if isinstance(placeholders, list):
            for item in placeholders:
                if isinstance(item, dict):
                    pos = item.get("position") or item.get("name")
                    if isinstance(pos, str) and pos.strip():
                        placements.add(pos.strip())

    return {"colors": colors, "sizes": sizes, "placements": sorted(placements)}


def filter_providers(providers: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
    needle = query.lower().strip()
    if not needle:
        return providers
    return [provider for provider in providers if needle in str(provider.get("title", "")).lower()]


def score_provider_for_template(
    provider: Dict[str, Any],
    variants: List[Dict[str, Any]],
    template: Optional[ProductTemplate] = None,
) -> Dict[str, Any]:
    summary = summarize_variant_options(variants)
    available_colors = set(summary["colors"])
    available_sizes = set(summary["sizes"])
    available_placements = set(summary["placements"])

    wanted_colors = set(template.enabled_colors) if template else set()
    wanted_sizes = set(template.enabled_sizes) if template else set()
    wanted_placements = {placement.placement_name for placement in template.placements} if template else set()

    matching_color_count = len(available_colors & wanted_colors) if wanted_colors else len(available_colors)
    matching_size_count = len(available_sizes & wanted_sizes) if wanted_sizes else len(available_sizes)
    matching_variant_count = 0
    for variant in variants:
        color_ok = not wanted_colors or _variant_option_value(variant, "color") in wanted_colors
        size_ok = not wanted_sizes or _variant_option_value(variant, "size") in wanted_sizes
        placement_ok = (not wanted_placements) or (not available_placements) or bool(available_placements & wanted_placements)
        if color_ok and size_ok and placement_ok and variant.get("is_available", True):
            matching_variant_count += 1

    placement_match_count = len(available_placements & wanted_placements) if wanted_placements else len(available_placements)
    score = (matching_color_count * 4) + (matching_size_count * 4) + (matching_variant_count * 2) + (placement_match_count * 3)

    return {
        "provider_id": provider.get("id"),
        "provider_title": provider.get("title", ""),
        "score": score,
        "matching_color_count": matching_color_count,
        "matching_size_count": matching_size_count,
        "matching_variant_count": matching_variant_count,
        "placement_match_count": placement_match_count,
        "variant_count": len(variants),
        "colors": summary["colors"],
        "sizes": summary["sizes"],
        "placements": summary["placements"],
    }


def generate_template_snippet(
    *,
    key: str,
    blueprint_id: int,
    provider_id: int,
    variants: List[Dict[str, Any]],
) -> Dict[str, Any]:
    summary = summarize_variant_options(variants)
    placements = (summary["placements"] or ["front"])[:3]
    placement_rows = [
        {
            "placement_name": placement,
            "width_px": 4500,
            "height_px": 5400,
            "file_type": "png",
            "allow_upscale": False,
        }
        for placement in placements
    ]
    return {
        "key": key,
        "printify_blueprint_id": blueprint_id,
        "printify_print_provider_id": provider_id,
        "title_pattern": "{artwork_title}",
        "description_pattern": "<p>{artwork_title}</p>",
        "enabled_colors": summary["colors"][:12],
        "enabled_sizes": summary["sizes"][:8],
        "placements": placement_rows,
        "base_price": "24.99",
        "markup_type": "fixed",
        "markup_value": "5.00",
        "rounding_mode": "x_99",
        "seo_keywords": ["replace-me-keyword-1", "replace-me-keyword-2"],
        "audience": "",
        "product_type_label": "",
        "style_keywords": ["minimal", "giftable"],
    }


def generate_mug_template_snippet(
    *,
    key: str,
    blueprint_id: int,
    provider_id: int,
    variants: List[Dict[str, Any]],
) -> Dict[str, Any]:
    snippet = generate_template_snippet(
        key=key,
        blueprint_id=blueprint_id,
        provider_id=provider_id,
        variants=variants,
    )
    summary = summarize_variant_options(variants)
    preferred_colors = [color for color in summary["colors"] if color.lower() in {"white", "white glossy"}]
    preferred_sizes = [size for size in summary["sizes"] if "11" in size]
    if not preferred_colors:
        preferred_colors = summary["colors"][:1]
    if not preferred_sizes:
        preferred_sizes = summary["sizes"][:1]

    snippet.update(
        {
            "title_pattern": "{artwork_title} {product_type_label}",
            "description_pattern": "<p>{artwork_title} on a durable {product_type_label}.</p><p>Made for {audience}. Keywords: {seo_keywords}</p>",
            "enabled_colors": preferred_colors,
            "enabled_sizes": preferred_sizes,
            "enabled_variant_option_filters": {
                "color": preferred_colors,
                "size": preferred_sizes,
            },
            "max_enabled_variants": min(24, max(1, len(preferred_colors) * len(preferred_sizes))),
            "base_price": "12.00",
            "markup_type": "percent",
            "markup_value": "35",
            "rounding_mode": "whole_dollar",
            "seo_keywords": ["coffee mug", "desk accessory", "gift for coworkers"],
            "audience": "coffee drinkers and office gifting",
            "product_type_label": "11oz Mug",
            "style_keywords": ["clean", "playful"],
            "shopify_product_type": "Drinkware",
        }
    )
    return snippet


def template_blueprint_type_warning(*, template: ProductTemplate, blueprint_title: str) -> Optional[str]:
    def _family(text: str) -> Optional[str]:
        lowered = text.lower()
        if any(token in lowered for token in ("mug", "cup")):
            return "mug"
        if any(token in lowered for token in ("tee", "t-shirt", "shirt")):
            return "shirt"
        if any(token in lowered for token in ("hoodie", "sweatshirt", "crewneck")):
            return "sweatshirt"
        return None

    template_hint = " ".join(
        [
            template.key,
            template.product_type_label or "",
            template.shopify_product_type or "",
        ]
    )
    template_family = _family(template_hint)
    blueprint_family = _family(blueprint_title)
    if template_family and blueprint_family and template_family != blueprint_family:
        return (
            f"Template {template.key} looks like '{template_family}' but blueprint '{blueprint_title}' "
            f"looks like '{blueprint_family}'."
        )
    return None


def format_run_summary(summary: RunSummary) -> str:
    return (
        "Run summary "
        f"artworks_scanned={summary.artworks_scanned} "
        f"templates_processed={summary.templates_processed} "
        f"combinations_processed={summary.combinations_processed} "
        f"successes={summary.combinations_success} "
        f"failures={summary.combinations_failed} "
        f"skipped={summary.combinations_skipped} "
        f"products_created={summary.products_created} "
        f"products_updated={summary.products_updated} "
        f"products_rebuilt={summary.products_rebuilt} "
        f"products_skipped={summary.products_skipped} "
        f"template_failures={summary.failures} "
        f"publish_attempts={summary.publish_attempts} "
        f"publish_verified={summary.publish_verified} "
        f"verification_warnings={summary.verification_warnings}"
    )


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


def build_seo_context(template: ProductTemplate, artwork: Artwork) -> Dict[str, str]:
    title_info = resolve_artwork_title(template, artwork)
    display_title = title_info.cleaned_display_title
    metadata = artwork.metadata or {}
    merged_seo = [*template.seo_keywords, *_split_keywords(metadata.get("seo_keywords"))]
    merged_style = [*template.style_keywords, *_split_keywords(metadata.get("style_keywords"))]
    seo_keywords = ", ".join(list(dict.fromkeys(merged_seo))[:8])
    style_keywords = ", ".join(list(dict.fromkeys(merged_style))[:6])
    audience = str(metadata.get("audience") or template.audience or "").strip()
    product_type = (template.product_type_label or template.shopify_product_type or "Product").strip()
    subtitle = str(metadata.get("subtitle", "")).strip()
    theme = str(metadata.get("theme", "")).strip()
    collection = str(metadata.get("collection", "")).strip()
    color_story = str(metadata.get("color_story", "")).strip()
    occasion = str(metadata.get("occasion", "")).strip()
    artist_note = str(metadata.get("artist_note", "")).strip()
    return {
        "artwork_title": display_title,
        "clean_artwork_title": display_title,
        "seo_keywords": seo_keywords,
        "audience": audience,
        "product_type_label": product_type,
        "style_keywords": style_keywords,
        "subtitle": subtitle,
        "theme": theme,
        "collection": collection,
        "color_story": color_story,
        "occasion": occasion,
        "artist_note": artist_note,
        "title_source": title_info.title_source,
        "title_quality": title_info.quality_reason,
        "raw_title_source": title_info.raw_title_source,
    }


def render_product_title(template: ProductTemplate, artwork: Artwork) -> str:
    context = build_seo_context(template, artwork)
    product_label = context.get("product_type_label", "")
    if product_label and title_semantically_includes_product_label(context.get("artwork_title", ""), product_label):
        context = dict(context)
        context["product_type_label"] = ""
    rendered = template.title_pattern.format(**context).strip()
    return _dedupe_rendered_title(rendered)


def _render_listing_tags(template: ProductTemplate, artwork: Artwork) -> List[str]:
    metadata = artwork.metadata or {}
    merged = [
        *DEFAULT_TAGS,
        *artwork.tags,
        *template.tags,
        *_split_keywords(metadata.get("tags")),
        *_split_keywords(metadata.get("seo_keywords")),
    ]
    tags: List[str] = []
    seen: set[str] = set()
    for row in merged:
        cleaned = str(row).strip().lower()
        if not cleaned:
            continue
        cleaned = re.sub(r"\s+", " ", cleaned)
        if len(cleaned) > 32:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        tags.append(cleaned)
        if len(tags) >= 20:
            break
    return tags


def render_product_description(template: ProductTemplate, artwork: Artwork) -> str:
    context = build_seo_context(template, artwork)
    generated = build_generic_description_html(context["artwork_title"])
    pattern = (template.description_pattern or "").strip()
    metadata = artwork.metadata or {}
    metadata_description = str(metadata.get("description", "")).strip()
    if metadata_description:
        generated = f"<p><strong>{context['artwork_title']}</strong></p><p>{metadata_description}</p>"

    if not pattern or pattern in {"{artwork_title}", "<p>{artwork_title}</p>"}:
        audience_line = f"<p>Designed for {context['audience']} who love {context['style_keywords']}.</p>" if context["audience"] and context["style_keywords"] else ""
        theme_line = f"<p>Theme: {context['theme']}.</p>" if context["theme"] else ""
        occasion_line = f"<p>Perfect for {context['occasion']}.</p>" if context["occasion"] else ""
        collection_line = f"<p>Part of the {context['collection']} collection.</p>" if context["collection"] else ""
        return f"{generated}{audience_line}{theme_line}{occasion_line}{collection_line}".strip()

    return template.description_pattern.format(
        **context,
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

def _decimal_from_value(value: Any, *, default: str = "0") -> Decimal:
    if value is None:
        value = default
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return Decimal(default)
        return Decimal(stripped)
    raise ValueError(f"Unsupported decimal value type: {type(value).__name__}")


def apply_rounding_mode(price_minor: int, rounding_mode: str) -> int:
    if rounding_mode == "none":
        return max(0, price_minor)
    if rounding_mode == "whole_dollar":
        return max(0, int((Decimal(price_minor) / Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * 100))
    if rounding_mode == "x_99":
        dollars = int((price_minor + 99) // 100)
        return max(99, dollars * 100 - 1)
    raise ValueError(f"Unsupported rounding_mode '{rounding_mode}'")


def compute_sale_price_minor(template: ProductTemplate, variant: Dict[str, Any]) -> int:
    base_source = template.base_price if template.base_price is not None else variant.get("price")
    if base_source is None:
        base_source = variant.get("cost")
    if base_source is None:
        base_source = variant.get("price_cents")
    if base_source is None:
        base_source = template.default_price

    if isinstance(base_source, int):
        base_minor = base_source
    else:
        base_minor = normalize_printify_price(base_source)

    markup_value = _decimal_from_value(template.markup_value)
    if template.markup_type == "percent":
        final_minor = Decimal(base_minor) * (Decimal("1") + (markup_value / Decimal("100")))
        final_minor_int = int(final_minor.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    else:
        markup_minor = int((markup_value * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        final_minor_int = base_minor + markup_minor

    return apply_rounding_mode(final_minor_int, template.rounding_mode)


def compute_compare_at_price_minor(template: ProductTemplate, sale_price_minor: int) -> Optional[int]:
    if template.compare_at_price is None:
        return None
    compare_minor = normalize_printify_price(template.compare_at_price)
    return compare_minor if compare_minor > sale_price_minor else None



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


def derive_state_index(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    processed = state.get("processed", {}) if isinstance(state, dict) else {}
    index: Dict[str, Dict[str, Any]] = {}
    for artwork_record in processed.values():
        if not isinstance(artwork_record, dict):
            continue
        rows = artwork_record.get("products", [])
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            state_key = str(row.get("state_key") or "").strip()
            if state_key:
                index[state_key] = row
    return index


def list_state_keys(state: Dict[str, Any]) -> List[str]:
    return sorted(derive_state_index(state).keys())


def inspect_state_key(state: Dict[str, Any], state_key: str) -> Optional[Dict[str, Any]]:
    return derive_state_index(state).get(state_key)


def _row_status(row: Dict[str, Any]) -> str:
    result = row.get("result", {}) if isinstance(row.get("result"), dict) else {}
    if result.get("error"):
        return "failure"
    printify_result = result.get("printify", {}) if isinstance(result.get("printify"), dict) else {}
    if str(printify_result.get("status") or "").lower() == "skipped":
        return "skipped"
    status = str(result.get("status") or "").lower()
    if status.startswith("skipped") or status == "no_matching_variants":
        return "skipped"
    return "success"


def latest_rows_by_state_key(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return derive_state_index(state)


def is_state_key_successful(state: Dict[str, Any], state_key: str) -> bool:
    row = latest_rows_by_state_key(state).get(state_key)
    if not row:
        return False
    return _row_status(row) == "success"


def write_csv_report(path: pathlib.Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    headers = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


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
                        "Request %s %s failed with HTTP %s (%s/%s); retryable status, retrying in %.2fs",
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

                if 400 <= response.status_code < 500 and response.status_code != 429:
                    logger.error(
                        "Request %s %s failed with HTTP %s; non-retryable client error: %s",
                        method.upper(),
                        path,
                        response.status_code,
                        body,
                    )
                    raise NonRetryableRequestError(f"HTTP {response.status_code} for {method.upper()} {path}: {body}")

                logger.error(
                    "Request %s %s failed with HTTP %s after retry checks: %s",
                    method.upper(),
                    path,
                    response.status_code,
                    body,
                )
                raise RuntimeError(f"HTTP {response.status_code} for {method.upper()} {path}: {body}")
            except (DryRunMutationSkipped, NonRetryableRequestError):
                raise
            except Exception as exc:
                last_exc = exc
                if attempt >= RETRY_MAX_ATTEMPTS:
                    break
                sleep_seconds = _compute_backoff(attempt)
                logger.warning(
                    "Request exception for %s %s (%s/%s): %s; treated as transient transport error, retrying in %.2fs",
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

    def delete(self, path: str) -> Any:
        return self._request("DELETE", path, expected_statuses=(200, 202, 204), mutating=True)


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

    def update_product(self, shop_id: int, product_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.put(f"/shops/{shop_id}/products/{product_id}.json", payload)

    def delete_product(self, shop_id: int, product_id: str) -> Dict[str, Any]:
        return self.delete(f"/shops/{shop_id}/products/{product_id}.json")

    def publish_product(self, shop_id: int, product_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.post(f"/shops/{shop_id}/products/{product_id}/publish.json", payload)

    def get_product(self, shop_id: int, product_id: str) -> Dict[str, Any]:
        return self.get(f"/shops/{shop_id}/products/{product_id}.json")

    def list_products(self, shop_id: int) -> List[Dict[str, Any]]:
        return self.get(f"/shops/{shop_id}/products.json")


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
    tags = _render_listing_tags(template, artwork)
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

        metadata = load_artwork_metadata(path.with_suffix(".json"))
        title = str(metadata.get("title", "")).strip() or filename_slug_to_title(path.stem)
        artworks.append(
            Artwork(
                slug=slugify(path.stem),
                src_path=path,
                title=title,
                description_html=f"<p>{title}</p>",
                tags=[],
                image_width=width,
                image_height=height,
                metadata=metadata,
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
    if row.get("markup_type", "fixed") not in {"fixed", "percent"}:
        raise TemplateValidationError(f"Template[{index}] markup_type must be fixed|percent")
    if row.get("rounding_mode", "none") not in {"none", "whole_dollar", "x_99"}:
        raise TemplateValidationError(f"Template[{index}] rounding_mode must be none|whole_dollar|x_99")
    if row.get("max_enabled_variants") is not None and int(row.get("max_enabled_variants", 0)) <= 0:
        raise TemplateValidationError(f"Template[{index}] max_enabled_variants must be > 0 when provided")
    option_filters = row.get("enabled_variant_option_filters")
    if option_filters is not None and not isinstance(option_filters, dict):
        raise TemplateValidationError(f"Template[{index}] enabled_variant_option_filters must be an object")


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
                base_price=str(row["base_price"]) if "base_price" in row else None,
                markup_type=str(row.get("markup_type", "fixed")),
                markup_value=str(row.get("markup_value", "0")),
                rounding_mode=str(row.get("rounding_mode", "none")),
                compare_at_price=str(row["compare_at_price"]) if "compare_at_price" in row and row.get("compare_at_price") is not None else None,
                seo_keywords=[str(v) for v in row.get("seo_keywords", [])],
                audience=str(row.get("audience", "")).strip() or None,
                product_type_label=str(row.get("product_type_label", "")).strip() or None,
                style_keywords=[str(v) for v in row.get("style_keywords", [])],
                max_enabled_variants=int(row["max_enabled_variants"]) if row.get("max_enabled_variants") is not None else None,
                enabled_variant_option_filters={str(k): [str(v) for v in vals] for k, vals in (row.get("enabled_variant_option_filters") or {}).items()},
                placements=[PlacementRequirement(**p) for p in row.get("placements", [])],
            )
        )

    return templates


def choose_variants_from_catalog(catalog_variants: Any, template: ProductTemplate) -> List[Dict[str, Any]]:
    catalog_variants = normalize_catalog_variants_response(catalog_variants)
    chosen: List[Dict[str, Any]] = []
    option_filters = {str(k).lower().strip(): {str(v).strip() for v in values} for k, values in (template.enabled_variant_option_filters or {}).items() if values}
    for variant in catalog_variants:
        color = _variant_option_value(variant, "color")
        size = _variant_option_value(variant, "size")
        is_available = variant.get("is_available", True)
        option_filter_ok = True
        for option_key, allowed_values in option_filters.items():
            if _variant_option_value(variant, option_key) not in allowed_values:
                option_filter_ok = False
                break

        if (not template.enabled_colors or color in template.enabled_colors) and (not template.enabled_sizes or size in template.enabled_sizes) and option_filter_ok and is_available:
            chosen.append(variant)
    effective_limit = template.max_enabled_variants if template.max_enabled_variants is not None else DEFAULT_MAX_ENABLED_VARIANTS
    if effective_limit > 0 and len(chosen) > effective_limit:
        logger.warning(
            "Template %s matched %s variants; applying safety cap max_enabled_variants=%s before payload build",
            template.key,
            len(chosen),
            effective_limit,
        )
        chosen = chosen[:effective_limit]
    logger.info(
        "Variant selection template=%s selected=%s available=%s colors=%s sizes=%s option_filters=%s max_enabled_variants=%s",
        template.key,
        len(chosen),
        len(catalog_variants),
        template.enabled_colors or "*",
        template.enabled_sizes or "*",
        template.enabled_variant_option_filters or {},
        effective_limit,
    )
    return chosen


def enforce_variant_safety_limit(*, template: ProductTemplate, enabled_variant_count: int) -> None:
    effective_limit = template.max_enabled_variants if template.max_enabled_variants is not None else DEFAULT_MAX_ENABLED_VARIANTS
    if effective_limit > 0 and enabled_variant_count > effective_limit:
        raise RuntimeError(
            "Enabled variant count exceeds safety cap before Printify API call: "
            f"template={template.key} enabled_variant_count={enabled_variant_count} max_enabled_variants={effective_limit}. "
            "Reduce template filters (enabled_colors/enabled_sizes/enabled_variant_option_filters) or raise max_enabled_variants explicitly."
        )


def _is_http_404_error(exc: Exception) -> bool:
    return isinstance(exc, NonRetryableRequestError) and "HTTP 404" in str(exc)


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
    tags = _render_listing_tags(template, artwork)

    variants_payload: List[Dict[str, Any]] = []
    enabled_variant_ids: List[int] = []
    for variant in variant_rows:
        variant_id = int(variant["id"])
        enabled_variant_ids.append(variant_id)
        normalized_price = compute_sale_price_minor(template, variant)
        variant_payload = {"id": variant_id, "price": normalized_price, "is_enabled": True}
        compare_at_price = compute_compare_at_price_minor(template, normalized_price)
        if compare_at_price is not None:
            variant_payload["compare_at_price"] = compare_at_price
        variants_payload.append(variant_payload)

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




def validate_printify_payload_consistency(payload: Dict[str, Any]) -> Dict[str, Any]:
    variants = payload.get("variants", []) if isinstance(payload, dict) else []
    print_areas = payload.get("print_areas", []) if isinstance(payload, dict) else []
    enabled_variant_ids = {int(v.get("id")) for v in variants if isinstance(v, dict) and v.get("is_enabled", True) and "id" in v}

    area_variant_ids: set[int] = set()
    for area in print_areas if isinstance(print_areas, list) else []:
        if not isinstance(area, dict):
            continue
        for vid in area.get("variant_ids", []) or []:
            try:
                area_variant_ids.add(int(vid))
            except Exception:
                continue

    missing_variant_ids = sorted(enabled_variant_ids - area_variant_ids)
    is_consistent = len(missing_variant_ids) == 0 and bool(enabled_variant_ids)
    if not is_consistent:
        raise ValueError(
            "Inconsistent Printify payload: enabled variants are missing from print_areas.variant_ids "
            f"missing={missing_variant_ids} enabled_count={len(enabled_variant_ids)} print_area_variant_count={len(area_variant_ids)}"
        )

    return {
        "enabled_variant_ids": sorted(enabled_variant_ids),
        "print_area_variant_ids": sorted(area_variant_ids),
        "missing_variant_ids": missing_variant_ids,
        "enabled_variant_count": len(enabled_variant_ids),
        "print_area_variant_count": len(area_variant_ids),
    }


def assess_update_compatibility(existing_product: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    existing_blueprint = int(existing_product.get("blueprint_id") or 0)
    existing_provider = int(existing_product.get("print_provider_id") or 0)
    payload_blueprint = int(payload.get("blueprint_id") or 0)
    payload_provider = int(payload.get("print_provider_id") or 0)

    existing_enabled = {int(v.get("id")) for v in existing_product.get("variants", []) if isinstance(v, dict) and v.get("is_enabled", True) and "id" in v}
    payload_enabled = {int(v.get("id")) for v in payload.get("variants", []) if isinstance(v, dict) and v.get("is_enabled", True) and "id" in v}

    existing_positions = {str(ph.get("position")) for area in existing_product.get("print_areas", []) if isinstance(area, dict) for ph in (area.get("placeholders", []) or []) if isinstance(ph, dict) and ph.get("position")}
    payload_positions = {str(ph.get("position")) for area in payload.get("print_areas", []) if isinstance(area, dict) for ph in (area.get("placeholders", []) or []) if isinstance(ph, dict) and ph.get("position")}

    issues: List[str] = []
    if existing_blueprint and payload_blueprint and existing_blueprint != payload_blueprint:
        issues.append(f"blueprint mismatch existing={existing_blueprint} payload={payload_blueprint}")
    if existing_provider and payload_provider and existing_provider != payload_provider:
        issues.append(f"provider mismatch existing={existing_provider} payload={payload_provider}")
    missing_in_existing = sorted(payload_enabled - existing_enabled)
    if missing_in_existing:
        issues.append(f"existing product missing variant ids={missing_in_existing}")
    if payload_positions and not payload_positions.issubset(existing_positions):
        issues.append(f"print area position mismatch payload={sorted(payload_positions)} existing={sorted(existing_positions)}")

    return {
        "compatible": len(issues) == 0,
        "issues": issues,
        "missing_variant_ids": missing_in_existing,
        "existing_blueprint_id": existing_blueprint,
        "existing_print_provider_id": existing_provider,
    }

def preview_listing_copy(*, artworks: List[Artwork], templates: List[ProductTemplate]) -> None:
    for artwork in artworks:
        for template in templates:
            context = build_seo_context(template, artwork)
            title = render_product_title(template, artwork)
            description = render_product_description(template, artwork)
            tags = _render_listing_tags(template, artwork)
            text_preview = re.sub(r"<[^>]+>", "", description)
            text_preview = re.sub(r"\s+", " ", text_preview).strip()[:160]
            print(
                f"{artwork.src_path.name} | {template.key}\n"
                f"  title: {title}\n"
                f"  source: {context.get('title_source')} ({context.get('title_quality')})\n"
                f"  description: {text_preview}\n"
                f"  tags: {', '.join(tags[:12]) or '-'}"
            )


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


def _extract_enabled_variant_count(product: Dict[str, Any]) -> int:
    variants = product.get("variants", []) if isinstance(product, dict) else []
    if not isinstance(variants, list):
        return 0
    count = 0
    for variant in variants:
        if not isinstance(variant, dict):
            continue
        if variant.get("is_enabled") is False:
            continue
        count += 1
    return count


def verify_printify_product_readback(
    *,
    product: Dict[str, Any],
    expected_product_id: str,
    expected_title: str,
) -> Dict[str, Any]:
    warnings: List[str] = []
    observed_id = str(product.get("id") or product.get("product_id") or "")
    if expected_product_id and observed_id and observed_id != expected_product_id:
        warnings.append(f"id mismatch expected={expected_product_id} observed={observed_id}")

    observed_title = str(product.get("title") or "")
    if expected_title and observed_title and observed_title != expected_title:
        warnings.append("title mismatch")

    enabled_variant_count = _extract_enabled_variant_count(product)
    if enabled_variant_count <= 0:
        warnings.append("no enabled variants detected")

    print_areas = product.get("print_areas") if isinstance(product, dict) else None
    if not isinstance(print_areas, list) or not print_areas:
        warnings.append("print_areas missing")

    images = product.get("images") if isinstance(product, dict) else None
    if images is None:
        warnings.append("images field missing")

    storefront_ready = any(bool(product.get(flag)) for flag in ("visible", "is_visible", "is_published", "published"))
    publish_related = {
        "visible": product.get("visible"),
        "is_visible": product.get("is_visible"),
        "is_published": product.get("is_published"),
        "published": product.get("published"),
    }

    return {
        "ok": len(warnings) == 0,
        "warnings": warnings,
        "verified_product_id": observed_id,
        "verified_title": observed_title,
        "verified_variant_count": enabled_variant_count,
        "storefront_ready": storefront_ready,
        "publish_indicators": publish_related,
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


def log_template_summary(*, artwork_slug: str, template_key: str, success: bool, result: Dict[str, Any], blueprint_id: int = 0, provider_id: int = 0, action: str = "skip", upload_map: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
    printify_result = result.get("printify", {}) if isinstance(result, dict) else {}
    product_id = printify_result.get("printify_product_id") or "n/a"
    upload_strategy = summarize_upload_strategy(upload_map or {})
    status = "success" if success else "failure"
    logger.info(
        "Template summary artwork=%s template=%s status=%s action=%s product_id=%s provider_id=%s blueprint_id=%s upload_strategy=%s",
        artwork_slug,
        template_key,
        status,
        action,
        product_id,
        provider_id or "n/a",
        blueprint_id or "n/a",
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


def resolve_product_action(*, existing_product_id: str, create_only: bool, update_only: bool, rebuild_product: bool) -> str:
    if rebuild_product:
        return "rebuild"
    if create_only and update_only:
        raise RuntimeError("--create-only and --update-only cannot be used together")
    if create_only:
        return "skip" if existing_product_id else "create"
    if update_only:
        return "update" if existing_product_id else "skip"
    return "update" if existing_product_id else "create"


def upsert_in_printify(
    *,
    printify: PrintifyClient,
    shop_id: int,
    artwork: Artwork,
    template: ProductTemplate,
    variant_rows: List[Dict[str, Any]],
    upload_map: Dict[str, Dict[str, Any]],
    existing_product_id: str,
    action: str,
    publish_mode: str,
    verify_publish: bool,
    auto_rebuild_on_incompatible_update: bool = False,
) -> Dict[str, Any]:
    def _execute_create() -> Tuple[str, Dict[str, Any]]:
        try:
            created_resp = printify.create_product(shop_id, payload)
        except DryRunMutationSkipped:
            raise
        new_product_id = str(created_resp.get("id") or created_resp.get("data", {}).get("id") or "")
        return new_product_id, {"action": "create", "printify_product_id": new_product_id, "created": created_resp}

    payload = build_printify_product_payload(artwork, template, variant_rows, upload_map)
    payload_stats = validate_printify_payload_consistency(payload)
    enforce_variant_safety_limit(template=template, enabled_variant_count=payload_stats["enabled_variant_count"])
    logger.info("Mockup/image publish behavior template=%s publish_images=%s publish_mockups_override=%s", template.key, template.publish_images, template.publish_mockups)

    if action == "skip":
        return {"status": "skipped", "action": "skip", "printify_product_id": existing_product_id or ""}

    if action == "update" and not existing_product_id:
        raise RuntimeError("Cannot update product because no existing product_id was found")

    if action == "create":
        try:
            product_id, result = _execute_create()
        except DryRunMutationSkipped:
            return {"status": "dry-run", "action": "create", "payload_preview": payload}
    elif action == "update":
        try:
            existing_product = printify.get_product(shop_id, existing_product_id)
        except Exception as exc:
            if _is_http_404_error(exc):
                logger.warning(
                    "Stored product_id not found in Printify; treating as missing and creating a new product "
                    "template=%s stale_product_id=%s",
                    template.key,
                    existing_product_id,
                )
                logger.warning(
                    "Actionable guidance: state entry pointed at a deleted product. InkVibeAuto will create a new product and update state automatically."
                )
                action = "create"
                existing_product_id = ""
            else:
                raise

        if action == "create":
            try:
                product_id, result = _execute_create()
            except DryRunMutationSkipped:
                return {"status": "dry-run", "action": "create", "payload_preview": payload}

        if action == "update":
            compatibility = assess_update_compatibility(existing_product, payload)
            logger.info(
                "Update preflight product_id=%s action=%s enabled_variant_count=%s print_area_variant_count=%s",
                existing_product_id,
                action,
                payload_stats["enabled_variant_count"],
                payload_stats["print_area_variant_count"],
            )
            if not compatibility["compatible"]:
                logger.warning(
                    "Update compatibility failed product_id=%s issues=%s suggested_action=%s",
                    existing_product_id,
                    "; ".join(compatibility["issues"]),
                    "--rebuild-product" if not auto_rebuild_on_incompatible_update else "auto-rebuild",
                )
                if not auto_rebuild_on_incompatible_update:
                    raise RuntimeError(
                        "Incompatible update payload for existing Printify product. "
                        f"issues={compatibility['issues']}. Use --rebuild-product or --auto-rebuild-on-incompatible-update"
                    )
                action = "rebuild"

            if action == "update":
                try:
                    updated = printify.update_product(shop_id, existing_product_id, payload)
                except DryRunMutationSkipped:
                    return {"status": "dry-run", "action": "update", "payload_preview": payload, "printify_product_id": existing_product_id}
                result = {"action": "update", "printify_product_id": existing_product_id, "updated": updated}
                product_id = existing_product_id
            else:
                try:
                    printify.delete_product(shop_id, existing_product_id)
                except DryRunMutationSkipped:
                    return {"status": "dry-run", "action": "rebuild", "payload_preview": payload, "printify_product_id": existing_product_id}
                except Exception:
                    logger.warning("Delete existing product failed during rebuild product_id=%s", existing_product_id)
                product_id, create_result = _execute_create()
                result = {"action": "rebuild", "previous_product_id": existing_product_id, "printify_product_id": product_id, "created": create_result["created"]}
    elif action == "rebuild":
        if existing_product_id:
            try:
                printify.delete_product(shop_id, existing_product_id)
            except DryRunMutationSkipped:
                return {"status": "dry-run", "action": "rebuild", "payload_preview": payload, "printify_product_id": existing_product_id}
            except Exception:
                logger.warning("Delete existing product failed during rebuild product_id=%s", existing_product_id)
        try:
            product_id, create_result = _execute_create()
        except DryRunMutationSkipped:
            return {"status": "dry-run", "action": "rebuild", "payload_preview": payload, "printify_product_id": existing_product_id}
        result = {"action": "rebuild", "previous_product_id": existing_product_id, "printify_product_id": product_id, "created": create_result["created"]}
    else:
        raise RuntimeError(f"Unsupported action: {action}")

    logger.info("Printify product action=%s product_id=%s title=%s enabled_variants=%s", action, product_id, payload.get("title", ""), len(payload.get("variants", [])))
    should_publish = (template.publish_after_create if publish_mode == "default" else publish_mode == "publish") and bool(product_id)
    result["publish_attempted"] = False
    result["publish_verified"] = False

    if should_publish:
        result["publish_attempted"] = True
        try:
            result["published"] = printify.publish_product(shop_id, product_id, build_printify_publish_payload(template))
            logger.info("Printify publish completed product_id=%s", product_id)
        except DryRunMutationSkipped:
            result["published"] = {"status": "dry-run"}

    if verify_publish and product_id:
        try:
            readback = printify.get_product(shop_id, product_id)
            verification = verify_printify_product_readback(
                product=readback,
                expected_product_id=product_id,
                expected_title=str(payload.get("title") or ""),
            )
            result["verification"] = verification
            result["verified_product"] = readback
            if verification.get("ok"):
                result["publish_verified"] = True
                logger.info("Printify verification ok product_id=%s variants=%s", product_id, verification.get("verified_variant_count", 0))
            else:
                logger.warning("Printify verification warnings product_id=%s warnings=%s", product_id, verification.get("warnings", []))
                if not result["publish_attempted"]:
                    logger.info("Verification ran without publish attempt (requested --verify-publish).")
        except Exception as exc:
            result["verification"] = {"ok": False, "warnings": [f"readback failed: {exc}"]}
            logger.warning("Printify verification failed product_id=%s error=%s", product_id, exc)
    return result


def process_artwork(*, printify: PrintifyClient, shopify: Optional[ShopifyClient], shop_id: Optional[int], artwork: Artwork, templates: List[ProductTemplate], state: Dict[str, Any], force: bool, export_dir: pathlib.Path, state_path: pathlib.Path, artwork_options: ArtworkProcessingOptions, upload_strategy: str, r2_config: Optional[R2Config], create_only: bool = False, update_only: bool = False, rebuild_product: bool = False, publish_mode: str = "default", verify_publish: bool = False, auto_rebuild_on_incompatible_update: bool = False, summary: Optional[RunSummary] = None, failure_rows: Optional[List[FailureReportRow]] = None, run_rows: Optional[List[RunReportRow]] = None) -> None:
    processed = state.setdefault("processed", {})
    existing = processed.get(artwork.slug, {})
    existing_products = existing.get("products", []) if isinstance(existing.get("products", []), list) else []

    logger.info("Processing artwork: %s", artwork.src_path.name)
    record: Dict[str, Any] = {
        "products": existing_products if (existing_products and not force) else [],
        "completed": False,
        "last_run": datetime.now(timezone.utc).isoformat(),
    }

    all_templates_successful = True
    for template in templates:
        title_info = resolve_artwork_title(template, artwork)
        rendered_title = render_product_title(template, artwork)
        state_key = f"{artwork.slug}:{template.key}"
        matching_rows = [row for row in record["products"] if isinstance(row, dict) and row.get("state_key") == state_key]
        existing_product_id = ""
        for row in reversed(matching_rows):
            row_result = row.get("result", {}) if isinstance(row.get("result"), dict) else {}
            printify_row = row_result.get("printify", {}) if isinstance(row_result.get("printify"), dict) else {}
            candidate = printify_row.get("printify_product_id")
            if isinstance(candidate, str) and candidate.strip() and row_result.get("error") is None:
                existing_product_id = candidate.strip()
                break

        action = resolve_product_action(
            existing_product_id=existing_product_id,
            create_only=create_only,
            update_only=update_only,
            rebuild_product=rebuild_product,
        )

        if summary is not None:
            summary.templates_processed += 1

        upload_map: Dict[str, Dict[str, Any]] = {}
        try:
            if action == "skip":
                result = {"printify": {"status": "skipped", "action": "skip", "printify_product_id": existing_product_id}}
                if summary is not None:
                    summary.products_skipped += 1
                record["products"].append({
                    "template": template.key,
                    "state_key": state_key,
                    "blueprint_id": template.printify_blueprint_id,
                    "print_provider_id": template.printify_print_provider_id,
                    "last_action": "skip",
                    "publish_attempted": False,
                    "publish_verified": False,
                    "last_verified_at": None,
                    "verified_title": None,
                    "verified_variant_count": None,
                    "title_source": title_info.title_source,
                    "rendered_title": rendered_title,
                    "result": result,
                })
                if run_rows is not None:
                    run_rows.append(RunReportRow(
                        timestamp=datetime.now(timezone.utc).isoformat(),
                        artwork_filename=artwork.src_path.name,
                        artwork_slug=artwork.slug,
                        template_key=template.key,
                        status="skipped",
                        action="skip",
                        blueprint_id=template.printify_blueprint_id,
                        provider_id=template.printify_print_provider_id,
                        upload_strategy=upload_strategy,
                        product_id=existing_product_id,
                        publish_attempted=False,
                        publish_verified=False,
                        rendered_title=rendered_title,
                    ))
                log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=True, result=result, blueprint_id=template.printify_blueprint_id, provider_id=template.printify_print_provider_id, action="skip", upload_map=upload_map)
                continue

            catalog_variants = printify.list_variants(template.printify_blueprint_id, template.printify_print_provider_id)
            variant_rows = choose_variants_from_catalog(catalog_variants, template)
            if not variant_rows:
                all_templates_successful = False
                if summary is not None:
                    summary.products_skipped += 1
                result = {"status": "no_matching_variants"}
                record["products"].append({"template": template.key, "state_key": state_key, "title_source": title_info.title_source, "rendered_title": rendered_title, "result": result})
                if run_rows is not None:
                    run_rows.append(RunReportRow(datetime.now(timezone.utc).isoformat(), artwork.src_path.name, artwork.slug, template.key, "skipped", "skip", template.printify_blueprint_id, template.printify_print_provider_id, upload_strategy, "", False, False, rendered_title))
                log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": result}, blueprint_id=template.printify_blueprint_id, provider_id=template.printify_print_provider_id, action="skip", upload_map=upload_map)
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
                if summary is not None:
                    summary.products_skipped += 1
                result = {"status": "skipped_undersized", "placements": skipped_placements}
                record["products"].append({
                    "template": template.key,
                    "state_key": state_key,
                    "last_action": "skip",
                    "publish_attempted": False,
                    "publish_verified": False,
                    "last_verified_at": None,
                    "verified_title": None,
                    "verified_variant_count": None,
                    "title_source": title_info.title_source,
                    "rendered_title": rendered_title,
                    "result": result,
                })
                if run_rows is not None:
                    run_rows.append(RunReportRow(datetime.now(timezone.utc).isoformat(), artwork.src_path.name, artwork.slug, template.key, "skipped", "skip", template.printify_blueprint_id, template.printify_print_provider_id, upload_strategy, "", False, False, rendered_title))
                log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": result}, blueprint_id=template.printify_blueprint_id, provider_id=template.printify_print_provider_id, action="skip", upload_map=upload_map)
                continue

            upload_map = upload_assets_to_printify(printify, state, artwork, template, prepared_assets, state_path, upload_strategy, r2_config)

            result: Dict[str, Any] = {}
            result["printify"] = upsert_in_printify(
                printify=printify,
                shop_id=shop_id,
                artwork=artwork,
                template=template,
                variant_rows=variant_rows,
                upload_map=upload_map,
                existing_product_id=existing_product_id,
                action=action,
                publish_mode=publish_mode,
                verify_publish=verify_publish,
                auto_rebuild_on_incompatible_update=auto_rebuild_on_incompatible_update,
            ) if (template.push_via_printify and shop_id is not None) else {"status": "prepared_only", "action": action, "publish_attempted": False, "publish_verified": False}
            if template.publish_to_shopify and shopify is not None:
                result["shopify"] = create_in_shopify_only(shopify, artwork, template, variant_rows)

            printify_result = result.get("printify", {}) if isinstance(result.get("printify"), dict) else {}
            verification = printify_result.get("verification", {}) if isinstance(printify_result.get("verification"), dict) else {}
            verification_warnings = verification.get("warnings", []) if isinstance(verification.get("warnings", []), list) else []
            row = {
                "template": template.key,
                "state_key": state_key,
                "blueprint_id": template.printify_blueprint_id,
                "print_provider_id": template.printify_print_provider_id,
                "last_action": printify_result.get("action", action),
                "publish_attempted": bool(printify_result.get("publish_attempted", False)),
                "publish_verified": bool(printify_result.get("publish_verified", False)),
                "last_verified_at": datetime.now(timezone.utc).isoformat() if verification else None,
                "verified_title": verification.get("verified_title"),
                "verified_variant_count": verification.get("verified_variant_count"),
                "title_source": title_info.title_source,
                "rendered_title": rendered_title,
                "result": result,
            }
            record["products"].append(row)
            if run_rows is not None:
                run_rows.append(RunReportRow(
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    artwork_filename=artwork.src_path.name,
                    artwork_slug=artwork.slug,
                    template_key=template.key,
                    status="success",
                    action=str(printify_result.get("action", action)),
                    blueprint_id=template.printify_blueprint_id,
                    provider_id=template.printify_print_provider_id,
                    upload_strategy=summarize_upload_strategy(upload_map),
                    product_id=str(printify_result.get("printify_product_id") or ""),
                    publish_attempted=bool(row.get("publish_attempted")),
                    publish_verified=bool(row.get("publish_verified")),
                    rendered_title=rendered_title,
                ))
            if summary is not None:
                printify_action = (result.get("printify", {}) or {}).get("action", action)
                if printify_action == "create":
                    summary.products_created += 1
                elif printify_action == "update":
                    summary.products_updated += 1
                elif printify_action == "rebuild":
                    summary.products_rebuilt += 1
                elif printify_action == "skip":
                    summary.products_skipped += 1
                if row.get("publish_attempted"):
                    summary.publish_attempts += 1
                if row.get("publish_verified"):
                    summary.publish_verified += 1
                if verification_warnings:
                    summary.verification_warnings += 1
            log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=True, result=result, blueprint_id=template.printify_blueprint_id, provider_id=template.printify_print_provider_id, action=(result.get("printify", {}) or {}).get("action", action), upload_map=upload_map)
        except Exception as exc:
            all_templates_successful = False
            if summary is not None:
                summary.failures += 1
            logger.exception("Sync failed for artwork=%s template=%s", artwork.slug, template.key)
            error_result = {"error": str(exc)}
            record["products"].append({
                "template": template.key,
                "state_key": state_key,
                "last_action": action,
                "publish_attempted": False,
                "publish_verified": False,
                "last_verified_at": None,
                "verified_title": None,
                "verified_variant_count": None,
                "title_source": title_info.title_source,
                "rendered_title": rendered_title,
                "result": error_result,
            })
            if failure_rows is not None:
                failure_rows.append(FailureReportRow(
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    artwork_filename=artwork.src_path.name,
                    artwork_slug=artwork.slug,
                    template_key=template.key,
                    action_attempted=action,
                    blueprint_id=template.printify_blueprint_id,
                    provider_id=template.printify_print_provider_id,
                    upload_strategy=summarize_upload_strategy(upload_map),
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    suggested_next_action="Inspect state and rerun with --resume after fixing template or artwork",
                ))
            if run_rows is not None:
                run_rows.append(RunReportRow(datetime.now(timezone.utc).isoformat(), artwork.src_path.name, artwork.slug, template.key, "failure", action, template.printify_blueprint_id, template.printify_print_provider_id, summarize_upload_strategy(upload_map), "", False, False, rendered_title))
            log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": error_result}, blueprint_id=template.printify_blueprint_id, provider_id=template.printify_print_provider_id, action=action, upload_map=upload_map)
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
        blueprint_title = str(blueprints[template.printify_blueprint_id].get("title", ""))
        type_warning = template_blueprint_type_warning(template=template, blueprint_title=blueprint_title)
        if type_warning:
            logger.warning("Template blueprint-type mismatch: %s", type_warning)
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
    parser.add_argument("--batch-size", type=int, default=0, help="Limit number of artwork/template combinations processed this run (0 = no limit)")
    parser.add_argument("--stop-after-failures", type=int, default=0, help="Stop run after N combination failures (0 = no limit)")
    parser.add_argument("--fail-fast", action="store_true", help="Stop immediately after the first combination failure")
    parser.add_argument("--resume", action="store_true", help="Skip combinations already successful in state and continue pending work")
    parser.add_argument("--template-key", action="append", default=[], help="Only process matching template key(s); can be repeated")
    parser.add_argument("--limit-templates", type=int, default=0, help="Limit number of templates after filtering (0 = no limit)")
    parser.add_argument("--list-templates", action="store_true", help="List templates from config and exit")
    parser.add_argument("--upload-strategy", choices=["auto", "direct", "r2_url"], default="auto", help="Asset upload strategy: auto (default), direct, or r2_url")
    parser.add_argument("--list-blueprints", action="store_true", help="List Printify catalog blueprints")
    parser.add_argument("--search-blueprints", default="", help="Search Printify blueprints by title/keywords")
    parser.add_argument("--limit-blueprints", type=int, default=25, help="Limit blueprint results (0 = no limit)")
    parser.add_argument("--list-providers", action="store_true", help="List providers for a blueprint")
    parser.add_argument("--blueprint-id", type=int, default=0, help="Printify blueprint id used by provider/variant tools")
    parser.add_argument("--provider-id", type=int, default=0, help="Printify provider id used by variant/template tools")
    parser.add_argument("--limit-providers", type=int, default=25, help="Limit provider results (0 = no limit)")
    parser.add_argument("--inspect-variants", action="store_true", help="Inspect variants for a blueprint/provider")
    parser.add_argument("--recommend-provider", action="store_true", help="Recommend a provider for the blueprint")
    parser.add_argument("--generate-template-snippet", action="store_true", help="Generate a starter template snippet JSON")
    parser.add_argument("--auto-provider", action="store_true", help="Automatically choose the best provider for inspect/snippet catalog flows")
    parser.add_argument("--template-file", default="", help="Optional template JSON path for recommendation context")
    parser.add_argument("--template-output-file", default="", help="Optional file path to write generated template snippet JSON")
    parser.add_argument("--key", default="", help="Template key used for snippet generation or provider recommendation context")
    parser.add_argument("--create-only", action="store_true", help="Only create products; skip when state already has a product id")
    parser.add_argument("--update-only", action="store_true", help="Only update products; skip when no product id exists in state")
    parser.add_argument("--rebuild-product", action="store_true", help="Delete+recreate products when state already has a product id")
    parser.add_argument("--auto-rebuild-on-incompatible-update", action="store_true", help="Automatically rebuild when update preflight detects blueprint/provider/variant incompatibility")
    parser.add_argument("--publish", action="store_true", help="Force publish after create/update/rebuild")
    parser.add_argument("--skip-publish", action="store_true", help="Skip publish after create/update/rebuild")
    parser.add_argument("--verify-publish", action="store_true", help="Read back created/updated product and verify basic storefront indicators")
    parser.add_argument("--inspect-state-key", default="", help="Read-only inspect state entry by key (artwork_slug:template_key)")
    parser.add_argument("--list-state-keys", action="store_true", help="List known state keys from state.json and exit")
    parser.add_argument("--list-failures", action="store_true", help="List failed combinations from state and exit")
    parser.add_argument("--list-pending", action="store_true", help="List combinations not yet successful and exit")
    parser.add_argument("--export-failure-report", default="", help="Optional CSV export path for failed combinations")
    parser.add_argument("--export-run-report", default="", help="Optional CSV export path for all processed combinations")
    parser.add_argument("--preview-listing-copy", action="store_true", help="Render listing title/description/tags previews without creating/updating products")
    parser.epilog = (
        "Examples:\n"
        "  python printify_shopify_sync_pipeline.py --dry-run --template-key tshirt_gildan --template-key mug_11oz\n"
        "  python printify_shopify_sync_pipeline.py --list-blueprints --search-blueprints hoodie --limit-blueprints 10\n"
        "  python printify_shopify_sync_pipeline.py --list-providers --blueprint-id 5"
    )
    return parser.parse_args()


def select_templates(
    templates: List[ProductTemplate],
    *,
    template_keys: Optional[List[str]] = None,
    limit_templates: int = 0,
) -> List[ProductTemplate]:
    selected = templates
    if template_keys:
        wanted = {key.strip() for key in template_keys if key.strip()}
        selected = [template for template in templates if template.key in wanted]
    if limit_templates > 0:
        selected = selected[:limit_templates]
    return selected


def _find_template_by_key(config_path: pathlib.Path, template_key: str) -> Optional[ProductTemplate]:
    if not template_key:
        return None
    for template in load_templates(config_path):
        if template.key == template_key:
            return template
    return None


def _format_provider_choices(providers: List[Dict[str, Any]]) -> str:
    if not providers:
        return "none"
    return ", ".join(f"{int(provider.get('id', 0))} {provider.get('title', '').strip() or '<untitled>'}" for provider in providers)


def _require_provider_for_blueprint(
    *,
    providers: List[Dict[str, Any]],
    blueprint_id: int,
    provider_id: int,
) -> Dict[str, Any]:
    for provider in providers:
        if int(provider.get("id", 0)) == provider_id:
            return provider
    valid_choices = _format_provider_choices(providers)
    hint = (
        f"Try: --list-providers --blueprint-id {blueprint_id} or "
        f"--recommend-provider --blueprint-id {blueprint_id}"
    )
    raise CatalogCliUsageError(
        f"Provider {provider_id} is not available for blueprint {blueprint_id}. "
        f"Valid providers: {valid_choices}. {hint}"
    )


def _recommend_provider_for_blueprint(
    *,
    printify: PrintifyClient,
    providers: List[Dict[str, Any]],
    blueprint_id: int,
    template: Optional[ProductTemplate],
) -> List[Dict[str, Any]]:
    scored_rows: List[Dict[str, Any]] = []
    for provider in providers:
        variants = printify.list_variants(blueprint_id, int(provider.get("id", 0)))
        score = score_provider_for_template(provider, variants, template=template)
        scored_rows.append(score)
    scored_rows.sort(key=lambda row: row["score"], reverse=True)
    return scored_rows


def run_catalog_cli(
    *,
    printify: PrintifyClient,
    config_path: pathlib.Path,
    list_blueprints: bool,
    search_query: str,
    limit_blueprints: int,
    list_providers: bool,
    blueprint_id: int,
    provider_id: int,
    limit_providers: int,
    inspect_variants: bool,
    recommend_provider: bool,
    template_file: str,
    generate_template_snippet_flag: bool,
    auto_provider: bool,
    snippet_key: str,
    template_output_file: str,
) -> bool:
    if not any([list_blueprints, bool(search_query), list_providers, inspect_variants, recommend_provider, generate_template_snippet_flag]):
        return False

    if list_blueprints or search_query:
        blueprints = printify.list_blueprints()
        rows = search_blueprints(blueprints, search_query) if search_query else blueprints
        if limit_blueprints > 0:
            rows = rows[:limit_blueprints]
        for blueprint in rows:
            print(
                f"id={blueprint.get('id')}	title={blueprint.get('title', '')}	brand={_extract_blueprint_brand(blueprint) or '-'}	model={_extract_blueprint_model(blueprint) or '-'}"
            )
        return True

    if (list_providers or inspect_variants or recommend_provider or generate_template_snippet_flag) and blueprint_id <= 0:
        raise RuntimeError("--blueprint-id is required")

    if auto_provider and not (inspect_variants or generate_template_snippet_flag):
        raise RuntimeError("--auto-provider is only supported with --inspect-variants or --generate-template-snippet")

    if list_providers or recommend_provider:
        providers = printify.list_print_providers(blueprint_id)
        template = None
        if recommend_provider and snippet_key:
            template_path = pathlib.Path(template_file) if template_file else config_path
            template = _find_template_by_key(template_path, snippet_key)
        scored_rows = _recommend_provider_for_blueprint(
            printify=printify,
            providers=providers,
            blueprint_id=blueprint_id,
            template=template,
        )
        rows = scored_rows[:limit_providers] if limit_providers > 0 else scored_rows

        if list_providers:
            for row in rows:
                print(
                    f"provider_id={row['provider_id']}	title={row['provider_title']}	variants={row['variant_count']}	colors={','.join(row['colors'][:6]) or '-'}	sizes={','.join(row['sizes'][:6]) or '-'}"
                )
            return True

        if recommend_provider:
            if not rows:
                print("No providers found")
                return True
            for row in rows:
                print(
                    f"provider_id={row['provider_id']}	title={row['provider_title']}	score={row['score']}	matching_colors={row['matching_color_count']}	matching_sizes={row['matching_size_count']}	matching_variants={row['matching_variant_count']}	placements={','.join(row['placements'][:5]) or '-'}"
                )
            top = rows[0]
            print(
                f"Top provider rationale: provider_id={top['provider_id']} chosen for highest score ({top['score']}) with colors={top['matching_color_count']}, sizes={top['matching_size_count']}, variants={top['matching_variant_count']}, placements={top['placement_match_count']}."
            )
            return True

    if inspect_variants or generate_template_snippet_flag:
        providers = printify.list_print_providers(blueprint_id)
        selected_provider_id = provider_id
        if auto_provider:
            template_path = pathlib.Path(template_file) if template_file else config_path
            template = _find_template_by_key(template_path, snippet_key)
            rows = _recommend_provider_for_blueprint(
                printify=printify,
                providers=providers,
                blueprint_id=blueprint_id,
                template=template,
            )
            if not rows:
                raise CatalogCliUsageError(f"No providers available for blueprint {blueprint_id}.")
            top = rows[0]
            selected_provider_id = int(top["provider_id"])
            print(
                f"Auto-selected provider_id={selected_provider_id} title={top['provider_title']} score={top['score']} "
                f"(colors={top['matching_color_count']}, sizes={top['matching_size_count']}, variants={top['matching_variant_count']}, placements={top['placement_match_count']})."
            )

        if selected_provider_id <= 0:
            raise RuntimeError("--provider-id is required unless --auto-provider is used")
        _require_provider_for_blueprint(
            providers=providers,
            blueprint_id=blueprint_id,
            provider_id=selected_provider_id,
        )
        variants = printify.list_variants(blueprint_id, selected_provider_id)
        summary = summarize_variant_options(variants)

        if inspect_variants:
            print(
                f"blueprint_id={blueprint_id}	provider_id={selected_provider_id}	variant_count={len(variants)}	colors={','.join(summary['colors'][:12]) or '-'}	sizes={','.join(summary['sizes'][:12]) or '-'}"
            )
            return True

        if generate_template_snippet_flag:
            key = snippet_key or f"blueprint_{blueprint_id}_provider_{selected_provider_id}"
            snippet_factory = generate_mug_template_snippet if "mug" in key.lower() else generate_template_snippet
            snippet = snippet_factory(key=key, blueprint_id=blueprint_id, provider_id=selected_provider_id, variants=variants)
            rendered = json.dumps(snippet, indent=2)
            if template_output_file:
                pathlib.Path(template_output_file).write_text(rendered + "\n", encoding="utf-8")
            else:
                print(rendered)
            return True

    return False


def run(config_path: pathlib.Path, *, dry_run: bool = False, force: bool = False, allow_upscale: bool = False, upscale_method: str = "lanczos", skip_undersized: bool = False, image_dir: pathlib.Path = IMAGE_DIR, export_dir: pathlib.Path = EXPORT_DIR, state_path: pathlib.Path = STATE_PATH, skip_audit: bool = False, max_artworks: int = 0, batch_size: int = 0, stop_after_failures: int = 0, fail_fast: bool = False, resume: bool = False, upload_strategy: str = "auto", template_keys: Optional[List[str]] = None, limit_templates: int = 0, list_templates: bool = False, list_blueprints: bool = False, search_blueprints_query: str = "", limit_blueprints: int = 25, list_providers: bool = False, blueprint_id: int = 0, provider_id: int = 0, limit_providers: int = 25, inspect_variants: bool = False, recommend_provider: bool = False, template_file: str = "", generate_template_snippet_flag: bool = False, auto_provider: bool = False, snippet_key: str = "", template_output_file: str = "", create_only: bool = False, update_only: bool = False, rebuild_product: bool = False, publish_mode: str = "default", verify_publish: bool = False, auto_rebuild_on_incompatible_update: bool = False, inspect_state_key_value: str = "", list_state_keys_only: bool = False, list_failures_only: bool = False, list_pending_only: bool = False, export_failure_report: str = "", export_run_report: str = "", preview_listing_copy_only: bool = False) -> None:
    if publish_mode not in {"default", "publish", "skip"}:
        raise RuntimeError(f"Unsupported publish mode: {publish_mode}")

    state = ensure_state_shape(load_json(state_path, {}))
    if list_state_keys_only:
        for key in list_state_keys(state):
            print(key)
        return
    if inspect_state_key_value:
        row = inspect_state_key(state, inspect_state_key_value)
        if row is None:
            print(json.dumps({"state_key": inspect_state_key_value, "found": False}, ensure_ascii=False))
        else:
            print(json.dumps(row, indent=2, ensure_ascii=False))
        return

    if list_failures_only:
        for key, row in latest_rows_by_state_key(state).items():
            if _row_status(row) != "failure":
                continue
            print(f"{key}\t{str((row.get('result', {}) or {}).get('error', ''))[:120]}")
        return

    if not image_dir.exists():
        raise RuntimeError(f"Missing image directory: {image_dir}")

    templates = load_templates(config_path)
    if list_templates:
        for template in templates:
            print(f"{template.key}	blueprint={template.printify_blueprint_id}	provider={template.printify_print_provider_id}")
        return
    templates = select_templates(templates, template_keys=template_keys, limit_templates=limit_templates)
    artworks = discover_artworks(image_dir)
    if max_artworks > 0:
        artworks = artworks[:max_artworks]
    if preview_listing_copy_only:
        preview_listing_copy(artworks=artworks, templates=templates)
        return

    if list_pending_only:
        index = latest_rows_by_state_key(state)
        for artwork in artworks:
            for template in templates:
                state_key = f"{artwork.slug}:{template.key}"
                row = index.get(state_key)
                if row is None or _row_status(row) != "success":
                    print(state_key)
        return

    if not PRINTIFY_API_TOKEN:
        raise RuntimeError("Missing PRINTIFY_API_TOKEN")

    printify = PrintifyClient(PRINTIFY_API_TOKEN, dry_run=dry_run)

    if run_catalog_cli(
        printify=printify,
        config_path=config_path,
        list_blueprints=list_blueprints,
        search_query=search_blueprints_query,
        limit_blueprints=limit_blueprints,
        list_providers=list_providers,
        blueprint_id=blueprint_id,
        provider_id=provider_id,
        limit_providers=limit_providers,
        inspect_variants=inspect_variants,
        recommend_provider=recommend_provider,
        template_file=template_file,
        generate_template_snippet_flag=generate_template_snippet_flag,
        auto_provider=auto_provider,
        snippet_key=snippet_key,
        template_output_file=template_output_file,
    ):
        return

    shop_id = resolve_shop_id(printify, PRINTIFY_SHOP_ID)
    shopify = ShopifyClient(SHOPIFY_ADMIN_TOKEN, dry_run=dry_run) if SHOPIFY_ADMIN_TOKEN else None
    r2_config = load_r2_config_from_env()

    if not skip_audit:
        audit_printify_integration(printify, templates, shop_id)

    logger.info("Loaded %s template(s) and %s artwork file(s)", len(templates), len(artworks))
    summary = RunSummary(artworks_scanned=len(artworks))
    failure_rows: List[FailureReportRow] = []
    run_rows: List[RunReportRow] = []
    artwork_options = ArtworkProcessingOptions(allow_upscale=allow_upscale, upscale_method=upscale_method, skip_undersized=skip_undersized)
    combinations_processed = 0
    stop_requested = False
    for artwork in artworks:
        templates_for_artwork: List[ProductTemplate] = []
        for template in templates:
            if batch_size > 0 and combinations_processed >= batch_size:
                stop_requested = True
                break
            if resume and is_state_key_successful(state, f"{artwork.slug}:{template.key}"):
                continue
            templates_for_artwork.append(template)
            combinations_processed += 1
        for template in templates_for_artwork:
            before_failures = summary.failures
            process_artwork(
                printify=printify,
                shopify=shopify,
                shop_id=shop_id,
                artwork=artwork,
                templates=[template],
                state=state,
                force=force,
                export_dir=export_dir,
                state_path=state_path,
                artwork_options=artwork_options,
                upload_strategy=upload_strategy,
                r2_config=r2_config,
                create_only=create_only,
                update_only=update_only,
                rebuild_product=rebuild_product,
                publish_mode=publish_mode,
                verify_publish=verify_publish,
                auto_rebuild_on_incompatible_update=auto_rebuild_on_incompatible_update,
                summary=summary,
                failure_rows=failure_rows,
                run_rows=run_rows,
            )
            if summary.failures > before_failures:
                if fail_fast:
                    stop_requested = True
                    break
                if stop_after_failures > 0 and summary.failures >= stop_after_failures:
                    stop_requested = True
                    break
        if stop_requested:
            break

    summary.combinations_processed = len(run_rows)
    summary.combinations_success = sum(1 for row in run_rows if row.status == "success")
    summary.combinations_failed = sum(1 for row in run_rows if row.status == "failure")
    summary.combinations_skipped = sum(1 for row in run_rows if row.status == "skipped")

    if export_failure_report:
        write_csv_report(pathlib.Path(export_failure_report), [row.__dict__ for row in failure_rows])
        logger.info("Failure report exported path=%s rows=%s", export_failure_report, len(failure_rows))
    if export_run_report:
        write_csv_report(pathlib.Path(export_run_report), [row.__dict__ for row in run_rows])
        logger.info("Run report exported path=%s rows=%s", export_run_report, len(run_rows))

    save_json_atomic(state_path, state)
    logger.info(format_run_summary(summary))
    if failure_rows:
        logger.info("Failures encountered (showing up to 5):")
        for row in failure_rows[:5]:
            logger.info("- %s:%s action=%s error=%s", row.artwork_slug, row.template_key, row.action_attempted, row.error_message)
    logger.info("Done")


if __name__ == "__main__":
    args = parse_args()
    configure_logging(args.log_level)
    publish_mode = "default"
    if args.publish and args.skip_publish:
        raise SystemExit("--publish and --skip-publish cannot be used together")
    if args.publish:
        publish_mode = "publish"
    elif args.skip_publish:
        publish_mode = "skip"
    try:
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
            batch_size=args.batch_size,
            stop_after_failures=args.stop_after_failures,
            fail_fast=args.fail_fast,
            resume=args.resume,
            upload_strategy=args.upload_strategy,
            template_keys=args.template_key,
            limit_templates=args.limit_templates,
            list_templates=args.list_templates,
            list_blueprints=args.list_blueprints,
            search_blueprints_query=args.search_blueprints,
            limit_blueprints=args.limit_blueprints,
            list_providers=args.list_providers,
            blueprint_id=args.blueprint_id,
            provider_id=args.provider_id,
            limit_providers=args.limit_providers,
            inspect_variants=args.inspect_variants,
            recommend_provider=args.recommend_provider,
            template_file=args.template_file,
            generate_template_snippet_flag=args.generate_template_snippet,
            auto_provider=args.auto_provider,
            snippet_key=args.key,
            template_output_file=args.template_output_file,
            create_only=args.create_only,
            update_only=args.update_only,
            rebuild_product=args.rebuild_product,
            publish_mode=publish_mode,
            verify_publish=args.verify_publish,
            auto_rebuild_on_incompatible_update=args.auto_rebuild_on_incompatible_update,
            inspect_state_key_value=args.inspect_state_key,
            list_state_keys_only=args.list_state_keys,
            list_failures_only=args.list_failures,
            list_pending_only=args.list_pending,
            export_failure_report=args.export_failure_report,
            export_run_report=args.export_run_report,
            preview_listing_copy_only=args.preview_listing_copy,
        )
    except CatalogCliUsageError as exc:
        print(f"Catalog CLI error: {exc}")
        raise SystemExit(2)
    except NonRetryableRequestError as exc:
        catalog_mode = any([
            args.list_blueprints,
            bool(args.search_blueprints),
            args.list_providers,
            args.inspect_variants,
            args.recommend_provider,
            args.generate_template_snippet,
        ])
        if catalog_mode:
            print(f"Catalog request failed: {exc}")
            raise SystemExit(2)
        raise
