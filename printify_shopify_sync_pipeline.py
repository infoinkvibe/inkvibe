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
from dataclasses import dataclass, field, replace
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from PIL import Image, ImageOps
from r2_uploader import R2Config, build_r2_public_url, load_r2_config_from_env, upload_file_to_r2
import content_engine
import state_store
from artwork_metadata_generator import (
    MetadataGeneratorMode,
    MetadataReviewDecision,
    MetadataReviewRow,
    build_metadata_review_row,
    discover_artwork_images,
    evaluate_generated_metadata,
    export_metadata_review_csv,
    export_metadata_review_json,
    preview_generated_metadata,
    select_artwork_metadata_generator,
    should_write_sidecar,
    should_auto_approve_metadata,
    write_artwork_sidecar,
)
from artwork_generation import (
    ArtworkGenerationRequest,
    GeneratedArtworkAsset,
    choose_preferred_generated_asset,
    is_preview_or_low_value_asset,
    plan_generated_artwork_targets,
    generate_artwork_with_openai,
    validate_generated_asset_for_templates,
)

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
ARTWORK_METADATA_MAP_PATH = pathlib.Path(os.getenv("ARTWORK_METADATA_MAP_PATH", "./artwork_metadata_map.json"))

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
    placement_scale: float = 1.0
    placement_x: float = 0.5
    placement_y: float = 0.5
    placement_angle: float = 0.0
    artwork_fit_mode: str = "contain"
    max_upscale_factor: Optional[float] = None


@dataclass
class PlacementTransform:
    scale: float
    x: float
    y: float
    angle: float


@dataclass
class SourceHygieneOptions:
    filter_preview_assets: bool = True
    min_source_width: int = 1
    min_source_height: int = 1


def normalize_printify_transform(transform: PlacementTransform) -> Dict[str, Any]:
    normalized_scale = float(transform.scale)
    normalized_x = float(transform.x)
    normalized_y = float(transform.y)
    normalized_angle = int(round(float(transform.angle)))
    if normalized_angle != transform.angle:
        logger.debug(
            "Normalized Printify transform angle from %s to integer %s",
            transform.angle,
            normalized_angle,
        )
    return {
        "scale": normalized_scale,
        "x": normalized_x,
        "y": normalized_y,
        "angle": normalized_angle,
    }


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
    trim_artwork_bounds: bool = False
    trim_artwork_bounds_for_shirts: bool = False
    trim_bounds_preset: Optional[str] = None
    trim_bounds_min_alpha: int = 8
    trim_bounds_padding_pct: float = 0.015
    trim_bounds_min_reduction_pct: float = 0.01
    aggressive_subject_trim_for_shirts: bool = False
    aggressive_subject_trim_mode: Optional[str] = None
    shirt_subject_fill_target: Optional[float] = None
    seo_keywords: List[str] = field(default_factory=list)
    audience: Optional[str] = None
    product_type_label: Optional[str] = None
    style_keywords: List[str] = field(default_factory=list)
    max_upscale_factor: Optional[float] = None
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
    launch_plan_row: str = ""
    launch_plan_row_id: str = ""


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
    source_size: str = ""
    trimmed_bounds_size: str = ""
    trim_skip_reason: str = ""
    exported_canvas_size: str = ""
    placement_scale_used: str = ""
    effective_upscale_factor: str = ""
    requested_upscale_factor: str = ""
    applied_upscale_factor: str = ""
    upscale_capped: bool = False
    orientation_bucket: str = ""
    launch_plan_row: str = ""
    launch_plan_row_id: str = ""
    collection_handle: str = ""
    collection_title: str = ""
    collection_description: str = ""
    launch_name: str = ""
    campaign: str = ""
    merch_theme: str = ""
    subject_bounds_before_aggressive_trim: str = ""
    subject_bounds_after_aggressive_trim: str = ""
    subject_fill_target: str = ""
    aggressive_trim_used: bool = False
    collection_sync_attempted: bool = False
    collection_sync_status: str = ""
    shopify_collection_id: str = ""
    collection_membership_verified: bool = False
    collection_warning: str = ""
    collection_error: str = ""


@dataclass
class StorefrontQaRow:
    artwork_filename: str
    artwork_slug: str
    template_key: str
    title: str
    title_source: str
    title_quality: str
    title_warnings: str
    description_preview: str
    description_warnings: str
    tags_preview: str
    tag_count: int
    tag_warnings: str
    blueprint_id: int
    provider_id: int
    enabled_variant_count: int
    option_names: str
    sale_price_min: str
    sale_price_max: str
    compare_at_min: str
    compare_at_max: str
    pricing_warnings: str
    compare_at_valid: bool
    publish_images: bool
    publish_mockups: str
    mockup_warnings: str
    placement_preview_context: str
    qa_status: str
    qa_warning_count: int
    qa_error_count: int
    recommended_action: str
    launch_plan_row: str = ""
    launch_plan_row_id: str = ""
    collection_handle: str = ""
    collection_title: str = ""
    campaign: str = ""
    merch_theme: str = ""


@dataclass
class LaunchPlanRow:
    row_number: int
    row_id: str
    artwork_file: str
    template_key: str
    overrides: Dict[str, str] = field(default_factory=dict)
    collection_handle: str = ""
    collection_title: str = ""
    collection_description: str = ""
    launch_name: str = ""
    campaign: str = ""
    merch_theme: str = ""


@dataclass
class PreparedArtwork:
    artwork: Artwork
    template: ProductTemplate
    placement: PlacementRequirement
    export_path: pathlib.Path
    width_px: int
    height_px: int
    source_size: Tuple[int, int] = (0, 0)
    trimmed_size: Optional[Tuple[int, int]] = None
    exported_canvas_size: Tuple[int, int] = (0, 0)
    upscaled: bool = False
    effective_upscale_factor: float = 1.0
    requested_upscale_factor: float = 1.0
    applied_upscale_factor: float = 1.0
    upscale_capped: bool = False
    trim_applied: bool = False
    trim_skip_reason: Optional[str] = None
    subject_bounds_before_aggressive_trim: Optional[Tuple[int, int, int, int]] = None
    subject_bounds_after_aggressive_trim: Optional[Tuple[int, int, int, int]] = None
    subject_fill_target: Optional[float] = None
    aggressive_trim_used: bool = False


@dataclass
class ArtworkProcessingOptions:
    allow_upscale: bool = False
    upscale_method: str = "lanczos"
    skip_undersized: bool = False
    placement_preview: bool = False
    preview_dir: pathlib.Path = pathlib.Path("exports/previews")


@dataclass
class ArtworkResolution:
    image: Image.Image
    action: str
    upscaled: bool
    original_size: Tuple[int, int]
    trimmed_size: Optional[Tuple[int, int]]
    final_size: Tuple[int, int]
    resized_size: Tuple[int, int]
    requested_upscale_factor: float = 1.0
    applied_upscale_factor: float = 1.0
    upscale_capped: bool = False
    effective_upscale_factor: float = 1.0
    trim_bounds_pct: Optional[Tuple[float, float]] = None
    trim_applied: bool = False
    trim_skip_reason: Optional[str] = None
    subject_bounds_before_aggressive_trim: Optional[Tuple[int, int, int, int]] = None
    subject_bounds_after_aggressive_trim: Optional[Tuple[int, int, int, int]] = None
    subject_fill_target: Optional[float] = None
    aggressive_trim_used: bool = False


@dataclass
class TrimBoundsResult:
    image: Image.Image
    trimmed_size: Optional[Tuple[int, int]]
    applied: bool
    skip_reason: Optional[str] = None
    subject_bounds_before: Optional[Tuple[int, int, int, int]] = None
    subject_bounds_after: Optional[Tuple[int, int, int, int]] = None


TRIM_PRESETS: Dict[str, Dict[str, float]] = {
    "conservative": {
        "trim_bounds_min_alpha": 16,
        "trim_bounds_padding_pct": 0.02,
        "trim_bounds_min_reduction_pct": 0.03,
    },
    "normal": {
        "trim_bounds_min_alpha": 8,
        "trim_bounds_padding_pct": 0.015,
        "trim_bounds_min_reduction_pct": 0.01,
    },
    "aggressive": {
        "trim_bounds_min_alpha": 1,
        "trim_bounds_padding_pct": 0.005,
        "trim_bounds_min_reduction_pct": 0.002,
    },
}

POSTER_SAFE_ENHANCEMENT_MAX_UPSCALE_FACTOR = 1.35
POSTER_SAFE_ENHANCEMENT_MIN_SOURCE_RATIO = 0.45


LAUNCH_PLAN_OVERRIDE_COLUMNS = [
    "title_override",
    "description_override",
    "tags_override",
    "audience_override",
    "style_keywords_override",
    "seo_keywords_override",
    "base_price_override",
    "markup_type_override",
    "markup_value_override",
    "compare_at_price_override",
    "publish_after_create_override",
    "collection_handle",
    "collection_title",
    "collection_description",
    "launch_name",
    "campaign",
    "merch_theme",
]


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
    return content_engine._normalize_title_tokens(value)


def filename_slug_to_title(value: str) -> str:
    return content_engine.filename_slug_to_title(value)



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


def load_artwork_metadata_map(mapping_path: pathlib.Path) -> Dict[str, Dict[str, Any]]:
    if not mapping_path.exists() or not mapping_path.is_file():
        return {}
    try:
        payload = json.loads(mapping_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Unable to parse artwork metadata map %s: %s", mapping_path, exc)
        return {}
    if not isinstance(payload, dict):
        logger.warning("Ignoring non-object artwork metadata map %s", mapping_path)
        return {}

    resolved: Dict[str, Dict[str, Any]] = {}
    for key, raw_entry in payload.items():
        if not isinstance(raw_entry, dict):
            continue
        normalized_key = slugify(str(key))
        if not normalized_key:
            continue
        title = str(raw_entry.get("title") or raw_entry.get("art_title") or "").strip()
        description = str(raw_entry.get("description") or raw_entry.get("short_description") or "").strip()
        tags = _split_keywords(raw_entry.get("tags"))
        subject = str(raw_entry.get("subject") or "").strip()
        mood = str(raw_entry.get("mood") or "").strip()
        aliases = [slugify(alias) for alias in _split_keywords(raw_entry.get("aliases")) if slugify(alias)]
        original_slug = str(raw_entry.get("original_slug") or "").strip()
        if original_slug:
            normalized_original_slug = slugify(original_slug)
            if normalized_original_slug:
                aliases.append(normalized_original_slug)
        entry = {
            "title": title,
            "description": description,
            "tags": tags,
            "subject": subject,
            "mood": mood,
            "theme": str(raw_entry.get("theme") or mood or "").strip(),
            "aliases": list(dict.fromkeys(aliases)),
            "original_slug": original_slug,
        }
        resolved[normalized_key] = entry
    return resolved


def _metadata_alias_candidates(entry: Dict[str, Any]) -> set[str]:
    candidates: set[str] = set()
    for alias in _split_keywords(entry.get("aliases")):
        normalized = slugify(alias)
        if normalized:
            candidates.add(normalized)
    original_slug = str(entry.get("original_slug") or "").strip()
    if original_slug:
        normalized = slugify(original_slug)
        if normalized:
            candidates.add(normalized)
    return candidates


def resolve_artwork_metadata_with_source(
    path: pathlib.Path,
    metadata_map: Dict[str, Dict[str, Any]],
    *,
    artwork_slug: str = "",
    persisted_aliases: Optional[List[str]] = None,
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    sidecar_path = path.with_suffix(".json")
    if sidecar_path.exists() and sidecar_path.is_file():
        return load_artwork_metadata(sidecar_path), {"source": "sidecar", "key": sidecar_path.name}

    def _lookup(candidate: str) -> Optional[Tuple[Dict[str, Any], str]]:
        normalized = slugify(candidate)
        if not normalized:
            return None
        entry = metadata_map.get(candidate) or metadata_map.get(normalized)
        if entry:
            canonical_key = normalized if metadata_map.get(normalized) else candidate
            return dict(entry), canonical_key
        return None

    canonical_slug = slugify(artwork_slug) if artwork_slug else ""
    if canonical_slug:
        matched = _lookup(canonical_slug)
        if matched:
            metadata, key = matched
            return metadata, {"source": "slug", "key": key}

    filename_stem = path.stem
    exact_stem_match = metadata_map.get(filename_stem)
    if isinstance(exact_stem_match, dict):
        return dict(exact_stem_match), {"source": "stem", "key": filename_stem}

    normalized_stem = slugify(filename_stem)
    if normalized_stem:
        matched = _lookup(normalized_stem)
        if matched:
            metadata, key = matched
            return metadata, {"source": "normalized_stem", "key": key}

    alias_values = [filename_stem, canonical_slug]
    if persisted_aliases:
        alias_values.extend([slugify(alias) for alias in persisted_aliases if slugify(alias)])
    normalized_aliases = {slugify(value) for value in alias_values if slugify(value)}
    if normalized_aliases:
        for key, entry in metadata_map.items():
            if not isinstance(entry, dict):
                continue
            entry_aliases = _metadata_alias_candidates(entry)
            if entry_aliases.intersection(normalized_aliases):
                return dict(entry), {"source": "alias", "key": key}

    fallback_key = normalized_stem or canonical_slug or slugify(path.name) or "unknown"
    return {}, {"source": "fallback", "key": fallback_key}


def resolve_artwork_metadata_for_path(path: pathlib.Path, metadata_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    metadata, _ = resolve_artwork_metadata_with_source(path, metadata_map)
    return metadata


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


def _is_weak_title_phrase(value: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    if not normalized:
        return True
    weak_phrases = {
        "signature",
        "signature product",
        "untitled",
        "untitled design",
        "product",
        "design",
        "artwork",
    }
    if normalized in weak_phrases:
        return True
    tokens = [token for token in normalized.split() if token]
    return len(tokens) <= 1 and tokens[0] in {"signature", "product", "design"} if tokens else True


def _select_fallback_title_phrase(*, artwork: Artwork, quality_reason: str) -> str:
    metadata = artwork.metadata or {}
    candidates: List[str] = []
    for key in ("title", "subtitle", "theme", "collection", "occasion", "color_story"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            candidates.append(value.strip())
    style_keywords = _split_keywords(metadata.get("style_keywords"))
    seo_keywords = _split_keywords(metadata.get("seo_keywords"))
    if style_keywords:
        candidates.append(style_keywords[0])
    if seo_keywords:
        candidates.append(seo_keywords[0])
    cleaned_slug_title = filename_slug_to_title(artwork.slug)
    if cleaned_slug_title and cleaned_slug_title != "Untitled Design":
        candidates.append(cleaned_slug_title)
    for candidate in candidates:
        cleaned = re.sub(r"\s+", " ", candidate).strip().title()
        if cleaned and not _is_weak_title_phrase(cleaned):
            return cleaned
    if quality_reason not in {"uuid_like", "hex_like", "long_numeric", "hashy_slug"}:
        filename_clean = filename_slug_to_title(artwork.src_path.stem or artwork.slug)
        if filename_clean != "Untitled Design":
            return filename_clean
    return "Signature"


def resolve_artwork_title(template: ProductTemplate, artwork: Artwork) -> TitleResolution:
    metadata_title = str(artwork.metadata.get("title", "")).strip()
    if metadata_title and not _is_weak_title_phrase(metadata_title):
        return TitleResolution(metadata_title, metadata_title, "metadata", "metadata_title")

    filename_stem = artwork.src_path.stem or artwork.slug
    quality_reason = filename_title_quality_reason(filename_stem)
    cleaned = filename_slug_to_title(filename_stem)
    if quality_reason == "ok":
        return TitleResolution(filename_stem, cleaned, "filename", "filename_clean")

    product_label = (template.product_type_label or template.shopify_product_type or content_engine.family_title_suffix(template) or "Product").strip()
    base_title = _select_fallback_title_phrase(artwork=artwork, quality_reason=quality_reason)
    fallback = base_title.strip()
    if product_label and not title_semantically_includes_product_label(fallback, product_label):
        fallback = f"{fallback} {product_label}".strip()
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
    return content_engine.choose_artwork_display_title(artwork)


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
            "placements": [
                {
                    "placement_name": "front",
                    "width_px": 2700,
                    "height_px": 1120,
                    "allow_upscale": False,
                    "placement_scale": 0.78,
                    "placement_x": 0.5,
                    "placement_y": 0.5,
                    "placement_angle": 0,
                    "artwork_fit_mode": "contain",
                }
            ],
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
    context = content_engine.build_listing_context(template, artwork)
    merged_style = [*template.style_keywords, *_split_keywords((artwork.metadata or {}).get("style_keywords"))]
    product_type = (template.product_type_label or template.shopify_product_type or context.get("family_label") or "Product").strip()
    context.update(
        {
            "product_type_label": product_type,
            "style_keywords": ", ".join(list(dict.fromkeys(merged_style))[:6]),
            "family_label": content_engine.family_title_suffix(template),
        }
    )
    return context


def render_product_title(template: ProductTemplate, artwork: Artwork) -> str:
    context = build_seo_context(template, artwork)
    product_label = context.get("product_type_label", "")
    if product_label and title_semantically_includes_product_label(context.get("artwork_title", ""), product_label):
        context = dict(context)
        context["product_type_label"] = ""
    rendered = template.title_pattern.format(**context).strip()
    deduped = _dedupe_rendered_title(rendered)
    if deduped.lower() == context.get("artwork_title", "").strip().lower():
        family = content_engine.infer_product_family(template)
        suffix = content_engine.family_title_suffix(template) if family != "default" else ""
        if suffix and not title_semantically_includes_product_label(deduped, suffix):
            deduped = f"{deduped} {suffix}".strip()
    return _dedupe_rendered_title(deduped)


def _render_listing_tags(template: ProductTemplate, artwork: Artwork) -> List[str]:
    metadata = artwork.metadata or {}
    context = build_seo_context(template, artwork)
    family = content_engine.infer_product_family(template)
    family_label = str(context.get("family_label", "")).strip().lower()
    family_bucket = [*content_engine.family_tags(template), family_label, template.product_type_label, template.shopify_product_type]
    theme_bucket = [
        metadata.get("theme"),
        metadata.get("subtitle"),
        metadata.get("collection"),
        metadata.get("color_story"),
        context.get("artwork_title"),
        *template.tags,
        *artwork.tags,
        *_split_keywords(metadata.get("tags")),
        *_split_keywords(metadata.get("seo_keywords")),
    ]
    audience_style_bucket = [
        metadata.get("audience"),
        *template.style_keywords,
        *_split_keywords(metadata.get("style_keywords")),
    ]
    gifting_bucket = [metadata.get("occasion"), "gift idea", "inkvibe", *DEFAULT_TAGS]
    bucket_order = [family_bucket, theme_bucket, audience_style_bucket, gifting_bucket]
    tags: List[str] = []
    seen: set[str] = set()
    generic_tokens = {"print-on-demand", "printify", "style", "design", "artwork", "product"}
    for bucket in bucket_order:
        for row in bucket:
            cleaned = str(row).strip().lower()
            if not cleaned:
                continue
            cleaned = re.sub(r"\s+", " ", cleaned)
            cleaned = re.sub(r"[^a-z0-9&' +/\-]", "", cleaned).strip()
            if not cleaned or len(cleaned) > 32:
                continue
            if cleaned in seen:
                continue
            if cleaned in generic_tokens and len(tags) >= 8:
                continue
            seen.add(cleaned)
            tags.append(cleaned)
            if len(tags) >= 20:
                break
        if len(tags) >= 20:
            break
    if family in {"hoodie", "sweatshirt", "long_sleeve", "poster", "mug", "tote"}:
        required = set(content_engine.family_tags(template))
        if required and not required.intersection(set(tags)):
            for tag in content_engine.family_tags(template):
                lowered = tag.lower()
                if lowered not in seen and len(lowered) <= 32:
                    tags.append(lowered)
                    seen.add(lowered)
                    break
    return tags


def render_product_description(template: ProductTemplate, artwork: Artwork) -> str:
    context = build_seo_context(template, artwork)
    generated = content_engine.build_branded_description(
        artwork_title=context["artwork_title"],
        short_description=str((artwork.metadata or {}).get("description", "")).strip(),
        template=template,
    )
    pattern = (template.description_pattern or "").strip()
    metadata = artwork.metadata or {}
    metadata_description = str(metadata.get("description", "")).strip()

    if not pattern or pattern in {"{artwork_title}", "<p>{artwork_title}</p>"}:
        if metadata_description:
            return generated.strip()
        details: List[str] = []
        family_label = str(context.get("family_label") or context.get("product_type_label") or "product").strip()
        theme = str(context.get("theme") or "").strip()
        subtitle = str(context.get("subtitle") or "").strip()
        collection = str(context.get("collection") or "").strip()
        occasion = str(context.get("occasion") or "").strip()
        color_story = str(context.get("color_story") or "").strip()
        audience = str(context.get("audience") or "").strip()
        style_keywords = str(context.get("style_keywords") or "").strip()
        artist_note = str(context.get("artist_note") or "").strip()
        if subtitle:
            details.append(f"<p>{subtitle}</p>")
        details.append(f"<p>This {family_label.lower()} features the <strong>{context['artwork_title']}</strong> design for an easy, everyday statement.</p>")
        metadata_signals = [signal for signal in (theme, collection, color_story) if signal]
        if metadata_signals:
            details.append(f"<p>Inspired by {', '.join(metadata_signals[:3])}.</p>")
        bullet_rows: List[str] = []
        if occasion:
            bullet_rows.append(f"Great for {occasion}")
        if audience:
            bullet_rows.append(f"Made for {audience}")
        if style_keywords:
            bullet_rows.append(f"Style mood: {style_keywords}")
        if bullet_rows:
            details.append("<ul>" + "".join(f"<li>{row}</li>" for row in bullet_rows[:3]) + "</ul>")
        if artist_note:
            details.append(f"<p>Artist note: {artist_note}</p>")
        return f"{generated}{''.join(details)}".strip()

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
    return state_store.ensure_state_shape(state)


def derive_state_index(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return state_store.derive_state_index(state)


def list_state_keys(state: Dict[str, Any]) -> List[str]:
    return state_store.list_state_keys(state)


def inspect_state_key(state: Dict[str, Any], state_key: str) -> Optional[Dict[str, Any]]:
    return state_store.inspect_state_key(state, state_key)


def _row_status(row: Dict[str, Any]) -> str:
    return state_store.row_status(row)


def latest_rows_by_state_key(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return state_store.latest_rows_by_state_key(state)


def is_state_key_successful(state: Dict[str, Any], state_key: str) -> bool:
    return state_store.is_state_key_successful(state, state_key)


def row_completion_label(row: Dict[str, Any]) -> str:
    return state_store.row_completion_label(row)


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


def _is_truthy_csv(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in {"1", "true", "yes", "y", "on"}


def _split_csv_keywords(value: str) -> List[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def _resolve_artwork_path_for_launch_plan(artwork_file: str, image_dir: pathlib.Path) -> pathlib.Path:
    value = str(artwork_file or "").strip()
    if not value:
        raise ValueError("artwork_file is required")
    candidate = pathlib.Path(value)
    if candidate.is_file():
        return candidate
    image_candidate = image_dir / value
    if image_candidate.is_file():
        return image_candidate
    raise ValueError(f"artwork_file not found: {value} (checked as provided and under {image_dir})")


def parse_launch_plan_csv(path: pathlib.Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("Launch plan CSV is missing headers")
        rows: List[Dict[str, str]] = []
        for row in reader:
            normalized = {str(k or "").strip(): str(v or "").strip() for k, v in row.items()}
            rows.append(normalized)
    return rows


def build_resolved_template(base_template: ProductTemplate, overrides: Dict[str, str]) -> ProductTemplate:
    resolved = replace(base_template)
    if overrides.get("title_override"):
        resolved.title_pattern = overrides["title_override"]
    if overrides.get("description_override"):
        resolved.description_pattern = overrides["description_override"]
    if overrides.get("tags_override"):
        resolved.tags = _split_csv_keywords(overrides["tags_override"])
    if overrides.get("audience_override"):
        resolved.audience = overrides["audience_override"]
    if overrides.get("style_keywords_override"):
        resolved.style_keywords = _split_csv_keywords(overrides["style_keywords_override"])
    if overrides.get("seo_keywords_override"):
        resolved.seo_keywords = _split_csv_keywords(overrides["seo_keywords_override"])
    if overrides.get("base_price_override"):
        resolved.base_price = overrides["base_price_override"]
    if overrides.get("markup_type_override"):
        resolved.markup_type = overrides["markup_type_override"]
    if overrides.get("markup_value_override"):
        resolved.markup_value = overrides["markup_value_override"]
    if overrides.get("compare_at_price_override"):
        resolved.compare_at_price = overrides["compare_at_price_override"]
    if overrides.get("publish_after_create_override"):
        resolved.publish_after_create = _is_truthy_csv(overrides["publish_after_create_override"])
    return resolved


def _blank_launch_plan_override_row() -> Dict[str, str]:
    return {column: "" for column in LAUNCH_PLAN_OVERRIDE_COLUMNS}


def write_launch_plan_template(path: pathlib.Path) -> None:
    rows = [
        {
            "artwork_file": "tee-artwork.png",
            "template_key": "tshirt_gildan",
            "enabled": "true",
            **_blank_launch_plan_override_row(),
            "row_id": "tee-example-1",
        },
        {
            "artwork_file": "mug-artwork.png",
            "template_key": "mug_11oz",
            "enabled": "true",
            **_blank_launch_plan_override_row(),
            "row_id": "mug-example-1",
        },
        {
            "artwork_file": "disabled-example.png",
            "template_key": "tshirt_gildan",
            "enabled": "false",
            **_blank_launch_plan_override_row(),
            "row_id": "disabled-example",
        },
    ]
    write_csv_report(path, rows)


def _orientation_bucket(width: int, height: int) -> str:
    if width <= 0 or height <= 0:
        return "square"
    ratio = width / height
    if ratio >= 1.2:
        return "landscape"
    if ratio <= (1 / 1.2):
        return "portrait"
    return "square"


def _is_shirt_template_key(template_key: str) -> bool:
    key = str(template_key or "").lower()
    return any(token in key for token in ["shirt", "tee", "hoodie", "sweatshirt"])


def compute_placement_transform_for_artwork(
    placement: PlacementRequirement,
    artwork: Artwork,
    template_key: str,
) -> PlacementTransform:
    orientation = _orientation_bucket(artwork.image_width, artwork.image_height)

    scale = placement.placement_scale

    if template_key == "tshirt_gildan":
        shirt_orientation_caps = {
            "portrait": 0.72,
            "square": 0.80,   # was 0.70
            "landscape": 0.66,
        }
        scale = min(scale, shirt_orientation_caps.get(orientation, scale))

    elif "mug" in str(template_key).lower() or "cup" in str(template_key).lower():
        mug_orientation_caps = {
            "portrait": 0.60,
            "square": 0.58,
            "landscape": 0.54,
        }
        scale = min(scale, mug_orientation_caps.get(orientation, scale))
    elif template_key == "poster_basic":
        poster_orientation_caps = {
            "portrait": 1.0,
            "square": 1.0,
            "landscape": 0.97,
        }
        scale = min(scale, poster_orientation_caps.get(orientation, scale))
        logger.info(
            "Poster transform strategy template=%s placement=%s orientation=%s strategy=tuned_scale scale=%.3f",
            template_key,
            placement.placement_name,
            orientation,
            scale,
        )

    return PlacementTransform(
        scale=scale,
        x=placement.placement_x,
        y=placement.placement_y,
        angle=placement.placement_angle,
    )



def export_launch_plan_from_images(
    *,
    path: pathlib.Path,
    image_dir: pathlib.Path,
    templates: List[ProductTemplate],
    include_disabled_template_rows: bool = False,
    default_enabled: bool = True,
) -> int:
    artworks = discover_artworks(image_dir)
    rows: List[Dict[str, str]] = []
    for artwork in artworks:
        rel_artwork = artwork.src_path.relative_to(image_dir)
        for template in templates:
            rows.append({
                "artwork_file": rel_artwork.as_posix(),
                "template_key": template.key,
                "enabled": "true" if default_enabled else "false",
                **_blank_launch_plan_override_row(),
                "row_id": slugify(f"{artwork.slug}-{template.key}"),
            })
            if include_disabled_template_rows:
                rows.append({
                    "artwork_file": rel_artwork.as_posix(),
                    "template_key": template.key,
                    "enabled": "false",
                    **_blank_launch_plan_override_row(),
                    "row_id": slugify(f"{artwork.slug}-{template.key}-disabled"),
                })
    write_csv_report(path, rows)
    return len(rows)


def resolve_launch_plan_rows(
    *,
    launch_plan_path: pathlib.Path,
    templates: List[ProductTemplate],
    image_dir: pathlib.Path,
) -> Tuple[List[LaunchPlanRow], List[FailureReportRow]]:
    template_map = {template.key: template for template in templates}
    launch_rows: List[LaunchPlanRow] = []
    failures: List[FailureReportRow] = []
    raw_rows = parse_launch_plan_csv(launch_plan_path)
    for idx, row in enumerate(raw_rows, start=2):
        row_id = str(row.get("row_id") or "").strip() or str(idx)
        enabled_value = str(row.get("enabled") or "").strip()
        if enabled_value and not _is_truthy_csv(enabled_value):
            continue
        artwork_file = str(row.get("artwork_file") or "").strip()
        template_key = str(row.get("template_key") or "").strip()
        try:
            if not template_key:
                raise ValueError("template_key is required")
            if template_key not in template_map:
                raise ValueError(f"Unknown template_key '{template_key}'")
            _resolve_artwork_path_for_launch_plan(artwork_file, image_dir)
            metadata_keys = {"collection_handle", "collection_title", "collection_description", "launch_name", "campaign", "merch_theme"}
            overrides = {
                key: str(row.get(key) or "").strip()
                for key in LAUNCH_PLAN_OVERRIDE_COLUMNS
                if key not in metadata_keys and str(row.get(key) or "").strip()
            }
            launch_rows.append(LaunchPlanRow(
                row_number=idx,
                row_id=row_id,
                artwork_file=artwork_file,
                template_key=template_key,
                overrides=overrides,
                collection_handle=str(row.get("collection_handle") or "").strip(),
                collection_title=str(row.get("collection_title") or "").strip(),
                collection_description=str(row.get("collection_description") or "").strip(),
                launch_name=str(row.get("launch_name") or "").strip(),
                campaign=str(row.get("campaign") or "").strip(),
                merch_theme=str(row.get("merch_theme") or "").strip(),
            ))
        except Exception as exc:
            failures.append(FailureReportRow(
                timestamp=datetime.now(timezone.utc).isoformat(),
                artwork_filename=artwork_file,
                artwork_slug=slugify(pathlib.Path(artwork_file).stem) if artwork_file else "",
                template_key=template_key,
                action_attempted="validate_launch_plan_row",
                blueprint_id=0,
                provider_id=0,
                upload_strategy="n/a",
                error_type=type(exc).__name__,
                error_message=str(exc),
                suggested_next_action="Fix launch-plan row and rerun.",
                launch_plan_row=str(idx),
                launch_plan_row_id=row_id,
            ))
    return launch_rows, failures


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

    def list_custom_collections(self, **params: Any) -> List[Dict[str, Any]]:
        response = self.get(f"/admin/api/{SHOPIFY_API_VERSION}/custom_collections.json", **params)
        return response.get("custom_collections", []) if isinstance(response, dict) else []

    def find_custom_collection(self, *, handle: str = "", title: str = "") -> Optional[Dict[str, Any]]:
        normalized_handle = handle.strip()
        normalized_title = title.strip()
        if normalized_handle:
            by_handle = self.list_custom_collections(handle=normalized_handle, limit=1)
            if by_handle:
                return by_handle[0]
        if normalized_title:
            by_title = self.list_custom_collections(title=normalized_title, limit=25)
            for row in by_title:
                if str(row.get("title") or "").strip().casefold() == normalized_title.casefold():
                    return row
            if by_title:
                return by_title[0]
        return None

    def create_custom_collection(self, *, handle: str, title: str, description: str = "") -> Dict[str, Any]:
        payload = {
            "custom_collection": {
                "handle": handle,
                "title": title,
                "body_html": description,
            }
        }
        response = self.post(f"/admin/api/{SHOPIFY_API_VERSION}/custom_collections.json", payload)
        return response.get("custom_collection", {}) if isinstance(response, dict) else {}

    def update_custom_collection(self, *, collection_id: int, title: str, description: str = "") -> Dict[str, Any]:
        payload = {"custom_collection": {"id": collection_id, "title": title, "body_html": description}}
        response = self.put(f"/admin/api/{SHOPIFY_API_VERSION}/custom_collections/{collection_id}.json", payload)
        return response.get("custom_collection", {}) if isinstance(response, dict) else {}

    def list_collects(self, **params: Any) -> List[Dict[str, Any]]:
        response = self.get(f"/admin/api/{SHOPIFY_API_VERSION}/collects.json", **params)
        return response.get("collects", []) if isinstance(response, dict) else []

    def add_product_to_collection(self, *, collection_id: int, product_id: int) -> Dict[str, Any]:
        payload = {"collect": {"collection_id": collection_id, "product_id": product_id}}
        response = self.post(f"/admin/api/{SHOPIFY_API_VERSION}/collects.json", payload)
        return response.get("collect", {}) if isinstance(response, dict) else {}

    def is_product_in_collection(self, *, collection_id: int, product_id: int) -> bool:
        return bool(self.list_collects(collection_id=collection_id, product_id=product_id, limit=1))


def _extract_numeric_shopify_id(value: Any) -> Optional[int]:
    if isinstance(value, int):
        return value
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.isdigit():
        return int(candidate)
    match = re.search(r"/(\d+)$", candidate)
    if match:
        return int(match.group(1))
    return None


def sync_shopify_collection(
    *,
    shopify: Optional[ShopifyClient],
    shopify_product_id: str,
    collection_handle: str,
    collection_title: str,
    collection_description: str,
    verify_membership: bool,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "collection_sync_attempted": False,
        "collection_sync_status": "skipped",
        "collection_id": "",
        "collection_handle": collection_handle.strip(),
        "collection_title": collection_title.strip(),
        "collection_membership_verified": False,
        "collection_warning": "",
        "collection_error": "",
    }
    normalized_handle = collection_handle.strip()
    normalized_title = collection_title.strip()
    normalized_description = collection_description.strip()

    if not normalized_handle and not normalized_title:
        result["collection_sync_status"] = "skipped_no_collection_metadata"
        result["collection_warning"] = "No collection_handle or collection_title provided"
        logger.info("Collection sync skipped: no collection metadata provided")
        return result

    if shopify is None:
        result["collection_sync_status"] = "skipped_no_shopify_client"
        result["collection_warning"] = "SHOPIFY_ADMIN_TOKEN missing; collection sync not available"
        logger.warning("Collection sync skipped: Shopify client unavailable")
        return result

    numeric_product_id = _extract_numeric_shopify_id(shopify_product_id)
    if numeric_product_id is None:
        result["collection_sync_status"] = "skipped_no_shopify_product_id"
        result["collection_warning"] = f"Unable to resolve Shopify product id from {shopify_product_id!r}"
        logger.warning("Collection sync skipped: Shopify product id unavailable product_id=%s", shopify_product_id or "-")
        return result

    result["collection_sync_attempted"] = True
    resolved_handle = normalized_handle or slugify(normalized_title)
    resolved_title = normalized_title or normalized_handle.replace("-", " ").title()

    try:
        collection = shopify.find_custom_collection(handle=resolved_handle, title=resolved_title)
        created = False
        if collection:
            logger.info("Collection resolved handle=%s title=%s id=%s", resolved_handle, resolved_title, collection.get("id"))
        if not collection:
            try:
                collection = shopify.create_custom_collection(handle=resolved_handle, title=resolved_title, description=normalized_description)
                created = True
                logger.info("Collection created handle=%s title=%s id=%s", resolved_handle, resolved_title, collection.get("id"))
            except DryRunMutationSkipped:
                result["collection_sync_status"] = "dry-run"
                logger.info("Collection sync dry-run: create skipped handle=%s", resolved_handle)
                return result

        collection_id = _extract_numeric_shopify_id(collection.get("id")) if isinstance(collection, dict) else None
        if collection_id is None:
            raise RuntimeError(f"Resolved collection is missing id: {collection}")

        existing_title = str(collection.get("title") or "").strip()
        existing_description = str(collection.get("body_html") or "").strip()
        needs_update = bool(
            (resolved_title and existing_title != resolved_title) or
            (normalized_description and existing_description != normalized_description)
        )
        if needs_update:
            try:
                collection = shopify.update_custom_collection(
                    collection_id=collection_id,
                    title=resolved_title,
                    description=normalized_description,
                )
                logger.info("Collection updated id=%s title=%s", collection_id, resolved_title)
            except DryRunMutationSkipped:
                result["collection_sync_status"] = "dry-run"
                logger.info("Collection sync dry-run: update skipped id=%s", collection_id)
                return result

        if not shopify.is_product_in_collection(collection_id=collection_id, product_id=numeric_product_id):
            try:
                shopify.add_product_to_collection(collection_id=collection_id, product_id=numeric_product_id)
                logger.info("Product attached to collection product_id=%s collection_id=%s", numeric_product_id, collection_id)
            except DryRunMutationSkipped:
                result["collection_sync_status"] = "dry-run"
                logger.info("Collection sync dry-run: collect create skipped collection_id=%s", collection_id)
                return result
        else:
            logger.info("Collection membership already exists product_id=%s collection_id=%s", numeric_product_id, collection_id)

        result["collection_sync_status"] = "created" if created else "synced"
        result["collection_id"] = str(collection_id)
        result["collection_handle"] = str(collection.get("handle") or resolved_handle)
        result["collection_title"] = str(collection.get("title") or resolved_title)

        if verify_membership:
            result["collection_membership_verified"] = shopify.is_product_in_collection(
                collection_id=collection_id,
                product_id=numeric_product_id,
            )
            logger.info(
                "Collection verification result product_id=%s collection_id=%s verified=%s",
                numeric_product_id,
                collection_id,
                result["collection_membership_verified"],
            )
    except Exception as exc:
        result["collection_sync_status"] = "error"
        result["collection_error"] = str(exc)
        logger.warning("Collection sync failed product_id=%s handle=%s error=%s", numeric_product_id, resolved_handle, exc)
    return result


def _variant_option_value(variant: Dict[str, Any], key: str) -> str:
    options = variant.get("options") or {}
    if isinstance(options, dict):
        return str(options.get(key, "")).strip()
    return str(variant.get(key, "")).strip()


def _shopify_money_string_from_minor(minor_units: int) -> str:
    return f"{Decimal(minor_units) / Decimal('100'):.2f}"


def build_shopify_product_options(template: ProductTemplate, variant_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
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

        sale_price_minor = compute_sale_price_minor(template, variant)
        compare_at_minor = compute_compare_at_price_minor(template, sale_price_minor)

        variant_payload = {
            "optionValues": option_values,
            "price": _shopify_money_string_from_minor(sale_price_minor),
            "inventoryPolicy": "CONTINUE",
            "taxable": True,
            "inventoryItem": {"tracked": False},
        }
        if compare_at_minor is not None:
            variant_payload["compareAtPrice"] = _shopify_money_string_from_minor(compare_at_minor)
        variants.append(variant_payload)

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
    product_options, variants = build_shopify_product_options(template, variant_rows)
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


def discover_artworks(
    image_dir: pathlib.Path,
    *,
    candidate_paths: Optional[List[pathlib.Path]] = None,
    source_hygiene: Optional[SourceHygieneOptions] = None,
) -> List[Artwork]:
    supported = {".png", ".jpg", ".jpeg", ".webp"}
    artworks: List[Artwork] = []
    metadata_map = load_artwork_metadata_map(ARTWORK_METADATA_MAP_PATH)
    hygiene = source_hygiene or SourceHygieneOptions()

    path_iterable = sorted(candidate_paths) if candidate_paths is not None else sorted(image_dir.glob("**/*"))
    for path in path_iterable:
        if path.suffix.lower() not in supported or not path.is_file():
            continue
        if hygiene.filter_preview_assets and is_preview_or_low_value_asset(path):
            logger.info("Skipping artwork source due to preview/thumbnail naming: %s", path.name)
            continue

        with Image.open(path) as im:
            width, height = im.size
        if width < hygiene.min_source_width or height < hygiene.min_source_height:
            logger.info(
                "Skipping artwork source due to tiny dimensions image=%s size=%sx%s min=%sx%s",
                path.name,
                width,
                height,
                hygiene.min_source_width,
                hygiene.min_source_height,
            )
            continue

        slug = slugify(path.stem)
        metadata, match_info = resolve_artwork_metadata_with_source(path, metadata_map, artwork_slug=slug)
        logger.info(
            "Content metadata match artwork=%s source=%s key=%s",
            path.name,
            match_info.get("source", "unknown"),
            match_info.get("key", ""),
        )
        title = str(metadata.get("title", "")).strip() or filename_slug_to_title(path.stem)
        artworks.append(
            Artwork(
                slug=slug,
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


def run_artwork_metadata_generation(
    *,
    image_dir: pathlib.Path,
    metadata_preview: bool,
    write_sidecars: bool,
    overwrite_sidecars: bool,
    metadata_only_missing: bool,
    metadata_max_artworks: int,
    metadata_output_dir: str,
    metadata_generator: str = MetadataGeneratorMode.HEURISTIC.value,
    metadata_openai_model: str = "",
    metadata_openai_timeout: float = 30.0,
    metadata_auto_approve: bool = False,
    metadata_min_confidence: float = 0.9,
    metadata_review_report: str = "",
    metadata_review_json: str = "",
    metadata_write_auto_approved_only: bool = False,
    metadata_allow_review_writes: bool = False,
    artwork_paths: Optional[List[pathlib.Path]] = None,
) -> None:
    generator = select_artwork_metadata_generator(
        mode=metadata_generator,
        openai_model=metadata_openai_model,
        openai_timeout_seconds=metadata_openai_timeout,
    )
    resolved_artwork_paths = artwork_paths[:] if artwork_paths is not None else discover_artwork_images(image_dir)
    if metadata_max_artworks > 0:
        resolved_artwork_paths = resolved_artwork_paths[:metadata_max_artworks]
    output_dir = pathlib.Path(metadata_output_dir) if metadata_output_dir else None

    candidates = [generator.generate_metadata_for_artwork(path) for path in resolved_artwork_paths]
    approval_mode_enabled = bool(
        metadata_auto_approve
        or metadata_write_auto_approved_only
        or metadata_review_report
        or metadata_review_json
        or metadata_allow_review_writes
    )
    review_decisions: Dict[str, MetadataReviewDecision] = {}
    for candidate in candidates:
        decision = evaluate_generated_metadata(candidate, min_confidence=metadata_min_confidence)
        review_decisions[str(candidate.image_path)] = decision
    if metadata_preview or not write_sidecars:
        preview_text = preview_generated_metadata(candidates)
        if approval_mode_enabled and preview_text:
            lines: List[str] = []
            for line in preview_text.splitlines():
                lines.append(line)
                if line.startswith("[") and " -> " in line:
                    image_name = line.split("] ", 1)[-1].split(" -> ", 1)[0].strip()
                    candidate = next((item for item in candidates if item.image_path.name == image_name), None)
                    if candidate:
                        decision = review_decisions.get(str(candidate.image_path))
                        if decision:
                            reasons = ", ".join(decision.review_reasons) if decision.review_reasons else "none"
                            lines.append(
                                f"    approval_status: {decision.approval_status} "
                                f"(confidence={decision.confidence:.3f}, reasons={reasons})"
                            )
            preview_text = "\n".join(lines)
        if preview_text:
            print(preview_text)
        logger.info("Artwork metadata preview generated artworks=%s", len(candidates))

    review_rows: List[MetadataReviewRow] = []
    for candidate in candidates:
        review_rows.append(build_metadata_review_row(candidate, review_decisions[str(candidate.image_path)]))
    if metadata_review_report:
        export_metadata_review_csv(review_rows, pathlib.Path(metadata_review_report))
    if metadata_review_json:
        export_metadata_review_json(review_rows, pathlib.Path(metadata_review_json))

    if not write_sidecars:
        return

    write_count = 0
    skipped_count = 0
    review_skipped_count = 0
    rejected_count = 0
    auto_approved_count = 0
    for candidate in candidates:
        target_sidecar = candidate.sidecar_path if not output_dir else (output_dir / f"{candidate.image_path.stem}.json")
        decision = review_decisions[str(candidate.image_path)]
        enforce_auto_approved_only = metadata_auto_approve or metadata_write_auto_approved_only
        can_write_for_approval = True
        if approval_mode_enabled:
            if decision.approval_status == "auto_approved":
                auto_approved_count += 1
            if decision.approval_status == "rejected":
                rejected_count += 1
                can_write_for_approval = False
            elif enforce_auto_approved_only and not should_auto_approve_metadata(decision):
                can_write_for_approval = bool(metadata_allow_review_writes)
            elif decision.approval_status == "needs_review" and not metadata_allow_review_writes and metadata_auto_approve:
                can_write_for_approval = False
        if not can_write_for_approval:
            review_skipped_count += 1
            skipped_count += 1
            decision.would_write_sidecar = False
            continue
        if not should_write_sidecar(
            target_sidecar,
            overwrite_sidecars=overwrite_sidecars,
            only_missing=metadata_only_missing and not overwrite_sidecars,
        ):
            skipped_count += 1
            decision.would_write_sidecar = False
            continue
        write_artwork_sidecar(candidate=candidate, output_dir=output_dir)
        write_count += 1
        decision.would_write_sidecar = True

    if metadata_review_report or metadata_review_json:
        review_rows = [build_metadata_review_row(candidate, review_decisions[str(candidate.image_path)]) for candidate in candidates]
        if metadata_review_report:
            export_metadata_review_csv(review_rows, pathlib.Path(metadata_review_report))
        if metadata_review_json:
            export_metadata_review_json(review_rows, pathlib.Path(metadata_review_json))

    logger.info(
        "Artwork metadata generation complete generated=%s written=%s skipped=%s review_skipped=%s rejected=%s auto_approved=%s overwrite=%s only_missing=%s generator=%s",
        len(candidates),
        write_count,
        skipped_count,
        review_skipped_count,
        rejected_count,
        auto_approved_count,
        overwrite_sidecars,
        metadata_only_missing and not overwrite_sidecars,
        metadata_generator,
    )


def run_prompt_artwork_generation(
    *,
    request: ArtworkGenerationRequest,
    templates: List[ProductTemplate],
) -> List[pathlib.Path]:
    plan = plan_generated_artwork_targets(template_keys=[template.key for template in templates], target_mode=request.target_mode)
    for reason in plan.rationale:
        logger.info("Artwork generation plan: %s", reason)
    for target in plan.targets:
        logger.info("Artwork generation target mode=%s openai_size=%s", target.mode, target.openai_size)

    if request.dry_run_plan:
        return []

    generated_assets = generate_artwork_with_openai(request=request, plan=plan)
    hydrated_assets: List[GeneratedArtworkAsset] = []
    for asset in generated_assets:
        if not asset.path.exists():
            asset.skipped_reason = "file_missing_after_generation"
            logger.warning("Generated asset missing after generation path=%s", asset.path)
            continue
        with Image.open(asset.path) as im:
            asset.width, asset.height = im.size
        skipped_reason = validate_generated_asset_for_templates(
            asset,
            min_width=request.min_source_width,
            min_height=request.min_source_height,
        )
        if skipped_reason:
            asset.skipped_reason = skipped_reason
            logger.info("Skipping generated source image=%s reason=%s", asset.path.name, skipped_reason)
            continue
        hydrated_assets.append(asset)

    preferred_assets = choose_preferred_generated_asset(hydrated_assets)
    kept_paths = [asset.path for asset in preferred_assets]
    dropped_paths = sorted({asset.path for asset in hydrated_assets} - set(kept_paths))
    for dropped in dropped_paths:
        logger.info("Skipping generated source image=%s reason=duplicate_concept_preferred_master_selected", dropped.name)
    return kept_paths


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


def _trim_artwork_bounds(
    image: Image.Image,
    *,
    min_alpha: int = 8,
    padding_pct: float = 0.015,
    min_reduction_pct: float = 0.01,
) -> TrimBoundsResult:
    alpha = image.getchannel("A")
    if min_alpha > 1:
        alpha = alpha.point(lambda px: 255 if px >= min_alpha else 0)
    bbox = alpha.getbbox()
    alpha.close()
    if bbox is None:
        return TrimBoundsResult(image=image, trimmed_size=None, applied=False, skip_reason="no_meaningful_alpha_bounds")
    left, top, right, bottom = bbox
    pad = max(1, int(math.ceil(max(image.width, image.height) * max(0.0, padding_pct))))
    left = max(0, left - pad)
    top = max(0, top - pad)
    right = min(image.width, right + pad)
    bottom = min(image.height, bottom + pad)

    original_area = image.width * image.height
    trimmed_w = max(1, right - left)
    trimmed_h = max(1, bottom - top)
    trimmed_area = trimmed_w * trimmed_h
    reduction_pct = 0.0 if original_area <= 0 else max(0.0, 1.0 - (trimmed_area / original_area))

    if reduction_pct < max(0.0, min_reduction_pct):
        return TrimBoundsResult(
            image=image,
            trimmed_size=(image.width, image.height),
            applied=False,
            skip_reason="below_reduction_threshold",
        )

    if left == 0 and top == 0 and right == image.width and bottom == image.height:
        return TrimBoundsResult(
            image=image,
            trimmed_size=(image.width, image.height),
            applied=False,
            skip_reason="below_reduction_threshold",
        )
    trimmed = image.crop((left, top, right, bottom))
    image.close()
    return TrimBoundsResult(image=trimmed, trimmed_size=(trimmed.width, trimmed.height), applied=True)


def _compute_alpha_bbox(image: Image.Image, *, min_alpha: int = 1) -> Optional[Tuple[int, int, int, int]]:
    alpha = image.getchannel("A")
    if min_alpha > 1:
        alpha = alpha.point(lambda px: 255 if px >= min_alpha else 0)
    bbox = alpha.getbbox()
    alpha.close()
    return bbox


def _connected_components_bbox(mask: List[List[bool]], mode: str) -> Optional[Tuple[int, int, int, int]]:
    h = len(mask)
    w = len(mask[0]) if h else 0
    if w == 0 or h == 0:
        return None
    seen = [[False for _ in range(w)] for _ in range(h)]
    best: Optional[Tuple[int, int, int, int]] = None
    best_score = -1.0
    cx = (w - 1) / 2.0
    cy = (h - 1) / 2.0
    diag = max(1.0, math.hypot(cx, cy))
    for y in range(h):
        for x in range(w):
            if seen[y][x] or not mask[y][x]:
                continue
            stack = [(x, y)]
            seen[y][x] = True
            left = right = x
            top = bottom = y
            count = 0
            sum_x = 0.0
            sum_y = 0.0
            while stack:
                px, py = stack.pop()
                count += 1
                sum_x += px
                sum_y += py
                left = min(left, px)
                right = max(right, px)
                top = min(top, py)
                bottom = max(bottom, py)
                for nx, ny in ((px - 1, py), (px + 1, py), (px, py - 1), (px, py + 1)):
                    if 0 <= nx < w and 0 <= ny < h and not seen[ny][nx] and mask[ny][nx]:
                        seen[ny][nx] = True
                        stack.append((nx, ny))
            area = float(count)
            if mode == "clipart_central":
                comp_cx = sum_x / count
                comp_cy = sum_y / count
                center_penalty = math.hypot(comp_cx - cx, comp_cy - cy) / diag
                score = area * max(0.1, 1.0 - (center_penalty * 0.6))
            else:
                score = area
            if score > best_score:
                best_score = score
                best = (left, top, right + 1, bottom + 1)
    return best


def _aggressive_subject_trim(
    image: Image.Image,
    *,
    min_alpha: int = 1,
    min_reduction_pct: float = 0.002,
    mode: str = "clipart_central",
    subject_fill_target: Optional[float] = None,
) -> TrimBoundsResult:
    before_bbox = _compute_alpha_bbox(image, min_alpha=min_alpha)
    if before_bbox is None:
        result = TrimBoundsResult(image=image, trimmed_size=None, applied=False, skip_reason="no_meaningful_alpha_bounds")
        result.subject_bounds_before = None
        result.subject_bounds_after = None
        return result

    alpha = image.getchannel("A")
    pixels = alpha.load()
    w, h = image.size
    mask = [[pixels[x, y] >= min_alpha for x in range(w)] for y in range(h)]
    alpha.close()

    after_bbox = _connected_components_bbox(mask, mode) or before_bbox
    left, top, right, bottom = after_bbox

    if subject_fill_target is not None and 0 < subject_fill_target <= 1.0:
        sw = max(1, right - left)
        sh = max(1, bottom - top)
        expand_x = max(0, int(math.ceil((sw / subject_fill_target - sw) / 2.0)))
        expand_y = max(0, int(math.ceil((sh / subject_fill_target - sh) / 2.0)))
        left = max(0, left - expand_x)
        top = max(0, top - expand_y)
        right = min(image.width, right + expand_x)
        bottom = min(image.height, bottom + expand_y)

    trimmed_w = max(1, right - left)
    trimmed_h = max(1, bottom - top)
    original_area = image.width * image.height
    reduction_pct = 0.0 if original_area <= 0 else max(0.0, 1.0 - ((trimmed_w * trimmed_h) / original_area))
    if reduction_pct < max(0.0, min_reduction_pct):
        result = TrimBoundsResult(image=image, trimmed_size=(image.width, image.height), applied=False, skip_reason="below_reduction_threshold")
        result.subject_bounds_before = before_bbox
        result.subject_bounds_after = (left, top, right, bottom)
        return result

    if left == 0 and top == 0 and right == image.width and bottom == image.height:
        result = TrimBoundsResult(image=image, trimmed_size=(image.width, image.height), applied=False, skip_reason="below_reduction_threshold")
        result.subject_bounds_before = before_bbox
        result.subject_bounds_after = (left, top, right, bottom)
        return result

    trimmed = image.crop((left, top, right, bottom))
    image.close()
    result = TrimBoundsResult(image=trimmed, trimmed_size=(trimmed.width, trimmed.height), applied=True)
    result.subject_bounds_before = before_bbox
    result.subject_bounds_after = (left, top, right, bottom)
    return result

def _resolve_trim_bounds_settings(template: ProductTemplate) -> Tuple[int, float, float]:
    preset_name = (template.trim_bounds_preset or "").strip().lower()
    preset = TRIM_PRESETS.get(preset_name, {})
    min_alpha = int(preset.get("trim_bounds_min_alpha", template.trim_bounds_min_alpha))
    padding_pct = float(preset.get("trim_bounds_padding_pct", template.trim_bounds_padding_pct))
    min_reduction_pct = float(preset.get("trim_bounds_min_reduction_pct", template.trim_bounds_min_reduction_pct))

    # Explicit numeric overrides always win over preset defaults.
    if template.trim_bounds_min_alpha != 8:
        min_alpha = int(template.trim_bounds_min_alpha)
    if template.trim_bounds_padding_pct != 0.015:
        padding_pct = float(template.trim_bounds_padding_pct)
    if template.trim_bounds_min_reduction_pct != 0.01:
        min_reduction_pct = float(template.trim_bounds_min_reduction_pct)
    return min_alpha, padding_pct, min_reduction_pct


def _resolve_max_upscale_factor(template: ProductTemplate, placement: PlacementRequirement) -> Optional[float]:
    """Resolve optional upscale cap with shirt-only safety behavior.

    Precedence: placement override -> template default. Applies only to shirt templates.
    """
    if not _is_shirt_template_key(template.key):
        return None
    if placement.max_upscale_factor is not None:
        return float(placement.max_upscale_factor)
    if template.max_upscale_factor is not None:
        return float(template.max_upscale_factor)
    return None


def resolve_artwork_for_placement(
    artwork: Artwork,
    placement: PlacementRequirement,
    *,
    template_key: str = "",
    allow_upscale: bool,
    upscale_method: str,
    skip_undersized: bool,
    trim_artwork_bounds: bool = False,
    trim_min_alpha: int = 8,
    trim_padding_pct: float = 0.015,
    trim_min_reduction_pct: float = 0.01,
    max_upscale_factor: Optional[float] = None,
    aggressive_subject_trim_mode: Optional[str] = None,
    subject_fill_target: Optional[float] = None,
) -> ArtworkResolution:
    with Image.open(artwork.src_path) as opened:
        image = ImageOps.exif_transpose(opened).convert("RGBA")
    original_size = (image.width, image.height)
    trimmed_size: Optional[Tuple[int, int]] = None
    trim_applied = False
    trim_skip_reason: Optional[str] = None
    subject_bounds_before_aggressive_trim: Optional[Tuple[int, int, int, int]] = None
    subject_bounds_after_aggressive_trim: Optional[Tuple[int, int, int, int]] = None
    aggressive_trim_used = False
    if trim_artwork_bounds:
        trim_result = _trim_artwork_bounds(
            image,
            min_alpha=trim_min_alpha,
            padding_pct=trim_padding_pct,
            min_reduction_pct=trim_min_reduction_pct,
        )
        image = trim_result.image
        trimmed_size = trim_result.trimmed_size
        trim_applied = trim_result.applied
        trim_skip_reason = trim_result.skip_reason

    if trim_artwork_bounds and aggressive_subject_trim_mode:
        aggressive_trim_result = _aggressive_subject_trim(
            image,
            min_alpha=max(1, trim_min_alpha),
            min_reduction_pct=trim_min_reduction_pct,
            mode=aggressive_subject_trim_mode,
            subject_fill_target=subject_fill_target,
        )
        subject_bounds_before_aggressive_trim = getattr(aggressive_trim_result, "subject_bounds_before", None)
        subject_bounds_after_aggressive_trim = getattr(aggressive_trim_result, "subject_bounds_after", None)
        aggressive_trim_used = aggressive_trim_result.applied
        image = aggressive_trim_result.image
        trimmed_size = aggressive_trim_result.trimmed_size
        trim_applied = trim_applied or aggressive_trim_result.applied
        if aggressive_trim_result.skip_reason:
            trim_skip_reason = aggressive_trim_result.skip_reason
    trim_bounds_pct: Optional[Tuple[float, float]] = None
    if trimmed_size:
        trim_bounds_pct = (
            0.0 if original_size[0] <= 0 else round((trimmed_size[0] / original_size[0]) * 100.0, 2),
            0.0 if original_size[1] <= 0 else round((trimmed_size[1] / original_size[1]) * 100.0, 2),
        )
    required_size = (placement.width_px, placement.height_px)
    fit_mode = str(placement.artwork_fit_mode or "contain").strip().lower()
    if fit_mode not in {"contain", "cover"}:
        logger.warning(
            "Unknown artwork_fit_mode=%s for placement=%s; defaulting to contain",
            fit_mode,
            placement.placement_name,
        )
        fit_mode = "contain"
    poster_strategy_path = ""
    poster_cover_eligible = False
    poster_enhancement_considered = False
    poster_enhancement_applied = False
    poster_requested_upscale_factor = 1.0
    poster_applied_upscale_factor = 1.0
    poster_trim_fill_optimization_applied = False
    effective_allow_upscale = allow_upscale
    effective_max_upscale_factor = max_upscale_factor
    if template_key == "poster_basic":
        poster_cover_eligible = image.width >= placement.width_px and image.height >= placement.height_px
        logger.info(
            "Poster strategy candidate=cover eligible=%s reason=%s placement=%s source=%sx%s required=%sx%s",
            poster_cover_eligible,
            "sufficient_resolution" if poster_cover_eligible else "insufficient_resolution",
            placement.placement_name,
            image.width,
            image.height,
            placement.width_px,
            placement.height_px,
        )
        if poster_cover_eligible:
            fit_mode = "cover"
            poster_strategy_path = "cover"
            logger.info(
                "Poster strategy selected template=%s placement=%s strategy=cover reason=eligible_resolution",
                template_key,
                placement.placement_name,
            )
        else:
            fit_mode = "contain"
            poster_strategy_path = "contain_fallback"
            poster_enhancement_considered = True
            logger.warning(
                "Poster strategy fallback=contain reason=insufficient_resolution placement=%s source=%sx%s required=%sx%s",
                placement.placement_name,
                image.width,
                image.height,
                placement.width_px,
                placement.height_px,
            )
            poster_requested_scale = min(placement.width_px / image.width, placement.height_px / image.height)
            poster_requested_upscale_factor = poster_requested_scale if poster_requested_scale > 1.0 else 1.0
            min_source_ratio = min(image.width / placement.width_px, image.height / placement.height_px)
            enhancement_within_limits = (
                poster_requested_upscale_factor > 1.0
                and poster_requested_upscale_factor <= POSTER_SAFE_ENHANCEMENT_MAX_UPSCALE_FACTOR
                and min_source_ratio >= POSTER_SAFE_ENHANCEMENT_MIN_SOURCE_RATIO
            )
            if enhancement_within_limits:
                effective_allow_upscale = True
                effective_max_upscale_factor = (
                    min(max_upscale_factor, POSTER_SAFE_ENHANCEMENT_MAX_UPSCALE_FACTOR)
                    if max_upscale_factor is not None and max_upscale_factor > 0
                    else POSTER_SAFE_ENHANCEMENT_MAX_UPSCALE_FACTOR
                )
                poster_enhancement_applied = True
                poster_applied_upscale_factor = min(
                    poster_requested_upscale_factor,
                    effective_max_upscale_factor if effective_max_upscale_factor else poster_requested_upscale_factor,
                )
                logger.info(
                    "Poster enhancement applied placement=%s strategy=bounded_contain_upscale requested_upscale_factor=%.3f applied_upscale_factor=%.3f max_allowed=%.3f trim_fill_optimization_applied=%s",
                    placement.placement_name,
                    poster_requested_upscale_factor,
                    poster_applied_upscale_factor,
                    effective_max_upscale_factor if effective_max_upscale_factor is not None else 0.0,
                    poster_trim_fill_optimization_applied,
                )
            else:
                effective_allow_upscale = False
                logger.info(
                    "Poster enhancement skipped placement=%s reason=outside_safe_limits requested_upscale_factor=%.3f max_safe_upscale_factor=%.3f min_source_ratio=%.3f min_required_source_ratio=%.3f trim_fill_optimization_applied=%s",
                    placement.placement_name,
                    poster_requested_upscale_factor,
                    POSTER_SAFE_ENHANCEMENT_MAX_UPSCALE_FACTOR,
                    min_source_ratio,
                    POSTER_SAFE_ENHANCEMENT_MIN_SOURCE_RATIO,
                    poster_trim_fill_optimization_applied,
                )

    too_small = fit_mode == "cover" and (image.width < placement.width_px or image.height < placement.height_px)

    logger.info(
        "Artwork %s placement=%s fit_mode=%s original=%sx%s trimmed=%s required=%sx%s",
        artwork.src_path.name,
        placement.placement_name,
        fit_mode,
        original_size[0],
        original_size[1],
        f"{trimmed_size[0]}x{trimmed_size[1]}" if trimmed_size else "n/a",
        required_size[0],
        required_size[1],
    )
    if trim_bounds_pct:
        logger.info(
            "Trim debug artwork=%s placement=%s original_bounds_pct=100x100 trimmed_bounds_pct=%.2fx%.2f",
            artwork.src_path.name,
            placement.placement_name,
            trim_bounds_pct[0],
            trim_bounds_pct[1],
        )
    if aggressive_subject_trim_mode:
        logger.info(
            "Aggressive trim debug artwork=%s placement=%s mode=%s used=%s subject_bounds_before=%s subject_bounds_after=%s subject_fill_target=%s",
            artwork.src_path.name,
            placement.placement_name,
            aggressive_subject_trim_mode,
            aggressive_trim_used,
            subject_bounds_before_aggressive_trim,
            subject_bounds_after_aggressive_trim,
            subject_fill_target,
        )

    if trim_artwork_bounds and not trim_applied and trim_skip_reason:
        logger.info(
            "Trim skipped artwork=%s placement=%s reason=%s",
            artwork.src_path.name,
            placement.placement_name,
            trim_skip_reason,
        )

    if too_small and not effective_allow_upscale:
        if skip_undersized:
            logger.warning(
                "Skipping undersized artwork %s for placement %s: %sx%s < %sx%s (action=skip fit_mode=%s)",
                artwork.src_path.name,
                placement.placement_name,
                original_size[0],
                original_size[1],
                required_size[0],
                required_size[1],
                fit_mode,
            )
            return ArtworkResolution(
                image=image,
                action="skip",
                upscaled=False,
                original_size=original_size,
                trimmed_size=trimmed_size,
                final_size=original_size,
                resized_size=original_size,
                effective_upscale_factor=1.0,
                trim_bounds_pct=trim_bounds_pct,
                trim_applied=trim_applied,
                trim_skip_reason=trim_skip_reason,
            )
        logger.error(
            "Undersized artwork action=fail artwork=%s placement=%s fit_mode=%s original=%sx%s required=%sx%s",
            artwork.src_path.name,
            placement.placement_name,
            fit_mode,
            original_size[0],
            original_size[1],
            required_size[0],
            required_size[1],
        )
        raise ValueError(
            f"image too small ({original_size[0]}x{original_size[1]}) for "
            f"placement {placement.placement_name} ({required_size[0]}x{required_size[1]})"
        )

    if fit_mode == "cover":
        requested_scale = max(placement.width_px / image.width, placement.height_px / image.height)
    else:
        requested_scale = min(placement.width_px / image.width, placement.height_px / image.height)
        if not effective_allow_upscale:
            requested_scale = min(requested_scale, 1.0)
    requested_upscale_factor = requested_scale if requested_scale > 1.0 else 1.0
    scale = requested_scale
    upscale_capped = False
    if (
        scale > 1.0
        and effective_max_upscale_factor is not None
        and effective_max_upscale_factor > 0
        and scale > effective_max_upscale_factor
    ):
        scale = effective_max_upscale_factor
        upscale_capped = True
        logger.warning(
            "Upscale cap applied artwork=%s placement=%s fit_mode=%s requested_upscale_factor=%.3f applied_upscale_factor=%.3f max_upscale_factor=%.3f",
            artwork.src_path.name,
            placement.placement_name,
            fit_mode,
            requested_upscale_factor,
            scale,
            effective_max_upscale_factor,
        )
    applied_upscale_factor = scale if scale > 1.0 else 1.0
    upscaled = scale > 1
    resize_filter = _upscale_filter(upscale_method) if upscaled else Image.LANCZOS
    resized_size = (math.ceil(image.width * scale), math.ceil(image.height * scale))
    resized = image.resize(resized_size, resize_filter)

    if fit_mode == "cover":
        left = max(0, (resized.width - placement.width_px) // 2)
        top = max(0, (resized.height - placement.height_px) // 2)
        final = resized.crop((left, top, left + placement.width_px, top + placement.height_px))
        action = "covered_cropped_upscale" if upscaled else "covered_cropped"
    else:
        final = Image.new("RGBA", required_size, (0, 0, 0, 0))
        left = max(0, (placement.width_px - resized.width) // 2)
        top = max(0, (placement.height_px - resized.height) // 2)
        final.alpha_composite(resized, (left, top))
        action = "contained_padded_upscale" if upscaled else "contained_padded"

    logger.info(
        "Artwork resolution action=%s placement=%s fit_mode=%s original=%sx%s trimmed=%s resized=%sx%s final_canvas=%sx%s upscaled=%s requested_upscale_factor=%.3f applied_upscale_factor=%.3f upscale_capped=%s effective_upscale_factor=%.3f",
        action,
        placement.placement_name,
        fit_mode,
        original_size[0],
        original_size[1],
        f"{trimmed_size[0]}x{trimmed_size[1]}" if trimmed_size else "n/a",
        resized.width,
        resized.height,
        final.width,
        final.height,
        upscaled,
        requested_upscale_factor,
        applied_upscale_factor,
        upscale_capped,
        applied_upscale_factor,
    )
    if template_key == "poster_basic":
        logger.info(
            "Poster transform final scale=%.3f x=%s y=%s placement=%s",
            placement.placement_scale,
            placement.placement_x,
            placement.placement_y,
            placement.placement_name,
        )
        logger.info(
            "Poster final resolution path chosen strategy=%s fit_mode=%s action=%s upscaled=%s upscale_capped=%s enhancement_considered=%s enhancement_applied=%s requested_upscale_factor=%.3f applied_upscale_factor=%.3f trim_fill_optimization_applied=%s",
            poster_strategy_path or fit_mode,
            fit_mode,
            action,
            upscaled,
            upscale_capped,
            poster_enhancement_considered,
            poster_enhancement_applied,
            poster_requested_upscale_factor,
            applied_upscale_factor,
            poster_trim_fill_optimization_applied,
        )
        if upscaled:
            logger.info(
                "Poster upscale applied placement=%s requested_upscale_factor=%.3f applied_upscale_factor=%.3f",
                placement.placement_name,
                requested_upscale_factor,
                applied_upscale_factor,
            )
        elif upscale_capped:
            logger.info(
                "Poster upscale capped placement=%s requested_upscale_factor=%.3f applied_upscale_factor=%.3f",
                placement.placement_name,
                requested_upscale_factor,
                applied_upscale_factor,
            )
        else:
            logger.info(
                "Poster upscale not applied placement=%s requested_upscale_factor=%.3f",
                placement.placement_name,
                requested_upscale_factor,
            )
    resized.close()

    return ArtworkResolution(
        image=final,
        action=action,
        upscaled=upscaled,
        original_size=original_size,
        trimmed_size=trimmed_size,
        final_size=(final.width, final.height),
        resized_size=(resized_size[0], resized_size[1]),
        requested_upscale_factor=requested_upscale_factor,
        applied_upscale_factor=applied_upscale_factor,
        upscale_capped=upscale_capped,
        effective_upscale_factor=applied_upscale_factor,
        trim_bounds_pct=trim_bounds_pct,
        trim_applied=trim_applied,
        trim_skip_reason=trim_skip_reason,
        subject_bounds_before_aggressive_trim=subject_bounds_before_aggressive_trim,
        subject_bounds_after_aggressive_trim=subject_bounds_after_aggressive_trim,
        subject_fill_target=subject_fill_target,
        aggressive_trim_used=aggressive_trim_used,
    )




def _write_placement_preview(*, artwork: Artwork, template: ProductTemplate, placement: PlacementRequirement, resolution: ArtworkResolution, preview_dir: pathlib.Path) -> None:
    preview_dir.mkdir(parents=True, exist_ok=True)
    with Image.open(artwork.src_path) as source_opened:
        source = ImageOps.exif_transpose(source_opened).convert("RGBA")
    panel_w = max(source.width, resolution.image.width)
    panel_h = source.height + resolution.image.height
    canvas = Image.new("RGBA", (panel_w, panel_h), (255, 255, 255, 255))
    canvas.alpha_composite(source, (0, 0))
    canvas.alpha_composite(resolution.image, (0, source.height))
    orientation = _orientation_bucket(artwork.image_width, artwork.image_height)
    out = preview_dir / f"{template.key}-{artwork.slug}-{placement.placement_name}-{orientation}.png"
    canvas.save(out, "PNG")
    source.close()
    canvas.close()

def prepare_artwork_export(
    artwork: Artwork,
    template: ProductTemplate,
    placement: PlacementRequirement,
    export_dir: pathlib.Path,
    options: ArtworkProcessingOptions,
) -> Optional[PreparedArtwork]:
    export_path = export_dir / template.key / f"{artwork.slug}-{placement.placement_name}.png"
    export_path.parent.mkdir(parents=True, exist_ok=True)

    shirt_only_trim_requested = template.trim_artwork_bounds_for_shirts
    is_shirt_template = _is_shirt_template_key(template.key)
    trim_artwork_bounds = template.trim_artwork_bounds or (shirt_only_trim_requested and is_shirt_template)
    trim_min_alpha, trim_padding_pct, trim_min_reduction_pct = _resolve_trim_bounds_settings(template)
    use_aggressive_trim = bool(template.aggressive_subject_trim_for_shirts and is_shirt_template and placement.placement_name.strip().lower() == "front")
    aggressive_subject_trim_mode = (template.aggressive_subject_trim_mode or "clipart_central") if use_aggressive_trim else None
    subject_fill_target = template.shirt_subject_fill_target if use_aggressive_trim else None

    resolution = resolve_artwork_for_placement(
        artwork,
        placement,
        template_key=template.key,
        allow_upscale=options.allow_upscale or placement.allow_upscale,
        upscale_method=options.upscale_method,
        skip_undersized=options.skip_undersized,
        trim_artwork_bounds=trim_artwork_bounds,
        trim_min_alpha=trim_min_alpha,
        trim_padding_pct=trim_padding_pct,
        trim_min_reduction_pct=trim_min_reduction_pct,
        max_upscale_factor=_resolve_max_upscale_factor(template, placement),
        aggressive_subject_trim_mode=aggressive_subject_trim_mode,
        subject_fill_target=subject_fill_target,
    )
    if not trim_artwork_bounds and shirt_only_trim_requested and not is_shirt_template:
        resolution.trim_skip_reason = "non_shirt_template"
        logger.info(
            "Trim skipped artwork=%s placement=%s template=%s reason=%s",
            artwork.src_path.name,
            placement.placement_name,
            template.key,
            resolution.trim_skip_reason,
        )
    if resolution.action == "skip":
        return None

    resolution.image.save(export_path, "PNG")
    if options.placement_preview:
        _write_placement_preview(
            artwork=artwork,
            template=template,
            placement=placement,
            resolution=resolution,
            preview_dir=options.preview_dir,
        )
    resolution.image.close()
    return PreparedArtwork(
        artwork=artwork,
        template=template,
        placement=placement,
        export_path=export_path,
        width_px=placement.width_px,
        height_px=placement.height_px,
        source_size=resolution.original_size,
        trimmed_size=resolution.trimmed_size,
        exported_canvas_size=resolution.final_size,
        upscaled=resolution.upscaled,
        requested_upscale_factor=resolution.requested_upscale_factor,
        applied_upscale_factor=resolution.applied_upscale_factor,
        upscale_capped=resolution.upscale_capped,
        effective_upscale_factor=resolution.effective_upscale_factor,
        trim_applied=resolution.trim_applied,
        trim_skip_reason=resolution.trim_skip_reason,
        subject_bounds_before_aggressive_trim=resolution.subject_bounds_before_aggressive_trim,
        subject_bounds_after_aggressive_trim=resolution.subject_bounds_after_aggressive_trim,
        subject_fill_target=resolution.subject_fill_target,
        aggressive_trim_used=resolution.aggressive_trim_used,
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
        if str(placement.get("artwork_fit_mode", "contain")).strip().lower() not in {"contain", "cover"}:
            raise TemplateValidationError(f"Template[{index}] placement[{pidx}] artwork_fit_mode must be contain|cover")
        if "placement_scale" in placement and float(placement["placement_scale"]) <= 0:
            raise TemplateValidationError(f"Template[{index}] placement[{pidx}] placement_scale must be > 0")
        if "max_upscale_factor" in placement and placement.get("max_upscale_factor") is not None and float(placement.get("max_upscale_factor", 0)) <= 0:
            raise TemplateValidationError(f"Template[{index}] placement[{pidx}] max_upscale_factor must be > 0 when provided")
    if "trim_artwork_bounds" in row and not isinstance(row.get("trim_artwork_bounds"), bool):
        raise TemplateValidationError(f"Template[{index}] trim_artwork_bounds must be a boolean when provided")
    if "trim_artwork_bounds_for_shirts" in row and not isinstance(row.get("trim_artwork_bounds_for_shirts"), bool):
        raise TemplateValidationError(f"Template[{index}] trim_artwork_bounds_for_shirts must be a boolean when provided")
    if row.get("trim_bounds_preset") is not None:
        preset = str(row.get("trim_bounds_preset", "")).strip().lower()
        if preset not in TRIM_PRESETS:
            raise TemplateValidationError(
                f"Template[{index}] trim_bounds_preset must be one of {sorted(TRIM_PRESETS.keys())}"
            )
    if row.get("trim_bounds_min_alpha") is not None:
        min_alpha = int(row.get("trim_bounds_min_alpha", 8))
        if min_alpha < 0 or min_alpha > 255:
            raise TemplateValidationError(f"Template[{index}] trim_bounds_min_alpha must be between 0 and 255")
    if row.get("trim_bounds_padding_pct") is not None and float(row.get("trim_bounds_padding_pct", 0.0)) < 0:
        raise TemplateValidationError(f"Template[{index}] trim_bounds_padding_pct must be >= 0")
    if row.get("trim_bounds_min_reduction_pct") is not None and float(row.get("trim_bounds_min_reduction_pct", 0.0)) < 0:
        raise TemplateValidationError(f"Template[{index}] trim_bounds_min_reduction_pct must be >= 0")
    if "aggressive_subject_trim_for_shirts" in row and not isinstance(row.get("aggressive_subject_trim_for_shirts"), bool):
        raise TemplateValidationError(f"Template[{index}] aggressive_subject_trim_for_shirts must be a boolean when provided")
    if row.get("aggressive_subject_trim_mode") is not None:
        mode = str(row.get("aggressive_subject_trim_mode", "")).strip().lower()
        if mode not in {"clipart_central"}:
            raise TemplateValidationError(f"Template[{index}] aggressive_subject_trim_mode must be one of ['clipart_central']")
    if row.get("shirt_subject_fill_target") is not None:
        fill_target = float(row.get("shirt_subject_fill_target", 0))
        if fill_target <= 0 or fill_target > 1:
            raise TemplateValidationError(f"Template[{index}] shirt_subject_fill_target must be > 0 and <= 1 when provided")
    if row.get("markup_type", "fixed") not in {"fixed", "percent"}:
        raise TemplateValidationError(f"Template[{index}] markup_type must be fixed|percent")
    if row.get("rounding_mode", "none") not in {"none", "whole_dollar", "x_99"}:
        raise TemplateValidationError(f"Template[{index}] rounding_mode must be none|whole_dollar|x_99")
    if row.get("max_enabled_variants") is not None and int(row.get("max_enabled_variants", 0)) <= 0:
        raise TemplateValidationError(f"Template[{index}] max_enabled_variants must be > 0 when provided")
    if row.get("max_upscale_factor") is not None and float(row.get("max_upscale_factor", 0)) <= 0:
        raise TemplateValidationError(f"Template[{index}] max_upscale_factor must be > 0 when provided")
    option_filters = row.get("enabled_variant_option_filters")
    if option_filters is not None and not isinstance(option_filters, dict):
        raise TemplateValidationError(f"Template[{index}] enabled_variant_option_filters must be an object")


def load_templates(config_path: pathlib.Path) -> List[ProductTemplate]:
    raw = load_json(config_path, [])
    if isinstance(raw, dict):
        raw = [raw]
    elif not isinstance(raw, list):
        raise TemplateValidationError("product_templates.json must contain a top-level JSON array or object")

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
                trim_artwork_bounds=bool(row.get("trim_artwork_bounds", False)),
                trim_artwork_bounds_for_shirts=bool(row.get("trim_artwork_bounds_for_shirts", False)),
                trim_bounds_preset=str(row.get("trim_bounds_preset", "")).strip().lower() or None,
                trim_bounds_min_alpha=int(row.get("trim_bounds_min_alpha", 8)),
                trim_bounds_padding_pct=float(row.get("trim_bounds_padding_pct", 0.015)),
                trim_bounds_min_reduction_pct=float(row.get("trim_bounds_min_reduction_pct", 0.01)),
                aggressive_subject_trim_for_shirts=bool(row.get("aggressive_subject_trim_for_shirts", False)),
                aggressive_subject_trim_mode=str(row.get("aggressive_subject_trim_mode", "")).strip().lower() or None,
                shirt_subject_fill_target=float(row["shirt_subject_fill_target"]) if row.get("shirt_subject_fill_target") is not None else None,
                seo_keywords=[str(v) for v in row.get("seo_keywords", [])],
                audience=str(row.get("audience", "")).strip() or None,
                product_type_label=str(row.get("product_type_label", "")).strip() or None,
                style_keywords=[str(v) for v in row.get("style_keywords", [])],
                max_upscale_factor=float(row["max_upscale_factor"]) if row.get("max_upscale_factor") is not None else None,
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
    color_option_exists = any(_variant_option_value(variant, "color") for variant in catalog_variants)
    size_option_exists = any(_variant_option_value(variant, "size") for variant in catalog_variants)

    should_filter_colors = bool(template.enabled_colors) and color_option_exists
    should_filter_sizes = bool(template.enabled_sizes) and size_option_exists

    if template.enabled_colors and not color_option_exists:
        logger.warning(
            "Template %s specifies enabled_colors, but blueprint %s/provider %s exposes no color option; ignoring color filter.",
            template.key,
            template.printify_blueprint_id,
            template.printify_print_provider_id,
        )
    if template.enabled_sizes and not size_option_exists:
        logger.warning(
            "Template %s specifies enabled_sizes, but blueprint %s/provider %s exposes no size option; ignoring size filter.",
            template.key,
            template.printify_blueprint_id,
            template.printify_print_provider_id,
        )

    for variant in catalog_variants:
        color = _variant_option_value(variant, "color")
        size = _variant_option_value(variant, "size")
        is_available = variant.get("is_available", True)
        option_filter_ok = True
        for option_key, allowed_values in option_filters.items():
            if _variant_option_value(variant, option_key) not in allowed_values:
                option_filter_ok = False
                break

        color_ok = (not should_filter_colors) or color in template.enabled_colors
        size_ok = (not should_filter_sizes) or size in template.enabled_sizes

        if color_ok and size_ok and option_filter_ok and is_available:
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


def _validate_template_catalog_mapping(
    *,
    printify: PrintifyClient,
    template: ProductTemplate,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    blueprints = printify.list_blueprints()
    if not any(int(blueprint.get("id", 0)) == template.printify_blueprint_id for blueprint in blueprints):
        raise TemplateValidationError(
            f"Template {template.key} catalog validation failed: missing blueprint blueprint_id={template.printify_blueprint_id}"
        )

    providers = printify.list_print_providers(template.printify_blueprint_id)
    provider_ids = {int(provider.get("id", 0)) for provider in providers}
    if template.printify_print_provider_id not in provider_ids:
        valid = ", ".join(str(pid) for pid in sorted(pid for pid in provider_ids if pid > 0)) or "none"
        raise TemplateValidationError(
            f"Template {template.key} catalog validation failed: missing provider for blueprint blueprint_id={template.printify_blueprint_id} provider_id={template.printify_print_provider_id} valid_provider_ids=[{valid}]"
        )

    variants = printify.list_variants(template.printify_blueprint_id, template.printify_print_provider_id)
    if not variants:
        raise TemplateValidationError(
            f"Template {template.key} catalog validation failed: no valid variants for blueprint_id={template.printify_blueprint_id} provider_id={template.printify_print_provider_id}"
        )

    wanted_placements = {placement.placement_name.strip().lower() for placement in template.placements if placement.placement_name}
    available_placements = {
        placement.strip().lower()
        for placement in summarize_variant_options(variants).get("placements", [])
        if isinstance(placement, str) and placement.strip()
    }
    if available_placements and wanted_placements and not wanted_placements.issubset(available_placements):
        raise TemplateValidationError(
            f"Template {template.key} catalog validation failed: placement mismatch expected={sorted(wanted_placements)} available={sorted(available_placements)} blueprint_id={template.printify_blueprint_id} provider_id={template.printify_print_provider_id}"
        )

    placement_note = "placement_data_unavailable" if not available_placements else ",".join(sorted(available_placements))
    return variants, placement_note


def resolve_tote_template_catalog_mapping(
    *,
    printify: PrintifyClient,
    template: ProductTemplate,
) -> Tuple[ProductTemplate, List[Dict[str, Any]]]:
    logger.info(
        "Tote catalog resolution start template=%s blueprint_id=%s provider_id=%s",
        template.key,
        template.printify_blueprint_id,
        template.printify_print_provider_id,
    )
    try:
        variants, placement_note = _validate_template_catalog_mapping(printify=printify, template=template)
        logger.info(
            "Tote validation result template=%s status=ok blueprint_id=%s provider_id=%s placements=%s",
            template.key,
            template.printify_blueprint_id,
            template.printify_print_provider_id,
            placement_note,
        )
        return template, variants
    except TemplateValidationError as exc:
        root_reason = str(exc)
        logger.warning("Tote validation result template=%s status=failed reason=%s", template.key, exc)

    blueprints = search_blueprints(printify.list_blueprints(), "tote bag")
    if not blueprints:
        blueprints = search_blueprints(printify.list_blueprints(), "tote")

    best_template: Optional[ProductTemplate] = None
    best_variants: List[Dict[str, Any]] = []
    best_score = -1
    for blueprint in blueprints[:40]:
        blueprint_id = int(blueprint.get("id") or 0)
        if blueprint_id <= 0:
            continue
        providers = printify.list_print_providers(blueprint_id)
        for provider in providers:
            provider_id = int(provider.get("id") or 0)
            if provider_id <= 0:
                continue
            try:
                variants = printify.list_variants(blueprint_id, provider_id)
            except Exception:
                continue
            if not variants:
                continue
            candidate_template = replace(
                template,
                printify_blueprint_id=blueprint_id,
                printify_print_provider_id=provider_id,
            )
            score = score_provider_for_template(provider, variants, candidate_template)
            if score.get("matching_variant_count", 0) <= 0:
                continue
            if score["score"] > best_score:
                best_score = score["score"]
                best_template = candidate_template
                best_variants = variants

    if best_template is None:
        raise TemplateValidationError(
            f"{root_reason}; tote fallback discovery failed: no valid variants"
        )

    logger.info(
        "Tote catalog resolution fallback selected template=%s old_blueprint_id=%s old_provider_id=%s new_blueprint_id=%s new_provider_id=%s score=%s",
        template.key,
        template.printify_blueprint_id,
        template.printify_print_provider_id,
        best_template.printify_blueprint_id,
        best_template.printify_print_provider_id,
        best_score,
    )
    return best_template, best_variants


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
        transform = compute_placement_transform_for_artwork(placement, artwork, template.key)
        logger.info(
            "Placement transform template=%s placement=%s orientation=%s scale=%.3f x=%.3f y=%.3f angle=%.3f",
            template.key,
            placement.placement_name,
            _orientation_bucket(artwork.image_width, artwork.image_height),
            transform.scale,
            transform.x,
            transform.y,
            transform.angle,
        )
        normalized_transform = normalize_printify_transform(transform)
        print_areas.append({
            "variant_ids": enabled_variant_ids,
            "placeholders": [{
                "position": placement.placement_name,
                "images": [{
                    "id": upload_info["id"],
                    "x": normalized_transform["x"],
                    "y": normalized_transform["y"],
                    "scale": normalized_transform["scale"],
                    "angle": normalized_transform["angle"],
                }],
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


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _description_excerpt(description_html: str, *, max_len: int = 220) -> str:
    plain = re.sub(r"<[^>]+>", " ", str(description_html or ""))
    plain = _normalize_whitespace(plain)
    if len(plain) <= max_len:
        return plain
    return plain[: max_len - 1].rstrip() + "…"


def _collect_placeholder_tokens(text: str) -> List[str]:
    return sorted(set(re.findall(r"\{[^{}]+\}", str(text or ""))))


def _warnings_to_text(warnings: List[str]) -> str:
    return " | ".join(warnings)


def _money_str(minor: Optional[int]) -> str:
    if minor is None:
        return ""
    return f"{Decimal(minor) / Decimal('100'):.2f}"


def validate_storefront_title(*, title: str, title_source: str, title_quality: str, artwork: Artwork, template: ProductTemplate) -> Tuple[List[str], List[str]]:
    warnings: List[str] = []
    errors: List[str] = []
    normalized_title = _normalize_whitespace(title)
    if not normalized_title:
        errors.append("title_empty")
        return warnings, errors
    if _collect_placeholder_tokens(normalized_title):
        errors.append("title_unresolved_placeholder")
    lowered = normalized_title.lower()
    if len(normalized_title) > 140:
        warnings.append("title_too_long")
    if re.search(r"[a-f0-9]{12,}", lowered):
        warnings.append("title_hash_like")
    if re.fullmatch(r"[a-z0-9\-_,. ]{18,}", normalized_title):
        warnings.append("title_slug_like")
    if "untitled design" in lowered:
        warnings.append("title_bad_fallback")
    if "signature product" in lowered and str((artwork.metadata or {}).get("title") or "").strip():
        warnings.append("title_generic_with_metadata_available")
    for token in ("t-shirt t-shirt", "hoodie hoodie", "poster poster", "mug mug", "tote bag tote bag"):
        if token in lowered:
            warnings.append("title_duplicate_product_wording")
            break
    if title_source in {"filename_slug", "fallback"} and title_quality in {"hash_like", "slug_like", "too_short"}:
        warnings.append("title_low_quality_source")
    if not title_semantically_includes_product_label(normalized_title, template.product_type_label or template.shopify_product_type or ""):
        family = content_engine.infer_product_family(template)
        if family in {"shirt", "sweatshirt", "mug", "poster", "tote"} and "default" not in family:
            warnings.append("title_missing_product_signal")
    return warnings, errors


def validate_storefront_description(*, description_html: str, template: ProductTemplate, artwork: Artwork) -> Tuple[List[str], List[str]]:
    warnings: List[str] = []
    errors: List[str] = []
    normalized = _normalize_whitespace(description_html)
    if not normalized:
        errors.append("description_empty")
        return warnings, errors
    if _collect_placeholder_tokens(normalized):
        errors.append("description_unresolved_placeholder")
    excerpt = _description_excerpt(description_html, max_len=500)
    if len(excerpt) < 40:
        warnings.append("description_suspiciously_short")
    if normalized.count("<") != normalized.count(">"):
        warnings.append("description_html_unbalanced")
    metadata_description = str((artwork.metadata or {}).get("description") or "").strip()
    default_pattern = (template.description_pattern or "").strip() in {"", "{artwork_title}", "<p>{artwork_title}</p>"}
    if metadata_description and default_pattern and metadata_description.lower() not in normalized.lower():
        warnings.append("description_metadata_not_apparent")
    if metadata_description and "adds an easy style upgrade" in normalized.lower():
        warnings.append("description_generic_fallback_with_metadata")
    return warnings, errors


def validate_storefront_tags(*, tags: List[str], template: ProductTemplate, artwork: Artwork) -> Tuple[List[str], List[str]]:
    warnings: List[str] = []
    errors: List[str] = []
    if not tags:
        errors.append("tags_empty")
        return warnings, errors
    deduped = {_normalize_whitespace(tag).lower() for tag in tags if _normalize_whitespace(tag)}
    if len(deduped) != len(tags):
        warnings.append("tags_contain_duplicates")
    if any(len(tag) > 32 for tag in tags):
        warnings.append("tag_too_long")
    if len(tags) >= 20:
        warnings.append("tag_count_high")
    elif len(tags) >= 16:
        warnings.append("tag_count_near_limit")
    generic = {"print-on-demand", "printify", "gift", "style", "inkvibe", "apparel", "shirt", "clothing"}
    non_generic = [tag for tag in deduped if tag not in generic]
    if not non_generic:
        warnings.append("tags_generic_only")
    family_tags = set(content_engine.family_tags(template))
    if family_tags and not family_tags.intersection(deduped):
        warnings.append("tags_missing_family_signal")
    artwork_terms = {token for token in re.findall(r"[a-z0-9]+", artwork.slug.lower()) if len(token) >= 4}
    if artwork_terms and not any(any(term in tag for term in artwork_terms) for tag in deduped):
        warnings.append("tags_missing_artwork_theme_signal")
    return warnings, errors


def validate_storefront_pricing(*, template: ProductTemplate, variant_rows: List[Dict[str, Any]]) -> Tuple[List[str], List[str], Dict[str, Optional[int]]]:
    warnings: List[str] = []
    errors: List[str] = []
    sale_prices: List[int] = []
    compare_prices: List[int] = []
    markups: List[Decimal] = []
    for variant in variant_rows:
        try:
            sale = compute_sale_price_minor(template, variant)
        except Exception:
            errors.append("pricing_invalid_sale_value")
            continue
        if sale <= 0:
            errors.append("pricing_sale_non_positive")
        sale_prices.append(sale)
        try:
            compare = compute_compare_at_price_minor(template, sale)
        except Exception:
            errors.append("pricing_invalid_compare_at_value")
            compare = None
        if compare is not None:
            compare_prices.append(compare)
            if compare <= sale:
                errors.append("pricing_compare_at_not_greater")
        base_source = template.base_price if template.base_price is not None else variant.get("price")
        if base_source is not None:
            try:
                base_minor = normalize_printify_price(base_source)
                if base_minor > 0:
                    markups.append((Decimal(sale) / Decimal(base_minor)) - Decimal("1"))
            except Exception:
                pass
    if not sale_prices:
        errors.append("pricing_no_variant_prices")
    if sale_prices and (max(sale_prices) - min(sale_prices)) > 5000:
        warnings.append("pricing_variant_spread_high")
    if markups and max(markups) > Decimal("4.0"):
        warnings.append("pricing_extreme_markup_outlier")
    if template.rounding_mode == "x_99" and any((price % 100) != 99 for price in sale_prices):
        warnings.append("pricing_rounding_inconsistent")
    if template.rounding_mode == "whole_dollar" and any((price % 100) != 0 for price in sale_prices):
        warnings.append("pricing_rounding_inconsistent")
    summary = {
        "sale_min": min(sale_prices) if sale_prices else None,
        "sale_max": max(sale_prices) if sale_prices else None,
        "compare_min": min(compare_prices) if compare_prices else None,
        "compare_max": max(compare_prices) if compare_prices else None,
    }
    return warnings, errors, summary


def validate_storefront_options(*, template: ProductTemplate, variant_rows: List[Dict[str, Any]], product_options: List[Dict[str, Any]], variant_payloads: List[Dict[str, Any]]) -> Tuple[List[str], List[str], List[str]]:
    warnings: List[str] = []
    errors: List[str] = []
    option_names = [str(opt.get("name") or "") for opt in product_options if isinstance(opt, dict)]
    if not variant_rows:
        errors.append("options_zero_enabled_variants")
    colors_present = any(_variant_option_value(v, "color") for v in variant_rows)
    sizes_present = any(_variant_option_value(v, "size") for v in variant_rows)
    if colors_present and "Color" not in option_names:
        errors.append("options_missing_color_name")
    if sizes_present and "Size" not in option_names:
        errors.append("options_missing_size_name")
    if (colors_present or sizes_present) and "Title" in option_names:
        warnings.append("options_default_title_with_real_dimensions")
    seen_combos: set[Tuple[str, ...]] = set()
    for payload in variant_payloads:
        option_values = payload.get("optionValues") or []
        combo = tuple(sorted(f"{item.get('optionName')}={item.get('name')}" for item in option_values if isinstance(item, dict)))
        if not combo:
            errors.append("options_missing_option_values")
            continue
        if combo in seen_combos:
            errors.append("options_duplicate_combination")
        seen_combos.add(combo)
    return warnings, errors, option_names


def validate_storefront_mockups(*, template: ProductTemplate, publish_payload: Dict[str, Any], placement_context: str) -> Tuple[List[str], List[str]]:
    warnings: List[str] = []
    errors: List[str] = []
    if publish_payload.get("images") is False:
        warnings.append("mockups_publish_images_disabled")
    if template.publish_mockups is not None:
        warnings.append("mockups_publish_mockups_override_set")
    if not placement_context:
        warnings.append("mockups_no_placement_context")
    if not template.placements:
        errors.append("mockups_template_has_no_placements")
    warnings.append("mockups_channel_provider_dependent_selection")
    return warnings, errors


def build_storefront_qa_row(
    *,
    artwork: Artwork,
    template: ProductTemplate,
    variant_rows: List[Dict[str, Any]],
    launch_plan_row: str = "",
    launch_plan_row_id: str = "",
    collection_handle: str = "",
    collection_title: str = "",
    campaign: str = "",
    merch_theme: str = "",
) -> StorefrontQaRow:
    context = build_seo_context(template, artwork)
    title = render_product_title(template, artwork)
    description_html = render_product_description(template, artwork)
    tags = _render_listing_tags(template, artwork)
    product_options, variant_payloads = build_shopify_product_options(template, variant_rows)
    publish_payload = build_printify_publish_payload(template)
    placement_bits: List[str] = []
    for placement in template.placements:
        transform = compute_placement_transform_for_artwork(placement, artwork, template.key)
        placement_bits.append(
            f"{placement.placement_name}:mode={placement.artwork_fit_mode}:scale={transform.scale:.3f}:xy=({transform.x:.3f},{transform.y:.3f})"
        )
    placement_context = "; ".join(placement_bits)

    title_warnings, title_errors = validate_storefront_title(
        title=title,
        title_source=str(context.get("title_source") or ""),
        title_quality=str(context.get("title_quality") or ""),
        artwork=artwork,
        template=template,
    )
    description_warnings, description_errors = validate_storefront_description(
        description_html=description_html,
        template=template,
        artwork=artwork,
    )
    tag_warnings, tag_errors = validate_storefront_tags(tags=tags, template=template, artwork=artwork)
    pricing_warnings, pricing_errors, pricing_summary = validate_storefront_pricing(template=template, variant_rows=variant_rows)
    option_warnings, option_errors, option_names = validate_storefront_options(
        template=template,
        variant_rows=variant_rows,
        product_options=product_options,
        variant_payloads=variant_payloads,
    )
    mockup_warnings, mockup_errors = validate_storefront_mockups(
        template=template,
        publish_payload=publish_payload,
        placement_context=placement_context,
    )

    warning_messages = [*title_warnings, *description_warnings, *tag_warnings, *pricing_warnings, *option_warnings, *mockup_warnings]
    error_messages = [*title_errors, *description_errors, *tag_errors, *pricing_errors, *option_errors, *mockup_errors]
    qa_status = "fail" if error_messages else ("warn" if warning_messages else "pass")
    compare_at_valid = (pricing_summary["compare_min"] is None) or (
        (pricing_summary["sale_min"] is not None) and (pricing_summary["compare_min"] > pricing_summary["sale_min"])
    )
    recommended_action = "review" if error_messages else ("consider_tuning" if warning_messages else "none")

    return StorefrontQaRow(
        artwork_filename=artwork.src_path.name,
        artwork_slug=artwork.slug,
        template_key=template.key,
        title=title,
        title_source=str(context.get("title_source") or ""),
        title_quality=str(context.get("title_quality") or ""),
        title_warnings=_warnings_to_text(title_warnings),
        description_preview=_description_excerpt(description_html),
        description_warnings=_warnings_to_text(description_warnings),
        tags_preview=", ".join(tags[:20]),
        tag_count=len(tags),
        tag_warnings=_warnings_to_text([*tag_warnings, *option_warnings]),
        blueprint_id=template.printify_blueprint_id,
        provider_id=template.printify_print_provider_id,
        enabled_variant_count=len(variant_rows),
        option_names=", ".join(option_names),
        sale_price_min=_money_str(pricing_summary["sale_min"]),
        sale_price_max=_money_str(pricing_summary["sale_max"]),
        compare_at_min=_money_str(pricing_summary["compare_min"]),
        compare_at_max=_money_str(pricing_summary["compare_max"]),
        pricing_warnings=_warnings_to_text(pricing_warnings),
        compare_at_valid=compare_at_valid,
        publish_images=bool(publish_payload.get("images")),
        publish_mockups="" if template.publish_mockups is None else str(bool(template.publish_mockups)).lower(),
        mockup_warnings=_warnings_to_text(mockup_warnings),
        placement_preview_context=placement_context,
        qa_status=qa_status,
        qa_warning_count=len(warning_messages),
        qa_error_count=len(error_messages),
        recommended_action=recommended_action,
        launch_plan_row=launch_plan_row,
        launch_plan_row_id=launch_plan_row_id,
        collection_handle=collection_handle,
        collection_title=collection_title,
        campaign=campaign,
        merch_theme=merch_theme,
    )


def _log_storefront_qa_summary(rows: List[StorefrontQaRow]) -> None:
    status_counts = {
        "pass": sum(1 for row in rows if row.qa_status == "pass"),
        "warn": sum(1 for row in rows if row.qa_status == "warn"),
        "fail": sum(1 for row in rows if row.qa_status == "fail"),
    }
    warning_counter: Dict[str, int] = {}
    for row in rows:
        combined = " | ".join(filter(None, [row.title_warnings, row.description_warnings, row.tag_warnings, row.pricing_warnings, row.mockup_warnings]))
        for token in [part.strip() for part in combined.split("|") if part.strip()]:
            warning_counter[token] = warning_counter.get(token, 0) + 1
    top = sorted(warning_counter.items(), key=lambda item: item[1], reverse=True)[:5]
    top_text = ", ".join(f"{name}:{count}" for name, count in top) if top else "none"
    logger.info(
        "Storefront QA summary rows_checked=%s passed=%s warnings=%s failed=%s top_warning_categories=%s",
        len(rows),
        status_counts["pass"],
        status_counts["warn"],
        status_counts["fail"],
        top_text,
    )


def run_storefront_qa(
    *,
    printify: PrintifyClient,
    artworks: List[Artwork],
    templates: List[ProductTemplate],
    launch_plan_rows: Optional[List[LaunchPlanRow]] = None,
    launch_plan_image_dir: Optional[pathlib.Path] = None,
    export_csv_path: str = "",
    export_json_path: str = "",
) -> List[StorefrontQaRow]:
    qa_rows: List[StorefrontQaRow] = []
    if launch_plan_rows:
        if launch_plan_image_dir is None:
            raise RuntimeError("launch_plan_image_dir is required when launch_plan_rows are provided")
        template_by_key = {template.key: template for template in templates}
        metadata_map = load_artwork_metadata_map(ARTWORK_METADATA_MAP_PATH)
        for launch_row in launch_plan_rows:
            artwork_path = _resolve_artwork_path_for_launch_plan(launch_row.artwork_file, launch_plan_image_dir)
            with Image.open(artwork_path) as im:
                width, height = im.size
            metadata, _ = resolve_artwork_metadata_with_source(artwork_path, metadata_map, artwork_slug=slugify(artwork_path.stem))
            artwork = Artwork(
                slug=slugify(artwork_path.stem),
                src_path=artwork_path,
                title=filename_slug_to_title(artwork_path.stem),
                description_html=f"<p>{filename_slug_to_title(artwork_path.stem)}</p>",
                tags=DEFAULT_TAGS.copy(),
                image_width=width,
                image_height=height,
                metadata=metadata,
            )
            template = build_resolved_template(template_by_key[launch_row.template_key], launch_row.overrides)
            variant_rows = choose_variants_from_catalog(
                printify.list_variants(template.printify_blueprint_id, template.printify_print_provider_id),
                template,
            )
            qa_rows.append(
                build_storefront_qa_row(
                    artwork=artwork,
                    template=template,
                    variant_rows=variant_rows,
                    launch_plan_row=str(launch_row.row_number),
                    launch_plan_row_id=launch_row.row_id,
                    collection_handle=launch_row.collection_handle,
                    collection_title=launch_row.collection_title,
                    campaign=launch_row.campaign,
                    merch_theme=launch_row.merch_theme,
                )
            )
    else:
        for artwork in artworks:
            for template in templates:
                variant_rows = choose_variants_from_catalog(
                    printify.list_variants(template.printify_blueprint_id, template.printify_print_provider_id),
                    template,
                )
                qa_rows.append(build_storefront_qa_row(artwork=artwork, template=template, variant_rows=variant_rows))

    _log_storefront_qa_summary(qa_rows)
    if export_csv_path:
        write_csv_report(pathlib.Path(export_csv_path), [row.__dict__ for row in qa_rows])
        logger.info("Storefront QA CSV exported path=%s rows=%s", export_csv_path, len(qa_rows))
    if export_json_path:
        save_json_atomic(pathlib.Path(export_json_path), [row.__dict__ for row in qa_rows])
        logger.info("Storefront QA JSON exported path=%s rows=%s", export_json_path, len(qa_rows))
    return qa_rows


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


def process_artwork(*, printify: PrintifyClient, shopify: Optional[ShopifyClient], shop_id: Optional[int], artwork: Artwork, templates: List[ProductTemplate], state: Dict[str, Any], force: bool, export_dir: pathlib.Path, state_path: pathlib.Path, artwork_options: ArtworkProcessingOptions, upload_strategy: str, r2_config: Optional[R2Config], create_only: bool = False, update_only: bool = False, rebuild_product: bool = False, publish_mode: str = "default", verify_publish: bool = False, auto_rebuild_on_incompatible_update: bool = False, sync_collections: bool = False, verify_collections: bool = False, summary: Optional[RunSummary] = None, failure_rows: Optional[List[FailureReportRow]] = None, run_rows: Optional[List[RunReportRow]] = None, launch_plan_row: str = "", launch_plan_row_id: str = "", collection_handle: str = "", collection_title: str = "", collection_description: str = "", launch_name: str = "", campaign: str = "", merch_theme: str = "") -> None:
    processed = state.setdefault("processed", {})
    existing = processed.get(artwork.slug, {})
    existing_products = existing.get("products", []) if isinstance(existing.get("products", []), list) else []

    logger.info("Processing artwork: %s", artwork.src_path.name)
    if launch_plan_row or launch_plan_row_id:
        logger.info("Launch-plan context row=%s row_id=%s", launch_plan_row or "-", launch_plan_row_id or "-")
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
        source_size_report = ""
        trimmed_bounds_report = ""
        trim_skip_reason_report = ""
        exported_canvas_report = ""
        placement_scale_report = ""
        effective_upscale_factor_report = ""
        requested_upscale_factor_report = ""
        applied_upscale_factor_report = ""
        upscale_capped_report = False
        subject_bounds_before_aggressive_trim_report = ""
        subject_bounds_after_aggressive_trim_report = ""
        subject_fill_target_report = ""
        aggressive_trim_used_report = False
        orientation_report = _orientation_bucket(artwork.image_width, artwork.image_height)
        try:
            resolved_template = template
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
                    "dry_run": bool(printify.dry_run),
                    "completion_status": "dry-run-only" if printify.dry_run else "real-completed",
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
                        launch_plan_row=launch_plan_row,
                        launch_plan_row_id=launch_plan_row_id,
                    ))
                log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=True, result=result, blueprint_id=template.printify_blueprint_id, provider_id=template.printify_print_provider_id, action="skip", upload_map=upload_map)
                continue

            if template.key == "tote_basic":
                resolved_template, catalog_variants = resolve_tote_template_catalog_mapping(
                    printify=printify,
                    template=template,
                )
            else:
                catalog_variants = printify.list_variants(template.printify_blueprint_id, template.printify_print_provider_id)
            variant_rows = choose_variants_from_catalog(catalog_variants, resolved_template)
            if not variant_rows:
                all_templates_successful = False
                if summary is not None:
                    summary.products_skipped += 1
                result = {"status": "no_matching_variants"}
                record["products"].append({"template": template.key, "state_key": state_key, "title_source": title_info.title_source, "rendered_title": rendered_title, "result": result})
                if run_rows is not None:
                    run_rows.append(RunReportRow(datetime.now(timezone.utc).isoformat(), artwork.src_path.name, artwork.slug, template.key, "skipped", "skip", resolved_template.printify_blueprint_id, resolved_template.printify_print_provider_id, upload_strategy, "", False, False, rendered_title, "", "", "", "", "", "", "", "", False, orientation_report, launch_plan_row, launch_plan_row_id, collection_handle, collection_title, collection_description, launch_name, campaign, merch_theme))
                log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": result}, blueprint_id=resolved_template.printify_blueprint_id, provider_id=resolved_template.printify_print_provider_id, action="skip", upload_map=upload_map)
                continue

            prepared_assets: List[PreparedArtwork] = []
            skipped_placements: List[str] = []
            for placement in resolved_template.placements:
                prepared = prepare_artwork_export(artwork, resolved_template, placement, export_dir, artwork_options)
                if prepared is None:
                    skipped_placements.append(placement.placement_name)
                    continue
                prepared_assets.append(prepared)

            if prepared_assets:
                first_prepared = prepared_assets[0]
                source_size_report = f"{first_prepared.source_size[0]}x{first_prepared.source_size[1]}"
                if first_prepared.trimmed_size:
                    trimmed_bounds_report = f"{first_prepared.trimmed_size[0]}x{first_prepared.trimmed_size[1]}"
                if first_prepared.trim_skip_reason:
                    trim_skip_reason_report = first_prepared.trim_skip_reason
                exported_canvas_report = f"{first_prepared.exported_canvas_size[0]}x{first_prepared.exported_canvas_size[1]}"
                placement_transform = compute_placement_transform_for_artwork(first_prepared.placement, artwork, template.key)
                placement_scale_report = f"{placement_transform.scale:.3f}"
                effective_upscale_factor_report = f"{first_prepared.effective_upscale_factor:.3f}"
                requested_upscale_factor_report = f"{first_prepared.requested_upscale_factor:.3f}"
                applied_upscale_factor_report = f"{first_prepared.applied_upscale_factor:.3f}"
                upscale_capped_report = first_prepared.upscale_capped
                if first_prepared.subject_bounds_before_aggressive_trim:
                    b = first_prepared.subject_bounds_before_aggressive_trim
                    subject_bounds_before_aggressive_trim_report = f"{b[0]},{b[1]},{b[2]},{b[3]}"
                if first_prepared.subject_bounds_after_aggressive_trim:
                    b = first_prepared.subject_bounds_after_aggressive_trim
                    subject_bounds_after_aggressive_trim_report = f"{b[0]},{b[1]},{b[2]},{b[3]}"
                if first_prepared.subject_fill_target is not None:
                    subject_fill_target_report = f"{first_prepared.subject_fill_target:.3f}"
                aggressive_trim_used_report = first_prepared.aggressive_trim_used

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
                    run_rows.append(RunReportRow(datetime.now(timezone.utc).isoformat(), artwork.src_path.name, artwork.slug, template.key, "skipped", "skip", resolved_template.printify_blueprint_id, resolved_template.printify_print_provider_id, upload_strategy, "", False, False, rendered_title, source_size_report, trimmed_bounds_report, "", exported_canvas_report, placement_scale_report, effective_upscale_factor_report, requested_upscale_factor_report, applied_upscale_factor_report, upscale_capped_report, orientation_report, launch_plan_row, launch_plan_row_id, collection_handle, collection_title, collection_description, launch_name, campaign, merch_theme))
                log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": result}, blueprint_id=resolved_template.printify_blueprint_id, provider_id=resolved_template.printify_print_provider_id, action="skip", upload_map=upload_map)
                continue

            upload_map = upload_assets_to_printify(printify, state, artwork, resolved_template, prepared_assets, state_path, upload_strategy, r2_config)

            result: Dict[str, Any] = {}
            result["printify"] = upsert_in_printify(
                printify=printify,
                shop_id=shop_id,
                artwork=artwork,
                template=resolved_template,
                variant_rows=variant_rows,
                upload_map=upload_map,
                existing_product_id=existing_product_id,
                action=action,
                publish_mode=publish_mode,
                verify_publish=verify_publish,
                auto_rebuild_on_incompatible_update=auto_rebuild_on_incompatible_update,
            ) if (resolved_template.push_via_printify and shop_id is not None) else {"status": "prepared_only", "action": action, "publish_attempted": False, "publish_verified": False}
            if template.publish_to_shopify and shopify is not None:
                result["shopify"] = create_in_shopify_only(shopify, artwork, template, variant_rows)

            printify_result = result.get("printify", {}) if isinstance(result.get("printify"), dict) else {}
            collection_result = {
                "collection_sync_attempted": False,
                "collection_sync_status": "skipped_disabled",
                "collection_id": "",
                "collection_handle": collection_handle,
                "collection_title": collection_title,
                "collection_membership_verified": False,
                "collection_warning": "",
                "collection_error": "",
            }
            if sync_collections:
                shopify_product_id = str(
                    (
                        (result.get("shopify", {}) if isinstance(result.get("shopify"), dict) else {}).get("shopify_product_id")
                        or ""
                    )
                ).strip()
                collection_result = sync_shopify_collection(
                    shopify=shopify,
                    shopify_product_id=shopify_product_id,
                    collection_handle=collection_handle,
                    collection_title=collection_title,
                    collection_description=collection_description,
                    verify_membership=verify_collections,
                )
            elif collection_handle.strip() or collection_title.strip():
                collection_result["collection_warning"] = "Collection sync disabled (use --sync-collections)"
                logger.info(
                    "Collection sync skipped by CLI setting artwork=%s template=%s row_id=%s",
                    artwork.slug,
                    template.key,
                    launch_plan_row_id or "-",
                )
            result["collection"] = collection_result
            verification = printify_result.get("verification", {}) if isinstance(printify_result.get("verification"), dict) else {}
            verification_warnings = verification.get("warnings", []) if isinstance(verification.get("warnings", []), list) else []
            row = {
                "template": template.key,
                "state_key": state_key,
                "blueprint_id": resolved_template.printify_blueprint_id,
                "print_provider_id": resolved_template.printify_print_provider_id,
                "last_action": printify_result.get("action", action),
                "publish_attempted": bool(printify_result.get("publish_attempted", False)),
                "publish_verified": bool(printify_result.get("publish_verified", False)),
                "last_verified_at": datetime.now(timezone.utc).isoformat() if verification else None,
                "verified_title": verification.get("verified_title"),
                "verified_variant_count": verification.get("verified_variant_count"),
                "title_source": title_info.title_source,
                "rendered_title": rendered_title,
                "launch_plan_row": launch_plan_row,
                "launch_plan_row_id": launch_plan_row_id,
                "collection_handle": collection_handle,
                "collection_title": collection_title,
                "collection_description": collection_description,
                "launch_name": launch_name,
                "campaign": campaign,
                "merch_theme": merch_theme,
                "collection_sync_attempted": bool(collection_result.get("collection_sync_attempted", False)),
                "collection_sync_status": str(collection_result.get("collection_sync_status") or ""),
                "shopify_collection_id": str(collection_result.get("collection_id") or ""),
                "collection_membership_verified": bool(collection_result.get("collection_membership_verified", False)),
                "collection_warning": str(collection_result.get("collection_warning") or ""),
                "collection_error": str(collection_result.get("collection_error") or ""),
                "result": result,
                "dry_run": bool(printify.dry_run),
                "completion_status": "dry-run-only" if printify.dry_run else "real-completed",
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
                    blueprint_id=resolved_template.printify_blueprint_id,
                    provider_id=resolved_template.printify_print_provider_id,
                    upload_strategy=summarize_upload_strategy(upload_map),
                    product_id=str(printify_result.get("printify_product_id") or ""),
                    publish_attempted=bool(row.get("publish_attempted")),
                    publish_verified=bool(row.get("publish_verified")),
                    rendered_title=rendered_title,
                    source_size=source_size_report,
                    trimmed_bounds_size=trimmed_bounds_report,
                    trim_skip_reason=trim_skip_reason_report,
                    exported_canvas_size=exported_canvas_report,
                    placement_scale_used=placement_scale_report,
                    effective_upscale_factor=effective_upscale_factor_report,
                    requested_upscale_factor=requested_upscale_factor_report,
                    applied_upscale_factor=applied_upscale_factor_report,
                    upscale_capped=upscale_capped_report,
                    orientation_bucket=orientation_report,
                    launch_plan_row=launch_plan_row,
                    launch_plan_row_id=launch_plan_row_id,
                    collection_handle=collection_handle,
                    collection_title=collection_title,
                    collection_description=collection_description,
                    launch_name=launch_name,
                    campaign=campaign,
                    merch_theme=merch_theme,
                    subject_bounds_before_aggressive_trim=subject_bounds_before_aggressive_trim_report,
                    subject_bounds_after_aggressive_trim=subject_bounds_after_aggressive_trim_report,
                    subject_fill_target=subject_fill_target_report,
                    aggressive_trim_used=aggressive_trim_used_report,
                    collection_sync_attempted=bool(collection_result.get("collection_sync_attempted", False)),
                    collection_sync_status=str(collection_result.get("collection_sync_status") or ""),
                    shopify_collection_id=str(collection_result.get("collection_id") or ""),
                    collection_membership_verified=bool(collection_result.get("collection_membership_verified", False)),
                    collection_warning=str(collection_result.get("collection_warning") or ""),
                    collection_error=str(collection_result.get("collection_error") or ""),
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
            log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=True, result=result, blueprint_id=resolved_template.printify_blueprint_id, provider_id=resolved_template.printify_print_provider_id, action=(result.get("printify", {}) or {}).get("action", action), upload_map=upload_map)
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
                "launch_plan_row": launch_plan_row,
                "launch_plan_row_id": launch_plan_row_id,
                "collection_handle": collection_handle,
                "collection_title": collection_title,
                "collection_description": collection_description,
                "launch_name": launch_name,
                "campaign": campaign,
                "merch_theme": merch_theme,
                "result": error_result,
                "dry_run": bool(printify.dry_run),
                "completion_status": "failure",
            })
            if failure_rows is not None:
                failure_rows.append(FailureReportRow(
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    artwork_filename=artwork.src_path.name,
                    artwork_slug=artwork.slug,
                    template_key=template.key,
                    action_attempted=action,
                    blueprint_id=resolved_template.printify_blueprint_id,
                    provider_id=resolved_template.printify_print_provider_id,
                    upload_strategy=summarize_upload_strategy(upload_map),
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    suggested_next_action="Inspect state and rerun with --resume after fixing template or artwork",
                    launch_plan_row=launch_plan_row,
                    launch_plan_row_id=launch_plan_row_id,
                ))
            if run_rows is not None:
                run_rows.append(RunReportRow(datetime.now(timezone.utc).isoformat(), artwork.src_path.name, artwork.slug, template.key, "failure", action, resolved_template.printify_blueprint_id, resolved_template.printify_print_provider_id, summarize_upload_strategy(upload_map), "", False, False, rendered_title, source_size_report, trimmed_bounds_report, "", exported_canvas_report, placement_scale_report, effective_upscale_factor_report, requested_upscale_factor_report, applied_upscale_factor_report, upscale_capped_report, orientation_report, launch_plan_row, launch_plan_row_id, collection_handle, collection_title, collection_description, launch_name, campaign, merch_theme))
            log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": error_result}, blueprint_id=resolved_template.printify_blueprint_id, provider_id=resolved_template.printify_print_provider_id, action=action, upload_map=upload_map)
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
    parser.add_argument("--generate-artwork-metadata", action="store_true", help="Generate reviewable artwork sidecar metadata from local image analysis")
    parser.add_argument("--metadata-preview", action="store_true", help="Preview generated artwork metadata in stdout")
    parser.add_argument("--write-sidecars", action="store_true", help="Write generated artwork sidecar JSON files")
    parser.add_argument("--overwrite-sidecars", action="store_true", help="Allow generated metadata to overwrite existing sidecars")
    parser.add_argument("--metadata-max-artworks", type=int, default=0, help="Limit number of artworks processed for metadata generation (0 = no limit)")
    parser.add_argument("--metadata-output-dir", default="", help="Optional output directory for generated sidecars; defaults beside images")
    parser.add_argument("--metadata-only-missing", action="store_true", default=True, help="Write only missing sidecars (default safe behavior)")
    parser.add_argument(
        "--metadata-generator",
        choices=[mode.value for mode in MetadataGeneratorMode],
        default=MetadataGeneratorMode.HEURISTIC.value,
        help="Metadata generation strategy: heuristic, vision, openai, or auto (openai -> vision -> heuristic fallback)",
    )
    parser.add_argument(
        "--metadata-openai-model",
        default="",
        help="Optional OpenAI model override for metadata generation (defaults to OPENAI_MODEL or built-in default)",
    )
    parser.add_argument(
        "--metadata-openai-timeout",
        type=float,
        default=30.0,
        help="OpenAI metadata request timeout in seconds",
    )
    parser.add_argument("--metadata-auto-approve", action="store_true", help="Enable confidence/quality gated metadata approval workflow")
    parser.add_argument("--metadata-min-confidence", type=float, default=0.9, help="Minimum confidence required for metadata auto-approval")
    parser.add_argument("--metadata-review-report", default="", help="Optional CSV path for metadata review queue export")
    parser.add_argument("--metadata-review-json", default="", help="Optional JSON path for metadata review queue export")
    parser.add_argument("--metadata-write-auto-approved-only", action="store_true", help="When writing sidecars, write only auto-approved metadata candidates")
    parser.add_argument("--metadata-allow-review-writes", action="store_true", help="Allow writes for needs_review metadata candidates (off by default)")
    parser.add_argument("--generate-artwork-from-prompt", action="store_true", help="Generate artwork source image(s) from a text prompt before normal pipeline processing")
    parser.add_argument("--art-prompt", default="", help="Prompt used for artwork generation mode")
    parser.add_argument("--art-count", type=int, default=1, help="Number of concept sets to generate")
    parser.add_argument("--art-style", default="", help="Optional style hint appended to the art prompt")
    parser.add_argument("--art-negative-prompt", default="", help="Optional constraints describing what to avoid in generated artwork")
    parser.add_argument("--art-visible-text", default="", help="Optional exact visible text to include in the artwork")
    parser.add_argument("--art-output-dir", default="", help="Output directory for generated artwork (defaults to --image-dir)")
    parser.add_argument("--art-base-name", default="generated-art", help="Base filename slug for generated artwork files")
    parser.add_argument("--art-quality", choices=["low", "medium", "high"], default="high", help="OpenAI image quality level")
    parser.add_argument("--art-background", choices=["auto", "transparent", "opaque"], default="auto", help="OpenAI image background mode")
    parser.add_argument("--art-generator", choices=["openai"], default="openai", help="Artwork generation provider")
    parser.add_argument("--art-openai-model", default="", help="Optional OpenAI model override for image generation")
    parser.add_argument("--art-run-metadata", action="store_true", help="Generate sidecar metadata for newly generated artwork before exiting/continuing")
    parser.add_argument("--art-run-storefront-qa", action="store_true", help="After generation, run storefront QA against generated artwork")
    parser.add_argument("--art-publish", action="store_true", help="After generation, run full create/update flow and force publish")
    parser.add_argument("--art-verify-publish", action="store_true", help="After generation, verify publish/readback indicators")
    parser.add_argument("--art-target-mode", choices=["auto", "portrait", "square", "multi"], default="auto", help="Target aspect planning mode for generated artwork")
    parser.add_argument("--art-skip-existing-generated", action="store_true", help="Skip OpenAI generation when expected output filename already exists")
    parser.add_argument("--art-dry-run-plan", action="store_true", help="Only print generation targets/plan and exit without calling the image API")
    parser.add_argument("--source-min-width", type=int, default=1, help="Minimum source width allowed when scanning artwork files")
    parser.add_argument("--source-min-height", type=int, default=1, help="Minimum source height allowed when scanning artwork files")
    parser.add_argument("--include-preview-assets", action="store_true", help="Include preview/thumbnail/removebg-preview files in artwork discovery")
    parser.add_argument("--launch-plan", default="", help="Optional CSV launch plan path; when set, process enabled rows instead of folder-scan combinations")
    parser.add_argument("--export-launch-plan-template", default="", help="Write a starter launch-plan CSV template to this path and exit")
    parser.add_argument("--export-launch-plan-from-images", default="", help="Write a launch-plan CSV generated from files in --image-dir and exit")
    parser.add_argument("--include-disabled-template-rows", action="store_true", help="Also include disabled rows when exporting launch-plan CSV from images")
    parser.add_argument("--launch-plan-default-enabled", choices=["true", "false"], default="true", help="Default enabled value used by --export-launch-plan-from-images")
    parser.add_argument("--placement-preview", action="store_true", help="Render local placement QA previews to exports/previews")
    parser.add_argument("--storefront-qa", action="store_true", help="Run read-only storefront QA and skip create/update/publish mutations")
    parser.add_argument("--strict-storefront-qa", action="store_true", help="Return exit code 1 when storefront QA finds errors")
    parser.add_argument("--export-storefront-qa-report", default="", help="Optional CSV export path for storefront QA rows")
    parser.add_argument("--export-storefront-qa-json", default="", help="Optional JSON export path for storefront QA rows")
    parser.add_argument("--sync-collections", action="store_true", help="Sync Shopify custom collections from launch-plan collection metadata")
    parser.add_argument("--skip-collections", action="store_true", help="Explicitly disable Shopify collection sync")
    parser.add_argument("--verify-collections", action="store_true", help="Read-only verify Shopify collection membership after sync")
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
            rendered = json.dumps([snippet], indent=2)
            if template_output_file:
                pathlib.Path(template_output_file).write_text(rendered + "\n", encoding="utf-8")
            else:
                print(rendered)
            return True

    return False


def run(config_path: pathlib.Path, *, dry_run: bool = False, force: bool = False, allow_upscale: bool = False, upscale_method: str = "lanczos", skip_undersized: bool = False, image_dir: pathlib.Path = IMAGE_DIR, export_dir: pathlib.Path = EXPORT_DIR, state_path: pathlib.Path = STATE_PATH, skip_audit: bool = False, max_artworks: int = 0, batch_size: int = 0, stop_after_failures: int = 0, fail_fast: bool = False, resume: bool = False, upload_strategy: str = "auto", template_keys: Optional[List[str]] = None, limit_templates: int = 0, list_templates: bool = False, list_blueprints: bool = False, search_blueprints_query: str = "", limit_blueprints: int = 25, list_providers: bool = False, blueprint_id: int = 0, provider_id: int = 0, limit_providers: int = 25, inspect_variants: bool = False, recommend_provider: bool = False, template_file: str = "", generate_template_snippet_flag: bool = False, auto_provider: bool = False, snippet_key: str = "", template_output_file: str = "", create_only: bool = False, update_only: bool = False, rebuild_product: bool = False, publish_mode: str = "default", verify_publish: bool = False, auto_rebuild_on_incompatible_update: bool = False, sync_collections: bool = False, skip_collections: bool = False, verify_collections: bool = False, inspect_state_key_value: str = "", list_state_keys_only: bool = False, list_failures_only: bool = False, list_pending_only: bool = False, export_failure_report: str = "", export_run_report: str = "", preview_listing_copy_only: bool = False, generate_artwork_metadata: bool = False, metadata_preview: bool = False, write_sidecars: bool = False, overwrite_sidecars: bool = False, metadata_max_artworks: int = 0, metadata_output_dir: str = "", metadata_only_missing: bool = True, metadata_generator: str = MetadataGeneratorMode.HEURISTIC.value, metadata_openai_model: str = "", metadata_openai_timeout: float = 30.0, metadata_auto_approve: bool = False, metadata_min_confidence: float = 0.9, metadata_review_report: str = "", metadata_review_json: str = "", metadata_write_auto_approved_only: bool = False, metadata_allow_review_writes: bool = False, generate_artwork_from_prompt: bool = False, art_prompt: str = "", art_count: int = 1, art_style: str = "", art_negative_prompt: str = "", art_visible_text: str = "", art_output_dir: str = "", art_base_name: str = "generated-art", art_quality: str = "high", art_background: str = "auto", art_generator: str = "openai", art_openai_model: str = "", art_run_metadata: bool = False, art_run_storefront_qa: bool = False, art_publish: bool = False, art_verify_publish: bool = False, art_target_mode: str = "auto", art_skip_existing_generated: bool = False, art_dry_run_plan: bool = False, source_min_width: int = 1, source_min_height: int = 1, include_preview_assets: bool = False, launch_plan_path: str = "", export_launch_plan_template: str = "", export_launch_plan_from_images_path: str = "", include_disabled_template_rows: bool = False, launch_plan_default_enabled: bool = True, placement_preview: bool = False, storefront_qa: bool = False, strict_storefront_qa: bool = False, export_storefront_qa_report: str = "", export_storefront_qa_json: str = "") -> None:
    if publish_mode not in {"default", "publish", "skip"}:
        raise RuntimeError(f"Unsupported publish mode: {publish_mode}")

    if export_launch_plan_template:
        write_launch_plan_template(pathlib.Path(export_launch_plan_template))
        logger.info("Launch-plan CSV template exported path=%s", export_launch_plan_template)
        return

    if generate_artwork_metadata:
        run_artwork_metadata_generation(
            image_dir=image_dir,
            metadata_preview=metadata_preview,
            write_sidecars=write_sidecars,
            overwrite_sidecars=overwrite_sidecars,
            metadata_only_missing=metadata_only_missing,
            metadata_max_artworks=metadata_max_artworks,
            metadata_output_dir=metadata_output_dir,
            metadata_generator=metadata_generator,
            metadata_openai_model=metadata_openai_model,
            metadata_openai_timeout=metadata_openai_timeout,
            metadata_auto_approve=metadata_auto_approve,
            metadata_min_confidence=metadata_min_confidence,
            metadata_review_report=metadata_review_report,
            metadata_review_json=metadata_review_json,
            metadata_write_auto_approved_only=metadata_write_auto_approved_only,
            metadata_allow_review_writes=metadata_allow_review_writes,
        )
        return

    state = ensure_state_shape(load_json(state_path, {}))
    if list_state_keys_only:
        index = latest_rows_by_state_key(state)
        for key in list_state_keys(state):
            row = index.get(key, {})
            print(f"{key}	{row_completion_label(row)}")
        return
    if inspect_state_key_value:
        row = inspect_state_key(state, inspect_state_key_value)
        if row is None:
            print(json.dumps({"state_key": inspect_state_key_value, "found": False}, ensure_ascii=False))
        else:
            view = dict(row)
            view.setdefault("completion_status", row_completion_label(row))
            print(json.dumps(view, indent=2, ensure_ascii=False))
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
    if template_keys and not templates:
        available_template_keys = ", ".join(template.key for template in load_templates(config_path)) or "(none)"
        requested_template_keys = ", ".join(key.strip() for key in template_keys if key.strip()) or "(none)"
        raise RuntimeError(
            f"No templates matched the requested --template-key values: {requested_template_keys}. "
            f"Available template keys: {available_template_keys}"
        )
    source_hygiene = SourceHygieneOptions(
        filter_preview_assets=not include_preview_assets,
        min_source_width=max(1, int(source_min_width)),
        min_source_height=max(1, int(source_min_height)),
    )
    generated_paths: Optional[List[pathlib.Path]] = None
    if generate_artwork_from_prompt:
        if not art_prompt.strip():
            raise RuntimeError("--art-prompt is required when --generate-artwork-from-prompt is set")
        output_dir = pathlib.Path(art_output_dir) if art_output_dir else image_dir
        request = ArtworkGenerationRequest(
            prompt=art_prompt,
            count=max(1, int(art_count)),
            style=art_style,
            negative_prompt=art_negative_prompt,
            visible_text=art_visible_text,
            quality=art_quality,
            background=art_background,
            generator=art_generator,
            openai_model=art_openai_model,
            base_name=slugify(art_base_name or "generated-art"),
            output_dir=output_dir,
            target_mode=art_target_mode,
            dry_run_plan=art_dry_run_plan,
            skip_existing_generated=art_skip_existing_generated,
            min_source_width=source_hygiene.min_source_width,
            min_source_height=source_hygiene.min_source_height,
        )
        generated_paths = run_prompt_artwork_generation(request=request, templates=templates)
        if art_dry_run_plan:
            return
        if art_run_metadata and generated_paths:
            run_artwork_metadata_generation(
                image_dir=output_dir,
                metadata_preview=False,
                write_sidecars=True,
                overwrite_sidecars=overwrite_sidecars,
                metadata_only_missing=metadata_only_missing,
                metadata_max_artworks=0,
                metadata_output_dir="",
                metadata_generator=MetadataGeneratorMode.OPENAI.value,
                metadata_openai_model=metadata_openai_model,
                metadata_openai_timeout=metadata_openai_timeout,
                metadata_auto_approve=metadata_auto_approve,
                metadata_min_confidence=metadata_min_confidence,
                metadata_review_report="",
                metadata_review_json="",
                metadata_write_auto_approved_only=metadata_write_auto_approved_only,
                metadata_allow_review_writes=metadata_allow_review_writes,
                artwork_paths=generated_paths,
            )
        should_continue = bool(art_run_storefront_qa or art_publish or create_only or update_only or rebuild_product)
        if not should_continue:
            logger.info("Prompt artwork generation complete. Stopping before create/publish flow.")
            return
        image_dir = output_dir
        storefront_qa = storefront_qa or art_run_storefront_qa
        if art_publish:
            publish_mode = "publish"
        if art_verify_publish:
            verify_publish = True

    if export_launch_plan_from_images_path:
        exported_count = export_launch_plan_from_images(
            path=pathlib.Path(export_launch_plan_from_images_path),
            image_dir=image_dir,
            templates=templates,
            include_disabled_template_rows=include_disabled_template_rows,
            default_enabled=launch_plan_default_enabled,
        )
        logger.info("Launch-plan CSV exported from images path=%s rows=%s", export_launch_plan_from_images_path, exported_count)
        return
    try:
        artworks = discover_artworks(image_dir, candidate_paths=generated_paths, source_hygiene=source_hygiene)
    except TypeError:
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
    collection_sync_enabled = bool(sync_collections and not skip_collections)
    r2_config = load_r2_config_from_env()

    if not skip_audit:
        audit_printify_integration(printify, templates, shop_id)

    if storefront_qa:
        launch_rows: Optional[List[LaunchPlanRow]] = None
        if launch_plan_path:
            launch_rows, validation_failures = resolve_launch_plan_rows(
                launch_plan_path=pathlib.Path(launch_plan_path),
                templates=templates,
                image_dir=image_dir,
            )
            if validation_failures:
                logger.warning("Storefront QA launch-plan validation had %s failure row(s); skipping invalid rows", len(validation_failures))
        qa_rows = run_storefront_qa(
            printify=printify,
            artworks=artworks,
            templates=templates,
            launch_plan_rows=launch_rows,
            launch_plan_image_dir=image_dir,
            export_csv_path=export_storefront_qa_report,
            export_json_path=export_storefront_qa_json,
        )
        if strict_storefront_qa and any(row.qa_error_count > 0 for row in qa_rows):
            raise RuntimeError("Strict storefront QA failed due to one or more QA errors")
        return

    logger.info("Loaded %s template(s) and %s artwork file(s)", len(templates), len(artworks))
    summary = RunSummary(artworks_scanned=len(artworks))
    failure_rows: List[FailureReportRow] = []
    run_rows: List[RunReportRow] = []
    artwork_options = ArtworkProcessingOptions(allow_upscale=allow_upscale, upscale_method=upscale_method, skip_undersized=skip_undersized, placement_preview=placement_preview, preview_dir=export_dir / "previews")
    combinations_processed = 0
    stop_requested = False

    if launch_plan_path:
        template_by_key = {template.key: template for template in templates}
        metadata_map = load_artwork_metadata_map(ARTWORK_METADATA_MAP_PATH)
        artwork_by_path: Dict[str, Artwork] = {}
        for artwork in artworks:
            artwork_by_path[str(artwork.src_path.resolve())] = artwork
            artwork_by_path[str(artwork.src_path)] = artwork
            artwork_by_path[artwork.src_path.name] = artwork
            artwork_by_path[str(artwork.src_path.relative_to(image_dir))] = artwork if artwork.src_path.is_relative_to(image_dir) else artwork

        launch_rows, validation_failures = resolve_launch_plan_rows(
            launch_plan_path=pathlib.Path(launch_plan_path),
            templates=templates,
            image_dir=image_dir,
        )
        failure_rows.extend(validation_failures)
        summary.failures += len(validation_failures)

        for launch_row in launch_rows:
            if batch_size > 0 and combinations_processed >= batch_size:
                stop_requested = True
                break
            artwork_path = _resolve_artwork_path_for_launch_plan(launch_row.artwork_file, image_dir)
            lookup_keys = [str(artwork_path.resolve()), str(artwork_path), artwork_path.name]
            artwork = next((artwork_by_path.get(key) for key in lookup_keys if artwork_by_path.get(key) is not None), None)
            if artwork is None:
                with Image.open(artwork_path) as im:
                    width, height = im.size
                metadata, match_info = resolve_artwork_metadata_with_source(
                    artwork_path,
                    metadata_map,
                    artwork_slug=slugify(artwork_path.stem),
                )
                logger.info(
                    "Content metadata match artwork=%s source=%s key=%s",
                    artwork_path.name,
                    match_info.get("source", "unknown"),
                    match_info.get("key", ""),
                )
                title = filename_slug_to_title(artwork_path.stem)
                artwork = Artwork(
                    slug=slugify(artwork_path.stem),
                    src_path=artwork_path,
                    title=title,
                    description_html=f"<p>{title}</p>",
                    tags=DEFAULT_TAGS.copy(),
                    image_width=width,
                    image_height=height,
                    metadata=metadata,
                )
            template = build_resolved_template(template_by_key[launch_row.template_key], launch_row.overrides)
            if resume and is_state_key_successful(state, f"{artwork.slug}:{template.key}"):
                continue
            combinations_processed += 1
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
                sync_collections=collection_sync_enabled,
                verify_collections=verify_collections,
                summary=summary,
                failure_rows=failure_rows,
                run_rows=run_rows,
                launch_plan_row=str(launch_row.row_number),
                launch_plan_row_id=launch_row.row_id,
                collection_handle=launch_row.collection_handle,
                collection_title=launch_row.collection_title,
                collection_description=launch_row.collection_description,
                launch_name=launch_row.launch_name,
                campaign=launch_row.campaign,
                merch_theme=launch_row.merch_theme,
            )
            if summary.failures > before_failures:
                if fail_fast:
                    stop_requested = True
                    break
                if stop_after_failures > 0 and summary.failures >= stop_after_failures:
                    stop_requested = True
                    break
    else:
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
                    sync_collections=collection_sync_enabled,
                    verify_collections=verify_collections,
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
            sync_collections=args.sync_collections,
            skip_collections=args.skip_collections,
            verify_collections=args.verify_collections,
            inspect_state_key_value=args.inspect_state_key,
            list_state_keys_only=args.list_state_keys,
            list_failures_only=args.list_failures,
            list_pending_only=args.list_pending,
            export_failure_report=args.export_failure_report,
            export_run_report=args.export_run_report,
            preview_listing_copy_only=args.preview_listing_copy,
            generate_artwork_metadata=args.generate_artwork_metadata,
            metadata_preview=args.metadata_preview,
            write_sidecars=args.write_sidecars,
            overwrite_sidecars=args.overwrite_sidecars,
            metadata_max_artworks=args.metadata_max_artworks,
            metadata_output_dir=args.metadata_output_dir,
            metadata_only_missing=args.metadata_only_missing,
            metadata_generator=args.metadata_generator,
            metadata_openai_model=args.metadata_openai_model,
            metadata_openai_timeout=args.metadata_openai_timeout,
            metadata_auto_approve=args.metadata_auto_approve,
            metadata_min_confidence=args.metadata_min_confidence,
            metadata_review_report=args.metadata_review_report,
            metadata_review_json=args.metadata_review_json,
            metadata_write_auto_approved_only=args.metadata_write_auto_approved_only,
            metadata_allow_review_writes=args.metadata_allow_review_writes,
            generate_artwork_from_prompt=args.generate_artwork_from_prompt,
            art_prompt=args.art_prompt,
            art_count=args.art_count,
            art_style=args.art_style,
            art_negative_prompt=args.art_negative_prompt,
            art_visible_text=args.art_visible_text,
            art_output_dir=args.art_output_dir,
            art_base_name=args.art_base_name,
            art_quality=args.art_quality,
            art_background=args.art_background,
            art_generator=args.art_generator,
            art_openai_model=args.art_openai_model,
            art_run_metadata=args.art_run_metadata,
            art_run_storefront_qa=args.art_run_storefront_qa,
            art_publish=args.art_publish,
            art_verify_publish=args.art_verify_publish,
            art_target_mode=args.art_target_mode,
            art_skip_existing_generated=args.art_skip_existing_generated,
            art_dry_run_plan=args.art_dry_run_plan,
            source_min_width=args.source_min_width,
            source_min_height=args.source_min_height,
            include_preview_assets=args.include_preview_assets,
            launch_plan_path=args.launch_plan,
            export_launch_plan_template=args.export_launch_plan_template,
            export_launch_plan_from_images_path=args.export_launch_plan_from_images,
            include_disabled_template_rows=args.include_disabled_template_rows,
            launch_plan_default_enabled=(args.launch_plan_default_enabled == "true"),
            placement_preview=args.placement_preview,
            storefront_qa=args.storefront_qa,
            strict_storefront_qa=args.strict_storefront_qa,
            export_storefront_qa_report=args.export_storefront_qa_report,
            export_storefront_qa_json=args.export_storefront_qa_json,
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
