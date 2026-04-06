from __future__ import annotations

import argparse
import base64
from collections import Counter
import csv
import hashlib
import json
import logging
import math
import os
import pathlib
import random
import re
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field, replace
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests
from dotenv import load_dotenv
from PIL import Image, ImageOps
from r2_uploader import R2Config, build_r2_public_url, load_r2_config_from_env, upload_file_to_r2
import content_engine
import product_copy_generator
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
    APPAREL_FAMILY,
    BLANKET_FAMILY,
    POSTER_FAMILY,
    SQUARE_FAMILY,
    ArtworkGenerationRequest,
    GeneratedArtworkAsset,
    TemplateAssetRouting,
    choose_preferred_generated_asset,
    classify_template_family,
    is_preview_or_low_value_asset,
    plan_family_artwork_targets,
    plan_generated_artwork_targets,
    route_templates_to_generated_assets,
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
CATALOG_RETRY_BACKOFF_CAP_SECONDS = float(os.getenv("CATALOG_RETRY_BACKOFF_CAP_SECONDS", "15"))
MUTATION_RETRY_BACKOFF_CAP_SECONDS = float(os.getenv("MUTATION_RETRY_BACKOFF_CAP_SECONDS", "20"))
INTERACTIVE_RETRY_CAP_SECONDS = float(os.getenv("INTERACTIVE_RETRY_CAP_SECONDS", "12"))
MAX_RETRY_SLEEP_SECONDS = float(os.getenv("MAX_RETRY_SLEEP_SECONDS", "45"))
PRINTIFY_UPDATE_DISABLED_RETRY_ATTEMPTS = max(0, int(os.getenv("PRINTIFY_UPDATE_DISABLED_RETRY_ATTEMPTS", "1")))
PRINTIFY_UPDATE_DISABLED_RETRY_SLEEP_SECONDS = max(0.0, float(os.getenv("PRINTIFY_UPDATE_DISABLED_RETRY_SLEEP_SECONDS", "0.75")))
PRINTIFY_DIRECT_UPLOAD_LIMIT_BYTES = 5 * 1024 * 1024
CACHE_TTL_HOURS_DEFAULT = int(os.getenv("CATALOG_CACHE_TTL_HOURS", "24"))
CATALOG_CACHE_DIR_DEFAULT = os.getenv("CATALOG_CACHE_DIR", "./.inkvibe_cache/catalog")

logger = logging.getLogger("inkvibeauto")

DEFAULT_TEMPLATE_PRICE = "29.99"
DEFAULT_MAX_ENABLED_VARIANTS = int(os.getenv("MAX_ENABLED_VARIANTS_SAFETY_LIMIT", "100"))
PRODUCTION_BASELINE_TEMPLATE_KEYS = (
    "tshirt_gildan",
    "sweatshirt_gildan",
    "hoodie_gildan",
    "mug_new",
    "poster_basic",
    "phone_case_basic",
    "sticker_kisscut",
)

TEMPLATE_FIT_POLICY_VALUES = {"placement_defined", "contain_required", "cover_required"}
TEMPLATE_COVER_BEHAVIOR_VALUES = {"unspecified", "contain_preferred", "allow_safe_crop", "require_full_cover"}
TEMPLATE_HIGH_RES_INTENT_VALUES = {"standard", "high_resolution_required"}
TEMPLATE_CROP_TOLERANCE_VALUES = {"unspecified", "none", "safe", "moderate"}
TEMPLATE_CERTIFICATION_STAGE_VALUES = {"none", "candidate", "production_ready"}

AI_PRODUCT_COPY_MODEL_DEFAULT = os.getenv("OPENAI_MODEL", product_copy_generator.DEFAULT_COPY_MODEL)
ENABLE_AI_PRODUCT_COPY_DEFAULT = os.getenv("ENABLE_AI_PRODUCT_COPY", "").strip().lower() in {"1", "true", "yes", "on"}
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()


@dataclass
class AiProductCopySettings:
    enabled: bool = ENABLE_AI_PRODUCT_COPY_DEFAULT
    model: str = AI_PRODUCT_COPY_MODEL_DEFAULT
    api_key: str = OPENAI_API_KEY


AI_PRODUCT_COPY_SETTINGS = AiProductCopySettings()


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


class InsufficientArtworkResolutionError(ValueError):
    def __init__(
        self,
        *,
        template_key: str,
        placement_name: str,
        source_size: Tuple[int, int],
        required_size: Tuple[int, int],
        fit_mode: str,
    ):
        super().__init__(
            f"image too small ({source_size[0]}x{source_size[1]}) for "
            f"placement {placement_name} ({required_size[0]}x{required_size[1]})"
        )
        self.template_key = template_key
        self.placement_name = placement_name
        self.source_size = source_size
        self.required_size = required_size
        self.fit_mode = fit_mode


class RetryLimitExceededError(RuntimeError):
    def __init__(self, *, method: str, path: str, policy_bucket: str, status_code: int, attempts: int, reason_code: str):
        super().__init__(
            f"Retry limit exceeded for {method.upper()} {path} "
            f"(status={status_code}, attempts={attempts}, policy={policy_bucket}, reason_code={reason_code})"
        )
        self.method = method.upper()
        self.path = path
        self.policy_bucket = policy_bucket
        self.status_code = status_code
        self.attempts = attempts
        self.reason_code = reason_code


class CatalogCliUsageError(RuntimeError):
    pass


class TemplateSkipGuardrail(RuntimeError):
    def __init__(self, status: str, reason: str, margin_report: Optional[Dict[str, Any]] = None):
        super().__init__(reason)
        self.status = status
        self.reason = reason
        self.margin_report = margin_report or {}


@dataclass
class CatalogCacheStats:
    hits: int = 0
    misses: int = 0
    writes: int = 0
    requests_avoided: int = 0


class CatalogCache:
    def __init__(self, *, cache_dir: pathlib.Path, ttl_hours: int = CACHE_TTL_HOURS_DEFAULT, enabled: bool = True):
        self.cache_dir = cache_dir
        self.ttl_seconds = max(0, int(ttl_hours) * 3600)
        self.enabled = bool(enabled)
        self.stats = CatalogCacheStats()
        if self.enabled:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _entry_path(self, cache_key: str) -> pathlib.Path:
        digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{digest}.json"

    def get(self, cache_key: str) -> Optional[Any]:
        if not self.enabled:
            return None
        path = self._entry_path(cache_key)
        if not path.exists():
            self.stats.misses += 1
            logger.info("Catalog cache miss key=%s", cache_key)
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            expires_at = float(payload.get("expires_at", 0))
            if self.ttl_seconds > 0 and time.time() > expires_at:
                self.stats.misses += 1
                logger.info("Catalog cache stale key=%s", cache_key)
                return None
            self.stats.hits += 1
            self.stats.requests_avoided += 1
            logger.info("Catalog cache hit key=%s", cache_key)
            return payload.get("value")
        except Exception:
            self.stats.misses += 1
            logger.info("Catalog cache unreadable key=%s", cache_key)
            return None

    def set(self, cache_key: str, value: Any) -> None:
        if not self.enabled:
            return
        payload = {
            "cached_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": time.time() + self.ttl_seconds if self.ttl_seconds > 0 else time.time() + (365 * 24 * 3600),
            "value": value,
        }
        path = self._entry_path(cache_key)
        save_json_atomic(path, payload)
        self.stats.writes += 1
        logger.info("Catalog cache write key=%s", cache_key)


@dataclass
class TemplatePreflightIssue:
    template_key: str
    classification: str
    message: str
    requested_explicitly: bool = False
    blueprint_id: int = 0
    provider_id: int = 0
    selected_count: int = 0
    repriced_count: int = 0
    disabled_count_after_reprice: int = 0
    final_enabled_count: int = 0
    recommended_action: str = "deactivate_pending_validation"


@dataclass
class TemplatePreflightReportRow:
    template_key: str
    requested_explicitly: bool
    preflight_status: str
    classification: str
    message: str
    blueprint_id: int
    provider_id: int
    selected_count: int
    repriced_count: int
    disabled_count_after_reprice: int
    final_enabled_count: int
    recommended_action: str
    tote_original_sale_price_minor: int = 0
    tote_repriced_sale_price_minor: int = 0
    tote_printify_cost_minor: int = 0
    tote_shipping_basis_used: str = ""
    tote_target_margin_after_shipping_minor: int = 0
    tote_min_margin_after_shipping_minor: int = 0
    tote_margin_before_reprice_minor: int = 0
    tote_margin_after_reprice_minor: int = 0
    tote_max_allowed_price_minor: int = 0
    tote_failure_reason: str = ""
    apparel_original_sale_price_minor: int = 0
    apparel_repriced_sale_price_minor: int = 0
    apparel_printify_cost_minor: int = 0
    apparel_shipping_basis_used: str = ""
    apparel_shipping_minor: int = 0
    apparel_target_margin_after_shipping_minor: int = 0
    apparel_min_margin_after_shipping_minor: int = 0
    apparel_margin_before_reprice_minor: int = 0
    apparel_margin_after_reprice_minor: int = 0
    apparel_max_allowed_price_minor: int = 0
    apparel_failed_variant_reasons: str = ""
    apparel_failure_reason_counts: str = ""
    option_names: str = ""
    option_values_summary: str = ""
    requested_option_filters: str = ""
    filter_counts: str = ""
    zero_selection_reason: str = ""
    intended_family: str = ""
    resolved_blueprint_title: str = ""
    resolved_provider_title: str = ""
    family_mismatch_reason: str = ""
    template_hint_blueprint_id: int = 0
    template_hint_provider_id: int = 0
    runtime_mapping_overrode_hint: bool = False
    catalog_discovery_used: bool = False
    pinned_mapping_attempted_first: bool = False
    fallback_discovery_triggered: bool = False
    fallback_discovery_reason: str = ""
    catalog_resolution_mode: str = "normal"
    resolved_model_dimension: str = ""
    requested_model_overlap_count: int = 0
    fallback_model_set_applied: bool = False
    final_selected_models: str = ""
    high_resolution_family: bool = False


@dataclass
class VariantFilterDiagnostics:
    option_names: List[str] = field(default_factory=list)
    option_values_summary: Dict[str, List[str]] = field(default_factory=dict)
    requested_option_filters: Dict[str, List[str]] = field(default_factory=dict)
    filter_counts: List[Dict[str, Any]] = field(default_factory=list)
    zero_selection_reason: str = ""
    resolved_model_dimension: str = ""
    requested_model_overlap_count: int = 0
    fallback_model_set_applied: bool = False
    final_selected_models: List[str] = field(default_factory=list)
    selected_additional_colors: List[str] = field(default_factory=list)
    unavailable_additional_colors: List[str] = field(default_factory=list)


@dataclass
class RuntimeSkipDiagnostics:
    template_key: str
    blueprint_id: int
    provider_id: int
    selected_count: int = 0
    final_enabled_count: int = 0
    available_placements: List[str] = field(default_factory=list)
    required_placement_name: str = ""
    print_area_available: bool = False
    upload_map: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    payload_build_skip_reason: str = ""
    resolved_option_dimensions: Dict[str, str] = field(default_factory=dict)
    resolved_model_list: List[str] = field(default_factory=list)
    final_reason_code: str = ""


@dataclass
class CatalogResolutionDiagnostics:
    template_key: str
    template_hint_blueprint_id: int
    template_hint_provider_id: int
    resolved_blueprint_id: int
    resolved_provider_id: int
    discovery_mode: str = "normal"
    pinned_attempted_first: bool = False
    discovery_used: bool = False
    fallback_discovery_triggered: bool = False
    fallback_discovery_reason: str = ""


@dataclass
class CatalogFamilyValidationResult:
    intended_family: str
    plausible: bool
    reason: str = ""


def classify_failure(exc: Exception) -> str:
    if isinstance(exc, RetryLimitExceededError):
        return exc.reason_code
    if isinstance(exc, InsufficientArtworkResolutionError):
        return "insufficient_artwork_resolution"
    if isinstance(exc, TemplateValidationError):
        return "invalid_template_config"
    if isinstance(exc, TemplateSkipGuardrail):
        return "zero_enabled_after_guardrails"
    if isinstance(exc, NonRetryableRequestError):
        return "invalid_template_config" if "HTTP 404" in str(exc) else "runtime_api_failure"
    return "runtime_api_failure"


def _failure_reason_code(exc: Exception) -> str:
    if isinstance(exc, RetryLimitExceededError):
        return exc.reason_code
    return classify_failure(exc)


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
    metadata_resolution_source: str = "fallback"
    metadata_generated_inline: bool = False
    metadata_sidecar_written: bool = False
    weak_metadata_detected: List[str] = field(default_factory=list)
    final_title_source: str = ""


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
    max_allowed_price: Optional[str] = None
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
    active_placements: List[str] = field(default_factory=list)
    preferred_primary_placement: Optional[str] = None
    publish_only_primary_placement: bool = False
    poster_safe_max_upscale_factor: Optional[float] = None
    poster_safe_min_source_ratio: Optional[float] = None
    poster_fill_target_pct: Optional[float] = None
    poster_trim_fill_optimization: bool = False
    preferred_mockup_colors: List[str] = field(default_factory=list)
    preferred_default_variant_color: Optional[str] = None
    preferred_mockup_types: List[str] = field(default_factory=list)
    preferred_featured_image_strategy: str = "variant_color_then_mockup_type"
    preferred_mockup_position: Optional[str] = None
    secondary_collection_handles: List[str] = field(default_factory=list)
    min_margin_after_shipping: Optional[str] = None
    min_profit_after_shipping: Optional[str] = None
    target_margin_after_shipping: Optional[str] = None
    shipping_basis_for_margin: str = "cost"
    disable_variants_below_margin_floor: bool = False
    reprice_variants_to_margin_floor: bool = True
    mark_template_nonviable_if_needed: bool = False
    active: bool = True
    provider_selection_strategy: str = "pinned_then_printify_choice_then_lowest_cost"
    pinned_provider_id: Optional[int] = None
    pinned_blueprint_id: Optional[int] = None
    provider_preference_order: List[int] = field(default_factory=list)
    fallback_provider_allowed: bool = True
    high_resolution_family: bool = False
    skip_if_artwork_below_threshold: bool = False
    min_source_width: Optional[int] = None
    min_source_height: Optional[int] = None
    min_source_short_edge: Optional[int] = None
    min_source_long_edge: Optional[int] = None
    min_effective_cover_ratio: Optional[float] = None
    expanded_enabled_colors: List[str] = field(default_factory=list)
    storefront_display_color_priority: List[str] = field(default_factory=list)
    storefront_display_color_rotation_seed: Optional[str] = None
    storefront_default_color_candidates: List[str] = field(default_factory=list)
    artwork_routing_family: Optional[str] = None
    artwork_fit_policy: str = "placement_defined"
    cover_behavior: str = "unspecified"
    high_resolution_intent: str = "standard"
    crop_tolerance: str = "unspecified"
    requires_certification: bool = False
    certification_stage: str = "none"


@dataclass
class RunProgressTracker:
    enabled: bool
    total: int
    completed: int = 0
    current_artwork: str = ""
    current_template: str = ""
    current_stage: str = "prepare"
    stream: Any = sys.stderr

    def _render(self) -> None:
        if not self.enabled:
            return
        total = max(1, int(self.total))
        completed = max(0, min(int(self.completed), total))
        pct = int((completed / total) * 100)
        line = (
            f"\r[{completed}/{total} {pct:>3}%] "
            f"artwork={self.current_artwork or '-'} "
            f"template={self.current_template or '-'} "
            f"stage={self.current_stage or '-'}"
        )
        print(line[:220], end="", file=self.stream, flush=True)

    def update(self, *, artwork: str = "", template: str = "", stage: str = "") -> None:
        if artwork:
            self.current_artwork = artwork
        if template:
            self.current_template = template
        if stage:
            self.current_stage = stage
        self._render()

    def complete_one(self) -> None:
        self.completed += 1
        self._render()

    def finish(self) -> None:
        if self.enabled:
            self._render()
            print("", file=self.stream, flush=True)


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
    catalog_cache_hits: int = 0
    catalog_cache_misses: int = 0
    catalog_requests_avoided: int = 0
    chunks_completed: int = 0
    total_chunks: int = 0
    templates_skipped_catalog_rate_limited: int = 0
    resumed_combinations: int = 0
    products_created_but_not_published: int = 0
    publish_queue_total_count: int = 0
    publish_queue_pending_count: int = 0
    publish_queue_completed_count: int = 0
    publish_queue_failed_count: int = 0
    publish_rate_limit_events: int = 0
    rate_limit_events: Dict[str, int] = field(default_factory=dict)


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
    reason_code: str
    error_message: str
    suggested_next_action: str
    launch_plan_row: str = ""
    launch_plan_row_id: str = ""
    source_size: str = ""
    required_placement_size: str = ""
    required_fit_mode: str = ""
    eligibility_high_resolution_family: bool = False
    eligibility_outcome: str = ""
    eligibility_reason_code: str = ""
    eligibility_rule_failed: str = ""
    eligibility_gate_stage: str = ""


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
    publish_outcome: str = ""
    publish_queue_status: str = ""
    publish_queue_status_before: str = ""
    publish_queue_status_after: str = ""
    reason_code: str = ""
    resume_only_queue_processing: bool = False
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
    routed_asset_family: str = ""
    routed_asset_mode: str = ""
    template_family: str = ""
    product_family_label: str = ""
    tote_primary_placement: str = ""
    tote_active_placements: str = ""
    poster_cover_eligible: str = ""
    poster_enhancement_status: str = ""
    poster_enhancement_tier: str = ""
    poster_source_ratio: str = ""
    poster_requested_upscale_factor: str = ""
    poster_applied_upscale_factor: str = ""
    poster_fill_optimization_used: bool = False
    family_collection_handle: str = ""
    collection_image_source: str = ""
    collection_sort_strategy: str = ""
    preferred_featured_variant_color: str = ""
    selected_featured_mockup_color: str = ""
    metadata_resolution_source: str = ""
    metadata_generated_inline: bool = False
    metadata_sidecar_written: bool = False
    weak_metadata_detected: str = ""
    final_title_source: str = ""
    featured_image_strategy: str = ""
    featured_image_source: str = ""
    tote_scale_strategy: str = ""
    required_placement_size: str = ""
    required_fit_mode: str = ""
    eligibility_high_resolution_family: bool = False
    eligibility_outcome: str = ""
    eligibility_reason_code: str = ""
    eligibility_rule_failed: str = ""
    eligibility_gate_stage: str = ""


@dataclass
class PromptArtworkGenerationResult:
    generated_paths: List[pathlib.Path] = field(default_factory=list)
    template_routing: List[TemplateAssetRouting] = field(default_factory=list)


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
    family: str = ""
    product_type: str = ""
    department_key: str = ""
    department_label: str = ""
    primary_collection_handle: str = ""
    primary_collection_title: str = ""
    recommended_manual_collections: str = ""
    recommended_smart_collection_tags: str = ""
    normalized_theme_keys: str = ""
    normalized_audience_keys: str = ""
    normalized_season_keys: str = ""
    metadata_resolution_source: str = ""
    metadata_generated_inline: bool = False
    metadata_sidecar_written: bool = False
    copy_provenance: str = ""
    ai_product_copy_cache_reason: str = ""


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
    collection_image_src: str = ""
    collection_sort_order: str = ""
    secondary_collection_handles: str = ""


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
    poster_cover_eligible: Optional[bool] = None
    poster_enhancement_status: str = ""
    poster_enhancement_tier: str = ""
    poster_source_ratio: float = 0.0
    poster_requested_upscale_factor: float = 1.0
    poster_applied_upscale_factor: float = 1.0
    poster_fill_optimization_used: bool = False
    wallart_derivation_reason: str = ""


@dataclass
class ArtworkProcessingOptions:
    allow_upscale: bool = False
    upscale_method: str = "lanczos"
    skip_undersized: bool = False
    placement_preview: bool = False
    preview_dir: pathlib.Path = pathlib.Path("exports/previews")
    auto_wallart_master: bool = False


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
    poster_cover_eligible: Optional[bool] = None
    poster_enhancement_status: str = ""
    poster_enhancement_tier: str = ""
    poster_source_ratio: float = 0.0
    poster_requested_upscale_factor: float = 1.0
    poster_applied_upscale_factor: float = 1.0
    poster_fill_optimization_used: bool = False
    wallart_derivation_reason: str = ""


@dataclass
class ArtworkEligibilityResult:
    eligible: bool
    reason_code: str = ""
    rule_failed: str = ""
    source_size: Tuple[int, int] = (0, 0)
    required_size: Tuple[int, int] = (0, 0)
    fit_mode: str = ""
    high_resolution_family: bool = False


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
POSTER_FILL_TARGET_PCT_DEFAULT = 0.94
POSTER_SMALL_SOURCE_SAFE_MAX_UPSCALE_FACTOR = 1.85
POSTER_SMALL_SOURCE_MIN_SOURCE_RATIO = 0.22
POSTER_SMALL_SOURCE_PORTRAIT_ASPECT_MAX = 0.8
WALLART_AUTO_MASTER_TEMPLATE_KEYS = {"canvas_basic", "blanket_basic", "framed_poster_basic"}
WALLART_AUTO_MASTER_MAX_UPSCALE_FACTOR = 6.0
WALLART_AUTO_MASTER_MIN_SOURCE_RATIO = 0.16


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
    "collection_image_src",
    "collection_sort_order",
    "secondary_collection_handles",
]

FAMILY_COLLECTION_RULES: Dict[str, Dict[str, str]] = {
    "tshirt": {"handle": "t-shirts", "title": "T-Shirts", "department_key": "apparel", "department_label": "Apparel"},
    "long_sleeve": {"handle": "long-sleeve-shirts", "title": "Long Sleeve Shirts", "department_key": "apparel", "department_label": "Apparel"},
    "hoodie": {"handle": "hoodies", "title": "Hoodies", "department_key": "apparel", "department_label": "Apparel"},
    "sweatshirt": {"handle": "sweatshirts", "title": "Sweatshirts", "department_key": "apparel", "department_label": "Apparel"},
    "mug": {"handle": "mugs", "title": "Mugs", "department_key": "drinkware", "department_label": "Drinkware"},
    "poster": {"handle": "posters", "title": "Posters", "department_key": "wall-art", "department_label": "Wall Art"},
    "framed_poster": {"handle": "framed-posters", "title": "Framed Posters", "department_key": "wall-art", "department_label": "Wall Art"},
    "tote": {"handle": "tote-bags", "title": "Tote Bags", "department_key": "accessories", "department_label": "Accessories"},
    "tumbler": {"handle": "tumblers", "title": "Tumblers", "department_key": "drinkware", "department_label": "Drinkware"},
    "travel_mug": {"handle": "travel-mugs", "title": "Travel Mugs", "department_key": "drinkware", "department_label": "Drinkware"},
    "phone_case": {"handle": "phone-cases", "title": "Phone Cases", "department_key": "accessories", "department_label": "Accessories"},
    "sticker": {"handle": "stickers", "title": "Stickers", "department_key": "accessories", "department_label": "Accessories"},
    "canvas": {"handle": "canvas-prints", "title": "Canvas Prints", "department_key": "home-decor", "department_label": "Home Decor"},
    "blanket": {"handle": "blankets", "title": "Blankets", "department_key": "home-decor", "department_label": "Home Decor"},
    "throw_pillow": {"handle": "throw-pillows", "title": "Throw Pillows", "department_key": "home-decor", "department_label": "Home Decor"},
    "embroidered_hat": {"handle": "embroidered-hats", "title": "Embroidered Hats", "department_key": "accessories", "department_label": "Accessories"},
}

THEME_NORMALIZATION_PATTERNS: Dict[str, Tuple[str, ...]] = {
    "nature-wildlife": ("nature", "wildlife", "forest", "animal", "botanical", "floral", "woodland"),
    "ocean-coastal": ("ocean", "coastal", "beach", "sea", "shore", "surf", "nautical"),
    "food-fun": ("food", "snack", "dessert", "coffee", "pizza", "fruit", "fun"),
    "minimal-bold": ("minimal", "bold", "clean", "graphic", "modern"),
    "outdoor-vibes": ("outdoor", "mountain", "hiking", "camp", "adventure", "trail"),
    "giftable-art": ("gift", "giftable", "present"),
}

AUDIENCE_NORMALIZATION_PATTERNS: Dict[str, Tuple[str, ...]] = {
    "unisex": ("unisex",),
    "men": ("men", "mens", "male"),
    "women": ("women", "womens", "female"),
    "youth": ("youth", "kids", "children", "teen"),
    "giftable": ("gift", "giftable"),
    "home-decor-shoppers": ("decor", "home decor", "interior"),
    "coffee-lovers": ("coffee", "tea", "mug"),
    "phone-accessory-shoppers": ("phone", "phone case", "phone cases", "mobile accessory"),
    "sticker-lovers": ("sticker", "decal"),
}

SEASON_NORMALIZATION_PATTERNS: Dict[str, Tuple[str, ...]] = {
    "spring": ("spring", "bloom"),
    "summer": ("summer", "beach"),
    "fall": ("fall", "autumn"),
    "winter": ("winter", "snow", "holiday"),
    "holiday": ("holiday", "christmas", "xmas"),
}

MANUAL_MERCH_COLLECTIONS: Dict[str, Dict[str, str]] = {
    "featured": {"title": "Featured", "handle": "featured"},
    "new-drops": {"title": "New Drops", "handle": "new-drops"},
    "best-sellers": {"title": "Best Sellers", "handle": "best-sellers"},
}

SHOPIFY_PRODUCT_TYPE_BY_FAMILY: Dict[str, str] = {
    "tshirt": "T-Shirts",
    "long_sleeve": "Long Sleeve Shirts",
    "hoodie": "Hoodies",
    "sweatshirt": "Sweatshirts",
    "mug": "Mugs",
    "tumbler": "Tumblers",
    "travel_mug": "Travel Mugs",
    "poster": "Posters",
    "framed_poster": "Framed Posters",
    "canvas": "Canvas Prints",
    "blanket": "Blankets",
    "tote": "Tote Bags",
    "phone_case": "Phone Cases",
    "sticker": "Stickers",
    "throw_pillow": "Throw Pillows",
    "embroidered_hat": "Embroidered Hats",
}

SHOPIFY_CATEGORY_LABEL_BY_FAMILY: Dict[str, str] = {
    "tshirt": "Apparel & Accessories > Clothing > Shirts & Tops",
    "long_sleeve": "Apparel & Accessories > Clothing > Shirts & Tops",
    "hoodie": "Apparel & Accessories > Clothing > Activewear > Hoodies & Sweatshirts",
    "sweatshirt": "Apparel & Accessories > Clothing > Activewear > Hoodies & Sweatshirts",
    "mug": "Home & Garden > Kitchen & Dining > Tableware > Drinkware > Mugs",
    "tumbler": "Home & Garden > Kitchen & Dining > Tableware > Drinkware",
    "travel_mug": "Home & Garden > Kitchen & Dining > Tableware > Drinkware > Travel Mugs",
    "poster": "Home & Garden > Decor > Artwork > Posters, Prints, & Visual Artwork",
    "framed_poster": "Home & Garden > Decor > Artwork > Posters, Prints, & Visual Artwork",
    "canvas": "Home & Garden > Decor > Artwork > Posters, Prints, & Visual Artwork",
    "blanket": "Home & Garden > Linens & Bedding > Blankets",
    "tote": "Apparel & Accessories > Handbags, Wallets & Cases > Handbags",
    "phone_case": "Electronics > Electronics Accessories > Mobile Phone Accessories > Mobile Phone Cases",
    "sticker": "Arts & Entertainment > Hobbies & Creative Arts > Collectibles",
}


def resolve_shopify_product_type(template: ProductTemplate) -> str:
    family = content_engine.infer_product_family(template)
    mapped = str(SHOPIFY_PRODUCT_TYPE_BY_FAMILY.get(family) or "").strip()
    if mapped:
        return mapped
    return (template.product_type_label or template.shopify_product_type or content_engine.family_title_suffix(template) or "Product").strip()


def resolve_shopify_category_label(template: ProductTemplate) -> str:
    family = content_engine.infer_product_family(template)
    return str(SHOPIFY_CATEGORY_LABEL_BY_FAMILY.get(family) or "").strip()


# -----------------------------
# Logging / helpers
# -----------------------------


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def configure_ai_product_copy(*, enabled: Optional[bool] = None, model: str = "", api_key: str = "") -> None:
    resolved_enabled = ENABLE_AI_PRODUCT_COPY_DEFAULT if enabled is None else bool(enabled)
    resolved_model = (model or os.getenv("OPENAI_MODEL") or AI_PRODUCT_COPY_MODEL_DEFAULT).strip()
    resolved_api_key = (api_key or os.getenv("OPENAI_API_KEY") or "").strip()
    AI_PRODUCT_COPY_SETTINGS.enabled = resolved_enabled
    AI_PRODUCT_COPY_SETTINGS.model = resolved_model
    AI_PRODUCT_COPY_SETTINGS.api_key = resolved_api_key
    logger.info(
        "AI product copy settings enabled=%s model=%s key_present=%s",
        AI_PRODUCT_COPY_SETTINGS.enabled,
        AI_PRODUCT_COPY_SETTINGS.model,
        bool(AI_PRODUCT_COPY_SETTINGS.api_key),
    )


def slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def _normalize_title_tokens(value: str) -> List[str]:
    return content_engine._normalize_title_tokens(value)


def filename_slug_to_title(value: str) -> str:
    return content_engine.filename_slug_to_title(value)


def _normalize_color_name(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).casefold()


def _is_white_like_color(value: str) -> bool:
    normalized = _normalize_color_name(value)
    return normalized in {"white", "white glossy", "snowwhite"}


def _choose_rotated_storefront_display_color(
    *,
    template: ProductTemplate,
    available_colors: List[str],
    deterministic_key: str,
) -> str:
    if not available_colors:
        return ""
    if not template.storefront_default_color_candidates:
        return ""
    key_basis = f"{template.storefront_display_color_rotation_seed or template.key}:{deterministic_key}"
    rotation_index = int(hashlib.sha256(key_basis.encode("utf-8")).hexdigest()[:8], 16)
    normalized_available = {_normalize_color_name(color): color for color in available_colors}
    non_white_candidates: List[str] = []
    white_candidates: List[str] = []
    for candidate in template.storefront_default_color_candidates:
        normalized = _normalize_color_name(candidate)
        if normalized not in normalized_available:
            continue
        resolved = normalized_available[normalized]
        if _is_white_like_color(resolved):
            white_candidates.append(resolved)
        else:
            non_white_candidates.append(resolved)
    if non_white_candidates:
        return non_white_candidates[rotation_index % len(non_white_candidates)]
    if white_candidates:
        return white_candidates[rotation_index % len(white_candidates)]
    return ""


def reorder_variants_for_storefront_display(
    *,
    template: ProductTemplate,
    artwork: Artwork,
    variant_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not variant_rows or not template.storefront_display_color_priority:
        return variant_rows
    available_colors: List[str] = []
    seen: set[str] = set()
    for variant in variant_rows:
        color = _variant_option_value(variant, "color")
        if not color:
            continue
        normalized = _normalize_color_name(color)
        if normalized in seen:
            continue
        seen.add(normalized)
        available_colors.append(color)
    deterministic_key = f"{artwork.slug}:{template.key}:{metadata_fingerprint(artwork.metadata)}"
    preferred_color = _choose_rotated_storefront_display_color(
        template=template,
        available_colors=available_colors,
        deterministic_key=deterministic_key,
    )
    priority_order = [preferred_color] if preferred_color else []
    for candidate in template.storefront_display_color_priority:
        if not candidate:
            continue
        if _normalize_color_name(candidate) == _normalize_color_name(preferred_color):
            continue
        priority_order.append(candidate)
    priority_map = {_normalize_color_name(color): index for index, color in enumerate(priority_order)}
    max_rank = len(priority_map) + 100

    def _sort_key(variant: Dict[str, Any]) -> Tuple[int, int]:
        color = _variant_option_value(variant, "color")
        color_rank = priority_map.get(_normalize_color_name(color), max_rank)
        variant_id = int(variant.get("id") or 0)
        return (color_rank, variant_id)

    reordered = sorted(variant_rows, key=_sort_key)
    if preferred_color:
        template.preferred_default_variant_color = preferred_color
    return reordered


def resolve_family_collection_target(template: ProductTemplate) -> Dict[str, str]:
    family = content_engine.infer_product_family(template)
    return dict(FAMILY_COLLECTION_RULES.get(family, {}))


def _normalize_taxonomy_keys(values: List[Any], allowlist: Dict[str, Tuple[str, ...]]) -> List[str]:
    haystack = " ".join(normalize_theme_tag(value) for value in values if normalize_theme_tag(value))
    if not haystack:
        return []
    padded_haystack = f" {haystack} "
    normalized: List[str] = []
    for key, patterns in allowlist.items():
        if any(re.search(rf"\b{re.escape(str(pattern).strip())}\b", padded_haystack) for pattern in patterns if str(pattern).strip()):
            normalized.append(key)
    return normalized


def build_normalized_shopify_organization(template: ProductTemplate, artwork: Artwork) -> Dict[str, Any]:
    family = content_engine.infer_product_family(template)
    family_cfg = dict(FAMILY_COLLECTION_RULES.get(family, {}))
    metadata = artwork.metadata or {}
    family_label = content_engine.family_title_suffix(template)
    department_key = str(family_cfg.get("department_key") or "").strip()
    department_label = str(family_cfg.get("department_label") or "").strip()
    theme_inputs: List[Any] = [
        metadata.get("theme"),
        metadata.get("collection"),
        metadata.get("occasion"),
        *_split_keywords(metadata.get("tags")),
        *_split_keywords(metadata.get("style_keywords")),
        *_split_keywords(metadata.get("seo_keywords")),
    ]
    audience_inputs: List[Any] = [metadata.get("audience"), *_split_keywords(metadata.get("tags")), *_split_keywords(metadata.get("seo_keywords"))]
    season_inputs: List[Any] = [
        metadata.get("season"),
        metadata.get("occasion"),
        metadata.get("theme"),
        metadata.get("collection"),
        *_split_keywords(metadata.get("tags")),
    ]
    normalized_theme_keys = _normalize_taxonomy_keys(theme_inputs, THEME_NORMALIZATION_PATTERNS)
    normalized_audience_keys = _normalize_taxonomy_keys(audience_inputs, AUDIENCE_NORMALIZATION_PATTERNS)
    normalized_season_keys = _normalize_taxonomy_keys(season_inputs, SEASON_NORMALIZATION_PATTERNS) or ["evergreen"]
    merchandising_collection_handles: List[str] = [
        MANUAL_MERCH_COLLECTIONS["featured"]["handle"],
        MANUAL_MERCH_COLLECTIONS["new-drops"]["handle"],
        MANUAL_MERCH_COLLECTIONS["best-sellers"]["handle"],
    ]
    smart_collection_tags = [f"family-{family.replace('_', '-')}"]
    if department_key:
        smart_collection_tags.append(f"dept-{department_key}")
    smart_collection_tags.extend(f"theme-{key}" for key in normalized_theme_keys)
    smart_collection_tags.extend(f"audience-{key}" for key in normalized_audience_keys)
    smart_collection_tags.extend(f"season-{key}" for key in normalized_season_keys)
    deduped_smart_tags = list(dict.fromkeys(smart_collection_tags))
    recommended_product_type = resolve_shopify_product_type(template)
    recommended_shopify_category_label = resolve_shopify_category_label(template)
    return {
        "family_key": family,
        "family_label": family_label,
        "primary_collection_handle": str(family_cfg.get("handle") or ""),
        "primary_collection_title": str(family_cfg.get("title") or ""),
        "department_key": department_key,
        "department_label": department_label,
        "shop_menu_group": department_label or "Catalog",
        "normalized_theme_keys": normalized_theme_keys,
        "normalized_audience_keys": normalized_audience_keys,
        "normalized_season_keys": normalized_season_keys,
        "merchandising_collection_handles": merchandising_collection_handles,
        "smart_collection_tags": deduped_smart_tags,
        "recommended_manual_collections": merchandising_collection_handles,
        "recommended_smart_collection_tags": deduped_smart_tags,
        "recommended_product_type": recommended_product_type,
        "recommended_shopify_category_label": recommended_shopify_category_label,
    }


def choose_preferred_featured_variant_color(*, template: ProductTemplate, variant_rows: List[Dict[str, Any]]) -> str:
    available_colors: List[str] = []
    seen: set[str] = set()
    for variant in variant_rows:
        color = _variant_option_value(variant, "color")
        if not color:
            continue
        key = _normalize_color_name(color)
        if key in seen:
            continue
        seen.add(key)
        available_colors.append(color)
    if not available_colors:
        return ""

    ranking = [template.preferred_default_variant_color] if template.preferred_default_variant_color else []
    ranking.extend(template.preferred_mockup_colors or [])
    for preferred in ranking:
        if not preferred:
            continue
        needle = _normalize_color_name(preferred)
        for available in available_colors:
            if _normalize_color_name(available) == needle:
                return available
    for available in available_colors:
        if not _is_white_like_color(available):
            return available
    return available_colors[0]


def choose_preferred_featured_mockup_candidate(
    *,
    template: ProductTemplate,
    variant_rows: List[Dict[str, Any]],
    product_images: List[Dict[str, Any]],
) -> Dict[str, str]:
    preferred_color = choose_preferred_featured_variant_color(template=template, variant_rows=variant_rows)
    preferred_types = [_normalize_color_name(v) for v in (template.preferred_mockup_types or []) if str(v).strip()]
    preferred_position = _normalize_color_name(template.preferred_mockup_position or "")
    family = content_engine.infer_product_family(template)

    def _image_signal_tokens(image: Dict[str, Any]) -> set[str]:
        buckets: List[str] = []
        for key in ("type", "image_type", "mockup_type", "position_name", "position", "view", "label", "name", "title"):
            value = image.get(key)
            if isinstance(value, str) and value.strip():
                buckets.append(value.strip().lower())
        for key in ("tags", "mockup_tags"):
            values = image.get(key)
            if isinstance(values, list):
                buckets.extend(str(v).strip().lower() for v in values if str(v).strip())
            elif isinstance(values, str) and values.strip():
                buckets.append(values.strip().lower())
        combined = " ".join(buckets)
        return {token for token in re.findall(r"[a-z0-9]+", combined) if token}

    def _hoodie_sweatshirt_framing_rank(image: Dict[str, Any], image_type: str, image_position: str) -> Tuple[int, int, int, int]:
        """
        Lower is better.
        Rank tuple: (top_heavy_penalty, composition_penalty, crop_penalty, neutral_color_penalty)
        """
        if family not in {"hoodie", "sweatshirt"}:
            return (0, 0, 0, 1)
        tokens = _image_signal_tokens(image)
        type_tokens = {token for token in re.findall(r"[a-z0-9]+", str(image_type).lower()) if token}
        position_tokens = {token for token in re.findall(r"[a-z0-9]+", str(image_position).lower()) if token}
        all_tokens = tokens.union(type_tokens).union(position_tokens)
        src = str(image.get("src") or image.get("preview_url") or image.get("url") or "").lower()
        all_tokens.update(token for token in re.findall(r"[a-z0-9]+", src) if token)

        top_heavy = {"top", "upper", "closeup", "close", "crop", "cropped", "detail", "zoom", "headshot", "neck", "torso", "half"}
        balanced = {"front", "full", "whole", "center", "centered", "straight", "studio", "flat", "catalog"}
        neutral_dark = {"black", "navy", "charcoal", "dark", "graphite", "midnight"}

        top_heavy_penalty = 1 if all_tokens.intersection(top_heavy) else 0
        has_composition_signal = bool(all_tokens.intersection(top_heavy.union(balanced)))
        composition_penalty = 0 if not has_composition_signal or all_tokens.intersection(balanced) else 1
        crop_penalty = 0
        if {"torso", "crop"}.intersection(all_tokens):
            crop_penalty = 2
        elif {"closeup", "close", "detail"}.intersection(all_tokens):
            crop_penalty = 1
        neutral_color_penalty = 0 if all_tokens.intersection(neutral_dark) else 1
        return (top_heavy_penalty, composition_penalty, crop_penalty, neutral_color_penalty)

    variant_id_to_color: Dict[str, str] = {}
    for variant in variant_rows:
        vid = str(variant.get("id") or "").strip()
        color = _variant_option_value(variant, "color")
        if vid and color:
            variant_id_to_color[vid] = color

    scored_rows: List[Tuple[Tuple[int, int, int, int, int], Dict[str, str]]] = []
    for idx, image in enumerate(product_images):
        if not isinstance(image, dict):
            continue
        image_type = str(image.get("type") or image.get("image_type") or image.get("mockup_type") or "").strip()
        image_position = str(image.get("position_name") or image.get("position") or image.get("view") or "").strip()
        src = str(image.get("src") or image.get("preview_url") or image.get("url") or "").strip()
        image_variant_ids = image.get("variant_ids") if isinstance(image.get("variant_ids"), list) else []
        color = ""
        for vid in image_variant_ids:
            resolved = variant_id_to_color.get(str(vid))
            if resolved:
                color = resolved
                break
        type_rank = preferred_types.index(_normalize_color_name(image_type)) if _normalize_color_name(image_type) in preferred_types else len(preferred_types)
        color_rank = 1
        if preferred_color and color and _normalize_color_name(color) == _normalize_color_name(preferred_color):
            color_rank = 0
        if not preferred_color:
            color_rank = 0
        position_rank = 1
        if preferred_position and image_position and _normalize_color_name(image_position) == preferred_position:
            position_rank = 0
        default_rank = 0 if bool(image.get("is_default")) else 1
        framing_rank = _hoodie_sweatshirt_framing_rank(image, image_type, image_position)
        score = (framing_rank[0], framing_rank[1], framing_rank[2], color_rank, type_rank, position_rank, default_rank, framing_rank[3], idx)
        scored_rows.append(
            (
                score,
                {
                    "selected_featured_mockup_color": color,
                    "selected_featured_mockup_type": image_type,
                    "selected_featured_mockup_position": image_position,
                    "selected_featured_mockup_src": src,
                },
            )
        )

    if not scored_rows:
        return {
            "selected_featured_mockup_color": "",
            "selected_featured_mockup_type": "",
            "selected_featured_mockup_position": "",
            "selected_featured_mockup_src": "",
        }
    scored_rows.sort(key=lambda row: row[0])
    return scored_rows[0][1]


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
    title = re.sub(r"\s+[|–—:]+\s*", " - ", title)
    title = re.sub(r"\s+[-]+\s+", " - ", title)
    patterns = [
        (r"\b(t-?shirt)\s+\1\b", r"\1"),
        (r"\b(tee)\s+\1\b", r"\1"),
        (r"\b(long sleeve t-?shirt)\s+\1\b", r"\1"),
        (r"\b(mug)\s+\1\b", r"\1"),
        (r"\b(poster)\s+\1\b", r"\1"),
        (r"\b(tote bag)\s+\1\b", r"\1"),
    ]
    for pattern, repl in patterns:
        title = re.sub(pattern, repl, title, flags=re.IGNORECASE)
    title = re.sub(r"(?:\s*-\s*){2,}", " - ", title)
    return re.sub(r"\s+", " ", title).strip()


def _title_product_signal_label(template: ProductTemplate) -> str:
    explicit_label = (template.product_type_label or "").strip()
    if explicit_label:
        return explicit_label
    shopify_type = (template.shopify_product_type or "").strip()
    if shopify_type and shopify_type.lower() not in {"apparel", "product", "accessories", "merchandise"}:
        return shopify_type
    family = content_engine.infer_product_family(template)
    if family == "default":
        return ""
    return content_engine.family_title_suffix(template).strip()


def _refine_rendered_title(*, artwork_title: str, rendered_title: str, product_label: str) -> str:
    candidate = _dedupe_rendered_title(rendered_title)
    candidate = re.sub(r"\b(signature product|untitled design|untitled)\b", "", candidate, flags=re.IGNORECASE)
    candidate = _dedupe_rendered_title(candidate).strip(" -")
    filler_words = {"happy", "animated", "abstract"}
    parts = candidate.split()
    while len(parts) > 2 and parts and parts[0].lower() in filler_words:
        parts = parts[1:]
    candidate = " ".join(parts).strip()
    if not candidate:
        candidate = artwork_title.strip()
    if product_label and not title_semantically_includes_product_label(candidate, product_label):
        if candidate.lower() == artwork_title.strip().lower():
            candidate = f"{candidate} {product_label}".strip()
        else:
            candidate = f"{candidate} - {product_label}".strip()
    return _dedupe_rendered_title(candidate)

def _split_keywords(value: Any) -> List[str]:
    if isinstance(value, list):
        rows = value
    elif isinstance(value, str):
        rows = re.split(r"[,;]", value)
    else:
        return []
    return [str(row).strip() for row in rows if str(row).strip()]


TAG_GENERIC_TOKENS = {"print-on-demand", "printify", "style", "design", "artwork", "product", "gift idea"}
THEME_GENERIC_TERMS = {
    "art",
    "artwork",
    "design",
    "style",
    "aesthetic",
    "vibe",
    "theme",
    "gift idea",
    "decor",
    "collection",
}
THEME_SIGNAL_KEYWORDS = {
    "abstract",
    "animal",
    "beach",
    "boho",
    "botanical",
    "coastal",
    "culture",
    "desert",
    "floral",
    "forest",
    "landscape",
    "mountain",
    "nature",
    "neon",
    "patriotic",
    "portrait",
    "retro",
    "rustic",
    "snow",
    "street",
    "summer",
    "surf",
    "tropical",
    "vintage",
    "wildlife",
    "woodland",
}


def normalize_theme_tag(value: Any) -> str:
    cleaned = str(value or "").strip().lower()
    if not cleaned:
        return ""
    cleaned = re.sub(r"[|_]", " ", cleaned)
    cleaned = re.sub(r"[^a-z0-9&' +/\-]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -")
    if not cleaned:
        return ""
    tokens = [token for token in re.findall(r"[a-z0-9&']+", cleaned) if token]
    if not tokens:
        return ""
    if len(tokens) == 1 and tokens[0] in THEME_GENERIC_TERMS:
        return ""
    if len(tokens) > 5:
        tokens = tokens[:5]
    candidate = " ".join(tokens).strip()
    if candidate in THEME_GENERIC_TERMS or len(candidate) > 32:
        return ""
    return candidate


def extract_theme_signal_candidates(*, artwork: Artwork, context: Dict[str, str], template: ProductTemplate) -> List[str]:
    metadata = artwork.metadata or {}
    raw_candidates: List[Any] = [
        metadata.get("theme"),
        metadata.get("subtitle"),
        metadata.get("occasion"),
        metadata.get("collection"),
        metadata.get("color_story"),
        *_split_keywords(metadata.get("style_keywords")),
        *_split_keywords(metadata.get("seo_keywords")),
        *template.style_keywords,
    ]
    subject_tokens = [token for token in re.findall(r"[a-z0-9]+", str(metadata.get("title") or context.get("artwork_title") or artwork.slug).lower()) if len(token) >= 4]
    env_source = " ".join(str(row or "") for row in raw_candidates)
    env_tokens = [token for token in re.findall(r"[a-z0-9]+", env_source.lower()) if len(token) >= 4]
    if subject_tokens and env_tokens:
        subject = subject_tokens[0]
        if any(token in {"mountain", "snow", "forest", "beach", "desert"} for token in env_tokens):
            raw_candidates.append(f"{env_tokens[0]} {subject}")
        elif any(token in {"portrait", "wildlife", "nature"} for token in env_tokens):
            raw_candidates.append(f"{env_tokens[0]} art")
    normalized: List[str] = []
    seen: set[str] = set()
    for raw in raw_candidates:
        candidate = normalize_theme_tag(raw)
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        normalized.append(candidate)
    return normalized


def choose_best_theme_signal(*, candidates: List[str], existing_tags: List[str]) -> str:
    if not candidates:
        return ""
    existing = {normalize_theme_tag(tag) for tag in existing_tags if normalize_theme_tag(tag)}

    def _score(candidate: str) -> Tuple[int, int, int]:
        tokens = re.findall(r"[a-z0-9]+", candidate)
        keyword_hits = sum(1 for token in tokens if token in THEME_SIGNAL_KEYWORDS)
        specificity = min(len(tokens), 4)
        brevity_bonus = 2 if len(tokens) <= 3 else 0
        return (keyword_hits, specificity + brevity_bonus, -len(candidate))

    ranked = sorted(candidates, key=_score, reverse=True)
    for candidate in ranked:
        if candidate not in existing:
            return candidate
    return ""


def _tags_contain_theme_signal(*, tags: Iterable[str], artwork: Artwork, template: ProductTemplate) -> bool:
    context = build_seo_context(template, artwork)
    candidates = set(extract_theme_signal_candidates(artwork=artwork, context=context, template=template))
    normalized_tags = {normalize_theme_tag(tag) for tag in tags if normalize_theme_tag(tag)}
    if candidates.intersection(normalized_tags):
        return True
    artwork_tokens = {token for token in re.findall(r"[a-z0-9]+", artwork.slug.lower()) if len(token) >= 4}
    for tag in normalized_tags:
        tag_tokens = {token for token in re.findall(r"[a-z0-9]+", tag) if len(token) >= 4}
        if tag_tokens.intersection(THEME_SIGNAL_KEYWORDS):
            return True
        if len(tag_tokens) >= 2 and artwork_tokens.intersection(tag_tokens):
            return True
    return False


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
        "metadata_provenance": "",
        "inline_upgrade_fingerprint": "",
        "inline_upgrade_timestamp": "",
        "inline_upgrade_reasons": [],
        "ai_product_copy": {},
    }
    for field_name in fields:
        value = payload.get(field_name)
        if field_name in {"tags", "seo_keywords", "style_keywords", "inline_upgrade_reasons"}:
            fields[field_name] = _split_keywords(value)
        elif field_name == "ai_product_copy":
            fields[field_name] = value if isinstance(value, dict) else {}
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


def _is_prompt_generated_artwork_slug(value: str) -> bool:
    normalized = slugify(value)
    if not normalized:
        return False
    return (
        normalized.startswith("generated-art")
        or normalized.startswith("prompt-apparel-")
        or normalized.startswith("prompt-poster-")
        or normalized.startswith("prompt-blanket-")
        or normalized.startswith("prompt-square-")
    )


def _should_bypass_sidecar_for_prompt_generated_artwork(*, path: pathlib.Path, sidecar_metadata: Dict[str, Any]) -> bool:
    if not _is_prompt_generated_artwork_slug(path.stem):
        return False
    provenance = str(sidecar_metadata.get("metadata_provenance") or "").strip().lower()
    # Generated prompt-art stems are reused across runs; without explicit fresh provenance,
    # sidecars are treated as untrusted to avoid stale cross-artwork title leakage.
    if provenance.startswith("prompt_art_run:"):
        return False
    return True


def _normalize_metadata_provenance(value: Any) -> str:
    provenance = str(value or "").strip().lower()
    if provenance.startswith("prompt_art_run:"):
        return provenance.split(":", 1)[1].strip().lower()
    return provenance


def _resolve_unique_alias_match(
    metadata_map: Dict[str, Dict[str, Any]],
    *,
    normalized_aliases: set[str],
) -> Tuple[Optional[Dict[str, Any]], Optional[str], List[str]]:
    matched_keys: List[str] = []
    matched_entry: Optional[Dict[str, Any]] = None
    for key, entry in metadata_map.items():
        if not isinstance(entry, dict):
            continue
        entry_aliases = _metadata_alias_candidates(entry)
        if not entry_aliases.intersection(normalized_aliases):
            continue
        matched_keys.append(key)
        if matched_entry is None:
            matched_entry = dict(entry)
    if len(matched_keys) == 1 and matched_entry is not None:
        return matched_entry, matched_keys[0], []
    if len(matched_keys) > 1:
        return None, None, sorted(matched_keys)
    return None, None, []


def resolve_artwork_metadata_with_source(
    path: pathlib.Path,
    metadata_map: Dict[str, Dict[str, Any]],
    *,
    artwork_slug: str = "",
    persisted_aliases: Optional[List[str]] = None,
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    sidecar_path = path.with_suffix(".json")
    if sidecar_path.exists() and sidecar_path.is_file():
        sidecar_metadata = load_artwork_metadata(sidecar_path)
        if _should_bypass_sidecar_for_prompt_generated_artwork(path=path, sidecar_metadata=sidecar_metadata):
            fallback_key = slugify(path.stem) or slugify(path.name) or "unknown"
            logger.warning(
                "Content metadata sidecar bypass artwork=%s reason=prompt_generated_artwork_reused_slug sidecar=%s",
                path.name,
                sidecar_path.name,
            )
            return {}, {
                "source": "sidecar_bypass_prompt_generated",
                "key": fallback_key,
                "reason": "prompt_generated_reused_slug_sidecar_bypassed",
                "weak_fallback_reason": "prompt_generated_reused_slug_sidecar_bypassed",
            }
        normalized_stem = slugify(path.stem)
        stronger_wins = bool(normalized_stem and metadata_map.get(normalized_stem))
        if not stronger_wins and metadata_map:
            alias_values = [path.stem]
            canonical_slug = slugify(artwork_slug) if artwork_slug else ""
            if canonical_slug:
                alias_values.append(canonical_slug)
            if persisted_aliases:
                alias_values.extend([slugify(alias) for alias in persisted_aliases if slugify(alias)])
            normalized_aliases = {slugify(value) for value in alias_values if slugify(value)}
            if normalized_aliases:
                matched_entry, matched_key, ambiguous_keys = _resolve_unique_alias_match(
                    metadata_map,
                    normalized_aliases=normalized_aliases,
                )
                stronger_wins = bool(matched_entry is not None and matched_key is not None and not ambiguous_keys)
        if stronger_wins:
            logger.info(
                "Content metadata resolution artwork=%s source=sidecar stronger_source_won_over=metadata_map weaker_key=%s",
                path.name,
                normalized_stem,
            )
        return sidecar_metadata, {
            "source": "sidecar",
            "key": sidecar_path.name,
            "reason": "sidecar_file_present",
            "stronger_source_won_over": "metadata_map" if stronger_wins else "",
        }

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
            return metadata, {"source": "slug", "key": key, "reason": "slug_lookup_match"}

    filename_stem = path.stem
    exact_stem_match = metadata_map.get(filename_stem)
    if isinstance(exact_stem_match, dict):
        return dict(exact_stem_match), {"source": "stem", "key": filename_stem, "reason": "filename_stem_exact_match"}

    normalized_stem = slugify(filename_stem)
    if normalized_stem:
        matched = _lookup(normalized_stem)
        if matched:
            metadata, key = matched
            return metadata, {"source": "normalized_stem", "key": key, "reason": "normalized_filename_stem_match"}

    alias_values = [filename_stem, canonical_slug]
    if persisted_aliases:
        alias_values.extend([slugify(alias) for alias in persisted_aliases if slugify(alias)])
    normalized_aliases = {slugify(value) for value in alias_values if slugify(value)}
    if normalized_aliases:
        matched_entry, matched_key, ambiguous_keys = _resolve_unique_alias_match(
            metadata_map,
            normalized_aliases=normalized_aliases,
        )
        if matched_entry is not None and matched_key is not None:
            return matched_entry, {"source": "alias", "key": matched_key, "reason": "alias_unique_match"}
        if ambiguous_keys:
            logger.warning(
                "Content metadata ambiguous alias artwork=%s aliases=%s candidates=%s ignored_for_safety=true reason=ambiguous_alias",
                path.name,
                ",".join(sorted(normalized_aliases)),
                ",".join(ambiguous_keys[:5]),
            )
            fallback_key = normalized_stem or canonical_slug or slugify(path.name) or "unknown"
            return {}, {
                "source": "ambiguous_alias",
                "key": fallback_key,
                "reason": "ambiguous_alias_candidates",
                "weak_fallback_reason": "ambiguous_alias",
            }

    fallback_key = normalized_stem or canonical_slug or slugify(path.name) or "unknown"
    return {}, {"source": "fallback", "key": fallback_key, "reason": "no_matching_metadata_source", "weak_fallback_reason": "metadata_not_found"}


def resolve_artwork_metadata_for_path(path: pathlib.Path, metadata_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    metadata, _ = resolve_artwork_metadata_with_source(path, metadata_map)
    return metadata


def title_is_sluglike_or_generic(title: str, *, artwork_path: Optional[pathlib.Path] = None) -> Tuple[bool, List[str]]:
    normalized = re.sub(r"\s+", " ", str(title or "").strip())
    lowered = normalized.lower()
    reasons: List[str] = []
    if not normalized:
        return True, ["title_missing"]
    if lowered in {"untitled", "untitled design", "signature", "signature product", "product", "design", "artwork"}:
        reasons.append("title_generic_phrase")
    if re.fullmatch(r"[a-f0-9]{24,64}", lowered):
        reasons.append("title_hash_like")
    if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", lowered):
        reasons.append("title_uuid_like")
    slug_or_name = ""
    if artwork_path is not None:
        slug_or_name = filename_slug_to_title(artwork_path.stem)
    if slug_or_name and lowered == slug_or_name.lower():
        quality_reason = filename_title_quality_reason(artwork_path.stem)
        if quality_reason in {"uuid_like", "hex_like", "long_numeric", "hashy_slug"}:
            reasons.append("title_slug_fallback_noisy")
    if re.search(r"\b(ai generated|generated image|stock photo)\b", lowered):
        reasons.append("title_generic_generator_phrase")
    if re.search(r"\d{5,}", lowered):
        reasons.append("title_long_numeric_suffix")
    if re.fullmatch(r"[a-z0-9\-_,. ]{18,}", lowered):
        reasons.append("title_slug_like")
    return bool(reasons), sorted(set(reasons))


def description_is_weak_fallback(description: str) -> Tuple[bool, List[str]]:
    plain = _normalize_whitespace(re.sub(r"<[^>]+>", " ", str(description or "")))
    lowered = plain.lower()
    reasons: List[str] = []
    if not plain:
        reasons.append("description_missing")
    if len(plain.split()) < 8:
        reasons.append("description_too_short")
    generic_phrases = {
        "perfect for any occasion",
        "great gift idea",
        "adds an easy style upgrade",
        "versatile piece",
        "print on demand",
    }
    if any(phrase in lowered for phrase in generic_phrases):
        reasons.append("description_generic_phrase")
    return bool(reasons), sorted(set(reasons))


def metadata_is_missing_or_weak(
    metadata: Dict[str, Any],
    *,
    artwork_path: pathlib.Path,
    metadata_source: str = "",
    only_when_weak: bool = True,
) -> Tuple[bool, List[str]]:
    if not metadata:
        return True, ["metadata_missing"]
    reasons: List[str] = []
    provenance = _normalize_metadata_provenance(metadata.get("metadata_provenance"))
    upgrade_fingerprint = str(metadata.get("inline_upgrade_fingerprint") or "").strip()
    title = str(metadata.get("title") or "").strip()
    if not title:
        reasons.append("title_missing")
    else:
        weak_title, title_reasons = title_is_sluglike_or_generic(title, artwork_path=artwork_path)
        if weak_title:
            reasons.extend(title_reasons)
    weak_description, desc_reasons = description_is_weak_fallback(str(metadata.get("description") or ""))
    if weak_description:
        reasons.extend(desc_reasons)
    tags = [normalize_theme_tag(tag) for tag in _split_keywords(metadata.get("tags"))]
    tags = [tag for tag in tags if tag and tag not in TAG_GENERIC_TOKENS]
    if len(tags) < 2:
        reasons.append("tags_sparse")
    if "title_slug_like" in reasons and metadata_source == "sidecar":
        if str(metadata.get("description") or "").strip() and len(tags) >= 2:
            reasons = [reason for reason in reasons if reason != "title_slug_like"]
            reasons.append("title_slug_like_curated_ok")
    if provenance in {"inline_openai", "inline_vision", "inline_heuristic"} and upgrade_fingerprint:
        reasons = [reason for reason in reasons if reason not in {"title_slug_like", "description_too_short"}]
    if metadata_source == "sidecar":
        # Keep concise sidecars intact unless signals are clearly weak.
        reasons = [reason for reason in reasons if reason not in {"description_too_short", "title_slug_like_curated_ok"}]
    if not only_when_weak:
        return True, reasons or ["metadata_refresh_requested"]
    return bool(reasons), sorted(set(reasons))


PROMPT_RESIDUE_PATTERNS = [
    re.compile(r"(?im)^\s*(style notes?|keywords?|prompt|prompt notes?|negative prompt)\s*:\s*.*$"),
]


def sanitize_description_text(value: str) -> str:
    text = _normalize_whitespace(re.sub(r"<[^>]+>", " ", str(value or "")))
    for pattern in PROMPT_RESIDUE_PATTERNS:
        text = pattern.sub(" ", text)
    text = re.sub(r"\b(great for any occasion|perfect for any occasion)\b", "", text, flags=re.I)
    text = re.sub(r"\bmade for general\b", "", text, flags=re.I)
    text = re.sub(r"\ba dynamic scene of a\b", "", text, flags=re.I)
    text = re.sub(r"\b(style mood:)\b", "", text, flags=re.I)
    return _normalize_whitespace(text)


def sanitize_metadata_for_publish(metadata: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(metadata, dict):
        return {}
    cleaned = dict(metadata)
    cleaned["title"] = re.sub(r"\s+", " ", str(metadata.get("title") or "").strip())
    cleaned["subtitle"] = sanitize_description_text(str(metadata.get("subtitle") or ""))
    cleaned["description"] = sanitize_description_text(str(metadata.get("description") or ""))
    raw_tags = _split_keywords(metadata.get("tags"))
    sanitized_tags: List[str] = []
    seen: set[str] = set()
    for tag in raw_tags:
        normalized = normalize_theme_tag(tag)
        if not normalized or normalized in seen:
            continue
        if normalized in TAG_GENERIC_TOKENS and len(raw_tags) > 5:
            continue
        if normalized in {"keywords", "style notes", "prompt", "ai generated"}:
            continue
        seen.add(normalized)
        sanitized_tags.append(normalized)
    cleaned["tags"] = sanitized_tags
    cleaned["seo_keywords"] = [normalize_theme_tag(v) for v in _split_keywords(metadata.get("seo_keywords")) if normalize_theme_tag(v)]
    return cleaned


def metadata_fingerprint(metadata: Dict[str, Any]) -> str:
    normalized = {
        "title": str(metadata.get("title") or "").strip(),
        "description": str(metadata.get("description") or "").strip(),
        "tags": [normalize_theme_tag(v) for v in _split_keywords(metadata.get("tags")) if normalize_theme_tag(v)],
    }
    payload = json.dumps(normalized, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def persist_inline_metadata_sidecar(
    *,
    artwork: Artwork,
    candidate_metadata: Dict[str, Any],
    generator_name: str,
    weak_reasons: List[str],
    metadata_source: str,
    inline_resolution_source: str = "",
) -> Tuple[bool, str]:
    sidecar_path = artwork.src_path.with_suffix(".json")
    existing_raw = load_json(sidecar_path, {}) if sidecar_path.exists() else {}
    existing = existing_raw if isinstance(existing_raw, dict) else {}
    sanitized_candidate = sanitize_metadata_for_publish(candidate_metadata)
    generated_fp = metadata_fingerprint(sanitized_candidate)
    existing_fp = str(existing.get("inline_upgrade_fingerprint") or "")
    existing_quality_reasons = metadata_is_missing_or_weak(
        sanitize_metadata_for_publish(existing),
        artwork_path=artwork.src_path,
        metadata_source="sidecar" if sidecar_path.exists() else metadata_source,
        only_when_weak=True,
    )[1]
    candidate_quality_reasons = metadata_is_missing_or_weak(
        sanitized_candidate,
        artwork_path=artwork.src_path,
        metadata_source="inline_generated",
        only_when_weak=True,
    )[1]
    materially_better = len(candidate_quality_reasons) < len(existing_quality_reasons) or not existing
    if existing_fp == generated_fp:
        return False, "unchanged_fingerprint"
    if not materially_better:
        return False, "not_materially_better"
    payload = dict(existing)
    payload.update(sanitized_candidate)
    normalized_source = _normalize_metadata_provenance(inline_resolution_source)
    provenance = normalized_source if normalized_source.startswith("inline_") else f"inline_{generator_name}"
    if _is_prompt_generated_artwork_slug(artwork.slug or artwork.src_path.stem):
        provenance = f"prompt_art_run:{provenance}"
    payload["metadata_provenance"] = provenance
    payload["metadata_generator"] = str(generator_name or "").strip()
    payload["inline_upgrade_fingerprint"] = generated_fp
    payload["inline_upgrade_timestamp"] = datetime.now(timezone.utc).isoformat()
    payload["inline_upgrade_reasons"] = sorted(set(weak_reasons))
    save_json_atomic(sidecar_path, payload)
    return True, "written"


def filename_title_quality_reason(value: str) -> str:
    lowered = value.strip().lower()
    if not lowered:
        return "empty"
    if _is_prompt_generated_artwork_slug(lowered):
        return "prompt_generated_slug"
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
    for tag in _split_keywords(metadata.get("tags")):
        cleaned_tag = normalize_theme_tag(tag)
        if cleaned_tag and cleaned_tag not in TAG_GENERIC_TOKENS:
            candidates.append(cleaned_tag)
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
    if quality_reason == "prompt_generated_slug" and _is_weak_title_phrase(fallback):
        fallback = ""
    if quality_reason == "prompt_generated_slug" and not fallback:
        fallback = product_label or "Product"
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


def _template_intended_family(template: ProductTemplate) -> str:
    hint = " ".join(
        [
            str(template.key or ""),
            str(template.product_type_label or ""),
            str(template.shopify_product_type or ""),
        ]
    ).lower()
    if "phone_case" in hint or "phone case" in hint:
        return "phone_case"
    if "sticker" in hint:
        return "sticker"
    if "canvas" in hint or "wall art" in hint:
        return "canvas"
    if "framed_poster" in hint or "framed poster" in hint:
        return "framed_poster"
    if "tumbler" in hint:
        return "tumbler"
    if "travel_mug" in hint or "travel mug" in hint:
        return "travel_mug"
    if "throw_pillow" in hint or "throw pillow" in hint or "pillow" in hint:
        return "throw_pillow"
    if "blanket" in hint or "throw" in hint or "fleece" in hint:
        return "blanket"
    if "embroidered_hat" in hint or "embroidered hat" in hint:
        return "embroidered_hat"
    return "other"


def _collect_option_names_and_values(variants: List[Dict[str, Any]]) -> Tuple[List[str], Dict[str, List[str]]]:
    option_names: set[str] = set()
    option_values: Dict[str, set[str]] = {}
    for variant in variants:
        options = variant.get("options", {})
        if not isinstance(options, dict):
            continue
        for option_name, option_value in options.items():
            name = str(option_name or "").strip()
            if not name:
                continue
            option_names.add(name)
            normalized_name = name.lower()
            if normalized_name not in option_values:
                option_values[normalized_name] = set()
            value = str(option_value or "").strip()
            if value:
                option_values[normalized_name].add(value)
    sorted_names = sorted(option_names, key=lambda item: item.lower())
    summarized_values = {key: sorted(values) for key, values in sorted(option_values.items())}
    return sorted_names, summarized_values


def _looks_like_phone_model_value(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return False
    canonical_tokens = (
        "iphone",
        "samsung galaxy",
        "galaxy s",
        "galaxy note",
        "google pixel",
        "pixel ",
        "oneplus",
        "motorola",
        "moto ",
    )
    if any(token in normalized for token in canonical_tokens):
        return True
    return bool(re.search(r"\b(?:iphone|pixel|galaxy)\s*[a-z]?\d{1,2}(?:\s*(?:pro|max|plus|ultra))?\b", normalized))


def _looks_like_canvas_size(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return False
    if "canvas" in normalized:
        return True
    return bool(re.search(r"\d{1,3}\s*(?:\"|″|in)\s*[x×]\s*\d{1,3}\s*(?:\"|″|in)?", normalized))


def _looks_like_blanket_size(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return False
    if "blanket" in normalized or "throw" in normalized:
        return True
    return bool(re.search(r"\d{1,3}\s*(?:\"|in)\s*[x×]\s*\d{1,3}\s*(?:\"|in)?", normalized))


def _looks_like_throw_pillow_size(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return False
    if "pillow" in normalized:
        return True
    match = re.search(r"(\d{1,3})\s*(?:\"|in)\s*[x×]\s*(\d{1,3})\s*(?:\"|in)?", normalized)
    if not match:
        return False
    width = int(match.group(1))
    height = int(match.group(2))
    return abs(width - height) <= 4


def _looks_like_framed_poster_size(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return False
    if "vertical" in normalized or "horizontal" in normalized:
        return True
    return bool(re.search(r"\d{1,3}\s*(?:\"|″|in)\s*[x×]\s*\d{1,3}\s*(?:\"|″|in)?", normalized))


def _looks_like_drinkware_capacity(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return False
    return bool(re.search(r"\b\d{1,2}\s*(?:oz|ounce|ounces)\b", normalized))


def validate_catalog_family_schema(
    *,
    template: ProductTemplate,
    variants: List[Dict[str, Any]],
    blueprint_title: str = "",
    provider_title: str = "",
) -> CatalogFamilyValidationResult:
    intended_family = _template_intended_family(template)
    if intended_family == "other":
        return CatalogFamilyValidationResult(intended_family=intended_family, plausible=True)

    option_names, option_values_summary = _collect_option_names_and_values(variants)
    option_name_tokens = {name.lower() for name in option_names}
    all_values = {value.lower() for values in option_values_summary.values() for value in values}
    title_tokens = f"{blueprint_title} {provider_title}".lower()

    if intended_family == "phone_case":
        has_model_dimension = any(token in option_name_tokens for token in ("model", "device model", "device", "phone model", "compatibility"))
        has_surface_dimension = any(token in option_name_tokens for token in ("surface", "finish"))
        size_values = option_values_summary.get("size", [])
        has_phone_model_like_size_values = any(_looks_like_phone_model_value(value) for value in size_values)
        has_size_surface_alias_schema = ("size" in option_name_tokens) and has_surface_dimension and has_phone_model_like_size_values
        has_model_schema = has_model_dimension or has_size_surface_alias_schema
        has_title_signal = any(token in title_tokens for token in ("phone case", "iphone", "samsung case", "tough case", "slim case"))
        apparel_size_values = {"s", "m", "l", "xl", "2xl", "3xl"}
        mostly_apparel_sizes = bool(all_values.intersection(apparel_size_values))
        if not (has_model_schema or has_title_signal):
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason=(
                    "Phone case schema mismatch: missing model/device-style schema (or size+surface phone-model alias) "
                    "and family-title hints; "
                    f"option_names={option_names}"
                ),
            )
        if mostly_apparel_sizes and not has_model_schema:
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason="Phone case schema mismatch: variant values look apparel-sized (S/M/L/XL) without device model options.",
            )
        return CatalogFamilyValidationResult(intended_family=intended_family, plausible=True)

    if intended_family == "sticker":
        has_sticker_size = any(token in option_name_tokens for token in ("size", "sticker size", "dimensions"))
        has_shape = any(token in option_name_tokens for token in ("shape", "cut", "finish"))
        has_title_signal = "sticker" in title_tokens
        textile_signals = any("seam thread" in value for value in all_values)
        apparel_size_values = {"s", "m", "l", "xl", "2xl", "3xl"}
        mostly_apparel_sizes = bool(all_values.intersection(apparel_size_values))
        if textile_signals:
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason="Sticker schema mismatch: textile/thread attributes detected in variant options.",
            )
        if mostly_apparel_sizes and not has_shape:
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason="Sticker schema mismatch: only apparel-like sizes present without sticker shape/finish dimensions.",
            )
        if not (has_sticker_size and (has_shape or has_title_signal)):
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason=f"Sticker schema mismatch: insufficient sticker-like options. option_names={option_names}",
            )
        return CatalogFamilyValidationResult(intended_family=intended_family, plausible=True)

    if intended_family == "canvas":
        has_size_dimension = any(token in option_name_tokens for token in ("size", "dimensions"))
        size_values = option_values_summary.get("size", []) + option_values_summary.get("dimensions", [])
        has_canvas_sizes = any(_looks_like_canvas_size(value) for value in size_values)
        has_title_signal = any(token in title_tokens for token in ("canvas", "framed canvas", "wall art", "gallery wrap"))
        wrong_family_title = any(token in title_tokens for token in ("blanket", "hoodie", "t-shirt", "sweatshirt", "phone case", "sticker"))
        has_device_or_apparel_schema = any(token in option_name_tokens for token in ("model", "device", "surface", "color"))
        if wrong_family_title:
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason="Canvas schema mismatch: blueprint/provider title indicates a different product family.",
            )
        if has_device_or_apparel_schema and not has_canvas_sizes:
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason="Canvas schema mismatch: model/device/apparel-style options detected without canvas-like dimensions.",
            )
        if not ((has_size_dimension and has_canvas_sizes) or has_title_signal):
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason=f"Canvas schema mismatch: missing canvas-like dimensions/title hints. option_names={option_names}",
            )
        return CatalogFamilyValidationResult(intended_family=intended_family, plausible=True)

    if intended_family == "framed_poster":
        has_size_dimension = any(token in option_name_tokens for token in ("size", "dimensions"))
        size_values = option_values_summary.get("size", []) + option_values_summary.get("dimensions", [])
        has_framed_sizes = any(_looks_like_framed_poster_size(value) for value in size_values)
        has_frame_dimension = any(token in option_name_tokens for token in ("frame", "frame color", "frame color/mat", "frame/mat"))
        has_title_signal = any(token in title_tokens for token in ("framed poster", "framed print", "framed art"))
        wrong_family_title = any(token in title_tokens for token in ("hoodie", "t-shirt", "sweatshirt", "phone case", "sticker", "mug", "tumbler", "travel mug"))
        if wrong_family_title:
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason="Framed poster schema mismatch: blueprint/provider title indicates a different product family.",
            )
        if not ((has_size_dimension and has_framed_sizes) and (has_frame_dimension or has_title_signal)):
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason=f"Framed poster schema mismatch: missing framed-poster size/frame schema. option_names={option_names}",
            )
        return CatalogFamilyValidationResult(intended_family=intended_family, plausible=True)

    if intended_family in {"tumbler", "travel_mug"}:
        has_capacity_dimension = any(token in option_name_tokens for token in ("size", "capacity"))
        capacity_values = option_values_summary.get("size", []) + option_values_summary.get("capacity", [])
        has_capacity_values = any(_looks_like_drinkware_capacity(value) for value in capacity_values)
        has_color_or_finish = any(token in option_name_tokens for token in ("color", "finish", "surface"))
        has_minimal_capacity_schema = has_capacity_dimension and has_capacity_values
        if intended_family == "tumbler":
            title_keywords = ("tumbler", "skinny tumbler")
            wrong_keywords = ("travel mug", "phone case", "sticker", "hoodie", "t-shirt")
            mismatch_reason = "Tumbler schema mismatch: missing tumbler capacity schema/title hints."
            has_structural_schema = has_minimal_capacity_schema and has_color_or_finish
        else:
            title_keywords = ("travel mug", "commuter mug")
            wrong_keywords = ("tumbler", "phone case", "sticker", "hoodie", "t-shirt")
            mismatch_reason = "Travel mug schema mismatch: missing travel-mug capacity schema/title hints."
            # Some valid travel-mug catalog mappings expose only a single capacity
            # option (for example, 15oz) without a separate color/finish dimension.
            has_structural_schema = has_minimal_capacity_schema
        has_title_signal = any(token in title_tokens for token in title_keywords)
        wrong_family_title = any(token in title_tokens for token in wrong_keywords)
        if wrong_family_title:
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason=f"{'Tumbler' if intended_family == 'tumbler' else 'Travel mug'} schema mismatch: blueprint/provider title indicates a different product family.",
            )
        if not (has_structural_schema or has_title_signal):
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason=f"{mismatch_reason} option_names={option_names}",
            )
        return CatalogFamilyValidationResult(intended_family=intended_family, plausible=True)

    if intended_family == "blanket":
        has_size_dimension = any(token in option_name_tokens for token in ("size", "dimensions"))
        size_values = option_values_summary.get("size", []) + option_values_summary.get("dimensions", [])
        has_blanket_sizes = any(_looks_like_blanket_size(value) for value in size_values)
        has_material_signal = any(token in all_values for token in ("fleece", "mink", "sherpa", "woven", "plush"))
        has_title_signal = any(token in title_tokens for token in ("blanket", "throw", "fleece", "sherpa"))
        wrong_family_title = any(token in title_tokens for token in ("hoodie", "t-shirt", "sweatshirt", "phone case", "sticker", "canvas", "poster"))
        apparel_sizes = {"s", "m", "l", "xl", "2xl", "3xl"}
        has_only_apparel_sizes = bool(all_values.intersection(apparel_sizes)) and not has_blanket_sizes
        if wrong_family_title:
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason="Blanket schema mismatch: blueprint/provider title indicates a different product family.",
            )
        if has_only_apparel_sizes:
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason="Blanket schema mismatch: only apparel-like sizes present without blanket dimensions.",
            )
        if not ((has_size_dimension and has_blanket_sizes) or has_material_signal or has_title_signal):
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason=f"Blanket schema mismatch: missing blanket/throw/fleece-style dimensions or material hints. option_names={option_names}",
            )
        return CatalogFamilyValidationResult(intended_family=intended_family, plausible=True)

    if intended_family == "throw_pillow":
        has_size_dimension = any(token in option_name_tokens for token in ("size", "dimensions"))
        size_values = option_values_summary.get("size", []) + option_values_summary.get("dimensions", [])
        has_pillow_sizes = any(_looks_like_throw_pillow_size(value) for value in size_values)
        has_material_signal = any(token in all_values for token in ("polyester", "linen", "spun", "faux suede", "insert", "cover"))
        has_title_signal = any(token in title_tokens for token in ("pillow", "throw pillow", "pillow cover"))
        wrong_family_title = any(token in title_tokens for token in ("hoodie", "t-shirt", "sweatshirt", "phone case", "sticker", "canvas", "blanket"))
        if wrong_family_title:
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason="Throw pillow schema mismatch: blueprint/provider title indicates a different product family.",
            )
        if not ((has_size_dimension and has_pillow_sizes) or has_material_signal or has_title_signal):
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason=f"Throw pillow schema mismatch: missing pillow-like square dimensions/material/title hints. option_names={option_names}",
            )
        return CatalogFamilyValidationResult(intended_family=intended_family, plausible=True)

    if intended_family == "embroidered_hat":
        option_names_flat = " ".join(option_name_tokens)
        has_hat_signal = any(token in title_tokens for token in ("hat", "cap", "dad hat", "trucker"))
        has_embroidery_signal = any(token in title_tokens for token in ("embroidered", "embroidery")) or ("embroid" in option_names_flat)
        wrong_family_title = any(token in title_tokens for token in ("hoodie", "t-shirt", "sweatshirt", "phone case", "sticker", "blanket", "poster"))
        if wrong_family_title:
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason="Embroidered hat schema mismatch: blueprint/provider title indicates a different product family.",
            )
        if not (has_hat_signal and has_embroidery_signal):
            return CatalogFamilyValidationResult(
                intended_family=intended_family,
                plausible=False,
                reason=f"Embroidered hat schema mismatch: missing hat+embroidery signals. option_names={option_names}",
            )
        return CatalogFamilyValidationResult(intended_family=intended_family, plausible=True)

    return CatalogFamilyValidationResult(intended_family=intended_family, plausible=True)


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


def _provider_is_printify_choice(provider: Dict[str, Any]) -> bool:
    title = str(provider.get("title") or provider.get("name") or "").strip().lower()
    return "printify choice" in title


def _discover_family_catalog_mapping(
    *,
    printify: PrintifyClient,
    template: ProductTemplate,
) -> Optional[Tuple[int, int]]:
    intended_family = _template_intended_family(template)
    if intended_family == "other":
        return None
    if not all(hasattr(printify, method) for method in ("list_blueprints", "list_print_providers", "list_variants")):
        return None

    family_queries = {
        "phone_case": ["phone case", "tough case", "slim case"],
        "sticker": ["kiss-cut sticker", "sticker", "die-cut sticker"],
        "canvas": ["canvas", "framed canvas", "wall art"],
        "framed_poster": ["framed poster", "framed print", "framed art"],
        "tumbler": ["tumbler", "skinny tumbler", "20oz tumbler"],
        "travel_mug": ["travel mug", "commuter mug", "insulated travel mug"],
        "blanket": ["blanket", "throw blanket", "fleece blanket"],
        "throw_pillow": ["throw pillow", "pillow", "pillow cover"],
        "embroidered_hat": ["embroidered hat", "dad hat embroidery", "trucker cap embroidery"],
    }
    queries = family_queries.get(intended_family, [])
    discovery_cache_key = f"family_discovery:{template.key}:{intended_family}:{template.printify_blueprint_id}:{template.printify_print_provider_id}"
    if getattr(printify, "catalog_cache", None):
        cached_discovery = printify.catalog_cache.get(discovery_cache_key)
        if isinstance(cached_discovery, list) and len(cached_discovery) == 2:
            return int(cached_discovery[0]), int(cached_discovery[1])
        if isinstance(cached_discovery, tuple) and len(cached_discovery) == 2:
            return int(cached_discovery[0]), int(cached_discovery[1])
    blueprints = printify.list_blueprints()
    seen_blueprint_ids: set[int] = set()
    candidates: List[Dict[str, Any]] = []
    for query in queries:
        for blueprint in search_blueprints(blueprints, query):
            blueprint_id = int(blueprint.get("id") or 0)
            if blueprint_id <= 0 or blueprint_id in seen_blueprint_ids:
                continue
            seen_blueprint_ids.add(blueprint_id)
            candidates.append(blueprint)

    best_pair: Optional[Tuple[int, int]] = None
    best_score = -1
    for blueprint in candidates[:60]:
        blueprint_id = int(blueprint.get("id") or 0)
        if blueprint_id <= 0:
            continue
        blueprint_title = str(blueprint.get("title") or "")
        try:
            providers = printify.list_print_providers(blueprint_id)
        except Exception:
            continue
        for provider in providers:
            provider_id = int(provider.get("id") or 0)
            if provider_id <= 0:
                continue
            provider_title = str(provider.get("title") or provider.get("name") or "")
            try:
                variants = normalize_catalog_variants_response(printify.list_variants(blueprint_id, provider_id))
            except Exception:
                continue
            if not variants:
                continue
            validation = validate_catalog_family_schema(
                template=template,
                variants=variants,
                blueprint_title=blueprint_title,
                provider_title=provider_title,
            )
            if not validation.plausible:
                continue
            option_names, _ = _collect_option_names_and_values(variants)
            score = len(variants)
            if intended_family == "phone_case":
                if any("model" in name.lower() or "device" in name.lower() for name in option_names):
                    score += 1000
            elif intended_family == "sticker":
                lowered = {name.lower() for name in option_names}
                if "shape" in lowered:
                    score += 1000
                if "size" in lowered:
                    score += 500
            elif intended_family == "canvas":
                lowered = {name.lower() for name in option_names}
                if "size" in lowered or "dimensions" in lowered:
                    score += 800
                if "canvas" in blueprint_title.lower():
                    score += 500
                if "wall art" in blueprint_title.lower():
                    score += 200
            elif intended_family == "framed_poster":
                lowered = {name.lower() for name in option_names}
                if "size" in lowered or "dimensions" in lowered:
                    score += 800
                if any(token in lowered for token in ("frame", "frame color", "frame color/mat")):
                    score += 500
                if "framed" in blueprint_title.lower():
                    score += 400
            elif intended_family in {"tumbler", "travel_mug"}:
                lowered = {name.lower() for name in option_names}
                if "size" in lowered or "capacity" in lowered:
                    score += 700
                if any(token in lowered for token in ("color", "finish", "surface")):
                    score += 200
                title_blob = f"{blueprint_title} {provider_title}".lower()
                if intended_family == "tumbler" and "tumbler" in title_blob:
                    score += 500
                if intended_family == "travel_mug" and "travel mug" in title_blob:
                    score += 500
            elif intended_family == "blanket":
                lowered = {name.lower() for name in option_names}
                if "size" in lowered or "dimensions" in lowered:
                    score += 800
                title_blob = f"{blueprint_title} {provider_title}".lower()
                if "blanket" in title_blob or "throw" in title_blob:
                    score += 500
                if "fleece" in title_blob or "sherpa" in title_blob:
                    score += 200
            elif intended_family == "throw_pillow":
                lowered = {name.lower() for name in option_names}
                if "size" in lowered or "dimensions" in lowered:
                    score += 700
                title_blob = f"{blueprint_title} {provider_title}".lower()
                if "pillow" in title_blob:
                    score += 500
                if "cover" in title_blob:
                    score += 150
            elif intended_family == "embroidered_hat":
                title_blob = f"{blueprint_title} {provider_title}".lower()
                if "hat" in title_blob or "cap" in title_blob:
                    score += 400
                if "embroider" in title_blob:
                    score += 700
            if _provider_is_printify_choice(provider):
                score += 200
            if score > best_score:
                best_score = score
                best_pair = (blueprint_id, provider_id)
    if getattr(printify, "catalog_cache", None) and best_pair is not None:
        printify.catalog_cache.set(discovery_cache_key, [int(best_pair[0]), int(best_pair[1])])
    return best_pair


def _mapping_is_plausible_for_template(
    *,
    printify: PrintifyClient,
    template: ProductTemplate,
    blueprint_id: int,
    provider_id: int,
) -> Tuple[bool, str]:
    plausibility_cache_key = (
        f"mapping_plausibility:{template.key}:{blueprint_id}:{provider_id}:"
        f"{template.pinned_blueprint_id}:{template.pinned_provider_id}"
    )
    if getattr(printify, "catalog_cache", None):
        cached = printify.catalog_cache.get(plausibility_cache_key)
        if isinstance(cached, dict):
            return bool(cached.get("plausible", False)), str(cached.get("reason", "cached"))
    if blueprint_id <= 0 or provider_id <= 0:
        return False, "template_mapping_ids_missing"
    intended_family = _template_intended_family(template)
    if intended_family == "other":
        return True, "family_validation_not_required"
    try:
        variants = normalize_catalog_variants_response(printify.list_variants(blueprint_id, provider_id))
    except Exception as exc:
        return False, f"template_mapping_variant_fetch_failed:{type(exc).__name__}"
    if not variants:
        return False, "template_mapping_zero_variants"
    validation = validate_catalog_family_schema(template=template, variants=variants)
    if validation.plausible:
        result = (True, "template_mapping_family_plausible")
    else:
        result = (False, f"template_mapping_family_mismatch:{validation.reason or 'unknown'}")
    if getattr(printify, "catalog_cache", None):
        printify.catalog_cache.set(plausibility_cache_key, {"plausible": result[0], "reason": result[1]})
    return result


def select_provider_for_template(
    *,
    printify: PrintifyClient,
    template: ProductTemplate,
) -> ProductTemplate:
    resolved_template, _ = _resolve_template_catalog_mapping(
        printify=printify,
        template=template,
        discovery_mode="normal",
    )
    return resolved_template


def _resolve_template_catalog_mapping(
    *,
    printify: PrintifyClient,
    template: ProductTemplate,
    discovery_mode: str = "normal",
) -> Tuple[ProductTemplate, CatalogResolutionDiagnostics]:
    if template.key == "tote_basic":
        diagnostics = CatalogResolutionDiagnostics(
            template_key=template.key,
            template_hint_blueprint_id=int(template.printify_blueprint_id),
            template_hint_provider_id=int(template.printify_print_provider_id),
            resolved_blueprint_id=int(template.printify_blueprint_id),
            resolved_provider_id=int(template.printify_print_provider_id),
            discovery_mode=discovery_mode,
        )
        return template, diagnostics
    if not hasattr(printify, "list_print_providers") or not hasattr(printify, "list_variants"):
        diagnostics = CatalogResolutionDiagnostics(
            template_key=template.key,
            template_hint_blueprint_id=int(template.printify_blueprint_id),
            template_hint_provider_id=int(template.printify_print_provider_id),
            resolved_blueprint_id=int(template.printify_blueprint_id),
            resolved_provider_id=int(template.printify_print_provider_id),
            discovery_mode=discovery_mode,
        )
        return template, diagnostics

    blueprint_id = int(template.pinned_blueprint_id or template.printify_blueprint_id)
    provider_id = int(template.pinned_provider_id or template.printify_print_provider_id)
    diagnostics = CatalogResolutionDiagnostics(
        template_key=template.key,
        template_hint_blueprint_id=int(template.printify_blueprint_id),
        template_hint_provider_id=int(template.printify_print_provider_id),
        resolved_blueprint_id=blueprint_id,
        resolved_provider_id=provider_id,
        discovery_mode=discovery_mode,
        pinned_attempted_first=(blueprint_id > 0 and provider_id > 0),
    )

    should_discover = discovery_mode != "normal"
    if discovery_mode == "normal":
        plausible, reason = _mapping_is_plausible_for_template(
            printify=printify,
            template=template,
            blueprint_id=blueprint_id,
            provider_id=provider_id,
        )
        if not plausible:
            should_discover = True
            diagnostics.fallback_discovery_triggered = True
            diagnostics.fallback_discovery_reason = reason

    discovered_pair = _discover_family_catalog_mapping(printify=printify, template=template) if should_discover else None
    diagnostics.discovery_used = should_discover
    if discovered_pair is not None:
        blueprint_id, discovered_provider_id = discovered_pair
        if (
            blueprint_id != int(template.printify_blueprint_id)
            or discovered_provider_id != int(template.printify_print_provider_id)
        ):
            logger.info(
                "Catalog mapping override template=%s template_hint_blueprint_id=%s template_hint_provider_id=%s resolved_blueprint_id=%s resolved_provider_id=%s source=runtime_family_discovery",
                template.key,
                template.printify_blueprint_id,
                template.printify_print_provider_id,
                blueprint_id,
                discovered_provider_id,
            )
        template = replace(template, printify_blueprint_id=blueprint_id, printify_print_provider_id=discovered_provider_id)
        provider_id = discovered_provider_id
    diagnostics.resolved_blueprint_id = blueprint_id
    diagnostics.resolved_provider_id = provider_id
    try:
        providers = printify.list_print_providers(blueprint_id)
    except Exception:
        return template, diagnostics
    if not providers:
        return template, diagnostics

    providers_by_id = {int(provider.get("id") or 0): provider for provider in providers}
    provider_candidates: List[int] = []
    pinned_provider = int(template.pinned_provider_id or 0)
    if pinned_provider > 0:
        provider_candidates.append(pinned_provider)
    provider_candidates.extend([int(pid) for pid in (template.provider_preference_order or []) if int(pid) > 0])
    if template.provider_selection_strategy.startswith("prefer_printify_choice"):
        for provider in providers:
            pid = int(provider.get("id") or 0)
            if pid > 0 and _provider_is_printify_choice(provider):
                provider_candidates.insert(0, pid)
    if pinned_provider <= 0 and int(template.printify_print_provider_id or 0) > 0:
        provider_candidates.append(int(template.printify_print_provider_id))

    seen: set[int] = set()
    ordered_candidates: List[int] = []
    for pid in provider_candidates:
        if pid > 0 and pid in providers_by_id and pid not in seen:
            seen.add(pid)
            ordered_candidates.append(pid)

    best_scored: Optional[Tuple[int, float]] = None
    for provider in providers:
        pid = int(provider.get("id") or 0)
        if pid <= 0:
            continue
        try:
            variants = printify.list_variants(blueprint_id, pid)
        except Exception:
            continue
        if not variants:
            continue
        score = score_provider_for_template(provider, variants, template)
        cost_samples = [
            normalize_printify_price(v.get("cost") if v.get("cost") is not None else v.get("price"))
            for v in variants[:12]
            if (v.get("cost") is not None or v.get("price") is not None)
        ]
        avg_cost = float(sum(cost_samples) / len(cost_samples)) if cost_samples else 10_000.0
        composite = float(score["score"]) - (avg_cost / 10_000.0)
        if best_scored is None or composite > best_scored[1]:
            best_scored = (pid, composite)
        if pid not in seen:
            ordered_candidates.append(pid)
            seen.add(pid)

    if not template.fallback_provider_allowed and ordered_candidates:
        ordered_candidates = ordered_candidates[:1]

    selected_provider_id = ordered_candidates[0] if ordered_candidates else int(template.printify_print_provider_id)
    if selected_provider_id != int(template.printify_print_provider_id) or blueprint_id != int(template.printify_blueprint_id):
        logger.info(
            "Provider selection template=%s strategy=%s selected_blueprint_id=%s selected_provider_id=%s previous_blueprint_id=%s previous_provider_id=%s",
            template.key,
            template.provider_selection_strategy,
            blueprint_id,
            selected_provider_id,
            template.printify_blueprint_id,
            template.printify_print_provider_id,
        )
    resolved = replace(template, printify_blueprint_id=blueprint_id, printify_print_provider_id=selected_provider_id)
    diagnostics.resolved_provider_id = int(selected_provider_id)
    logger.info(
        "Catalog resolution diagnostics template=%s mode=%s template_hint_blueprint_id=%s template_hint_provider_id=%s resolved_blueprint_id=%s resolved_provider_id=%s pinned_attempted_first=%s discovery_used=%s fallback_discovery_triggered=%s fallback_discovery_reason=%s",
        diagnostics.template_key,
        diagnostics.discovery_mode,
        diagnostics.template_hint_blueprint_id,
        diagnostics.template_hint_provider_id,
        diagnostics.resolved_blueprint_id,
        diagnostics.resolved_provider_id,
        diagnostics.pinned_attempted_first,
        diagnostics.discovery_used,
        diagnostics.fallback_discovery_triggered,
        diagnostics.fallback_discovery_reason or "-",
    )
    return resolved, diagnostics


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
        if any(token in lowered for token in ("hoodie", "sweatshirt", "crewneck")):
            return "sweatshirt"
        if any(token in lowered for token in ("tee", "t-shirt", "shirt")):
            return "shirt"
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
        f"products_created_but_not_published={summary.products_created_but_not_published} "
        f"publish_queue_total_count={summary.publish_queue_total_count} "
        f"publish_queue_pending_count={summary.publish_queue_pending_count} "
        f"publish_queue_completed_count={summary.publish_queue_completed_count} "
        f"publish_queue_failed_count={summary.publish_queue_failed_count} "
        f"publish_rate_limit_events={summary.publish_rate_limit_events} "
        f"verification_warnings={summary.verification_warnings} "
        f"catalog_cache_hits={summary.catalog_cache_hits} "
        f"catalog_cache_misses={summary.catalog_cache_misses} "
        f"catalog_requests_avoided={summary.catalog_requests_avoided} "
        f"chunks_completed={summary.chunks_completed}/{summary.total_chunks or 1} "
        f"templates_skipped_catalog_rate_limited={summary.templates_skipped_catalog_rate_limited} "
        f"resumed_combinations={summary.resumed_combinations} "
        f"rate_limit_events={summary.rate_limit_events}"
    )


def log_run_summary(summary: RunSummary) -> None:
    logger.info(format_run_summary(summary))


def should_enable_progress(*, force_enable: Optional[bool], stream: Any = sys.stderr) -> Tuple[bool, str]:
    if force_enable is True:
        return True, "forced_on"
    if force_enable is False:
        return False, "forced_off"
    is_tty = bool(getattr(stream, "isatty", lambda: False)())
    ci_detected = any(str(os.getenv(key, "")).strip() for key in ("CI", "GITHUB_ACTIONS", "BUILD_NUMBER"))
    if not is_tty:
        return False, "non_tty"
    if ci_detected:
        return False, "ci_environment"
    return True, "interactive_tty"


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
    organization = build_normalized_shopify_organization(template, artwork)
    merged_style = [*template.style_keywords, *_split_keywords((artwork.metadata or {}).get("style_keywords"))]
    product_type = resolve_shopify_product_type(template)
    context.update(
        {
            "product_type_label": product_type,
            "style_keywords": ", ".join(list(dict.fromkeys(merged_style))[:6]),
            "family_label": content_engine.family_title_suffix(template),
            "family_key": str(organization.get("family_key") or ""),
            "primary_collection_handle": str(organization.get("primary_collection_handle") or ""),
            "primary_collection_title": str(organization.get("primary_collection_title") or ""),
            "department_key": str(organization.get("department_key") or ""),
            "department_label": str(organization.get("department_label") or ""),
            "normalized_theme_keys": ",".join(organization.get("normalized_theme_keys", [])),
            "normalized_audience_keys": ",".join(organization.get("normalized_audience_keys", [])),
            "normalized_season_keys": ",".join(organization.get("normalized_season_keys", [])),
            "merchandising_collection_handles": ",".join(organization.get("merchandising_collection_handles", [])),
            "recommended_product_type": str(organization.get("recommended_product_type") or ""),
            "recommended_shopify_category_label": str(organization.get("recommended_shopify_category_label") or ""),
        }
    )
    return context


def _maybe_render_ai_product_copy(template: ProductTemplate, artwork: Artwork, context: Dict[str, Any]) -> Optional[product_copy_generator.GeneratedProductCopy]:
    family = content_engine.infer_product_family(template)
    return product_copy_generator.maybe_generate_product_copy(
        template=template,
        artwork=artwork,
        context=context,
        family=family,
        enabled=AI_PRODUCT_COPY_SETTINGS.enabled,
        model=AI_PRODUCT_COPY_SETTINGS.model,
        api_key=AI_PRODUCT_COPY_SETTINGS.api_key,
    )


def render_product_title(template: ProductTemplate, artwork: Artwork) -> str:
    context = build_seo_context(template, artwork)
    ai_copy = _maybe_render_ai_product_copy(template, artwork, context)
    if ai_copy and ai_copy.title:
        return _refine_rendered_title(
            artwork_title=context.get("artwork_title", ""),
            rendered_title=ai_copy.title.strip(),
            product_label=_title_product_signal_label(template),
        )
    product_label = _title_product_signal_label(template)
    if product_label and title_semantically_includes_product_label(context.get("artwork_title", ""), product_label):
        context = dict(context)
        context["product_type_label"] = ""
    rendered = template.title_pattern.format(**context).strip()
    return _refine_rendered_title(
        artwork_title=context.get("artwork_title", ""),
        rendered_title=rendered,
        product_label=product_label,
    )


def _render_listing_tags(template: ProductTemplate, artwork: Artwork) -> List[str]:
    metadata = artwork.metadata or {}
    context = build_seo_context(template, artwork)
    organization = build_normalized_shopify_organization(template, artwork)
    taxonomy_tags = list(organization.get("smart_collection_tags", []))
    ai_copy = _maybe_render_ai_product_copy(template, artwork, context)
    target_max_tags = 12
    if ai_copy and ai_copy.tags:
        merged: List[str] = []
        seen: set[str] = set()
        for row in [*ai_copy.tags, *content_engine.family_tags(template), *taxonomy_tags]:
            cleaned = normalize_theme_tag(row)
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            merged.append(cleaned)
            if len(merged) >= target_max_tags:
                break
        if merged:
            return merged
    family = content_engine.infer_product_family(template)
    family_label = str(context.get("family_label", "")).strip().lower()
    shopify_type = (template.shopify_product_type or "").strip()
    shopify_type_tag = "" if shopify_type.lower() in {"apparel", "product", "accessories", "merchandise"} else shopify_type
    family_bucket = [family_label, template.product_type_label, shopify_type_tag, *content_engine.family_tags(template)]
    subject_bucket = [
        metadata.get("title"),
        context.get("artwork_title"),
        metadata.get("theme"),
        metadata.get("collection"),
        *_split_keywords(metadata.get("tags")),
        *artwork.tags,
    ]
    theme_style_bucket = [
        metadata.get("theme"),
        metadata.get("subtitle"),
        metadata.get("color_story"),
        *template.tags,
        *_split_keywords(metadata.get("seo_keywords")),
        *template.style_keywords,
        *_split_keywords(metadata.get("style_keywords")),
    ]
    optional_bucket = [metadata.get("audience"), metadata.get("occasion"), "inkvibe", "gift idea", *DEFAULT_TAGS]
    taxonomy_bucket: List[Any] = [f"family:{family.replace('_', '-')}"]
    department_key = str(organization.get("department_key") or "").strip()
    if department_key:
        taxonomy_bucket.append(f"dept:{department_key}")
    taxonomy_bucket.extend(f"theme:{key}" for key in organization.get("normalized_theme_keys", []))
    taxonomy_bucket.extend(f"audience:{key}" for key in organization.get("normalized_audience_keys", []))
    taxonomy_bucket.extend(f"season:{key}" for key in organization.get("normalized_season_keys", []))
    bucket_order = [taxonomy_bucket, family_bucket, subject_bucket, theme_style_bucket, optional_bucket]
    tags: List[str] = []
    seen: set[str] = set()
    max_tags = target_max_tags

    def _push_tag(raw: Any, *, allow_generic: bool = False) -> None:
        nonlocal tags
        cleaned = normalize_theme_tag(raw)
        if not cleaned:
            return
        if cleaned in seen:
            return
        if not allow_generic and cleaned in TAG_GENERIC_TOKENS:
            return
        seen.add(cleaned)
        tags.append(cleaned)

    for bucket in bucket_order:
        for row in bucket:
            _push_tag(row)
            if len(tags) >= max_tags:
                break
        if len(tags) >= max_tags:
            break

    subject_signals = [metadata.get("title"), metadata.get("theme"), context.get("artwork_title"), artwork.slug]
    if not any(any(token in tag for token in re.findall(r"[a-z0-9]{4,}", str(signal).lower())) for signal in subject_signals for tag in tags):
        for signal in subject_signals:
            if len(tags) >= max_tags:
                break
            _push_tag(signal)
    theme_candidates = extract_theme_signal_candidates(artwork=artwork, context=context, template=template)
    if not _tags_contain_theme_signal(tags=tags, artwork=artwork, template=template):
        selected_theme = choose_best_theme_signal(candidates=theme_candidates, existing_tags=tags)
        if selected_theme:
            if len(tags) >= max_tags:
                protected = set(content_engine.family_tags(template))
                protected.update(
                    {
                        token
                        for signal in subject_signals
                        for token in [normalize_theme_tag(signal)]
                        if token
                    }
                )
                evictable = [tag for tag in tags if tag not in protected and (tag in TAG_GENERIC_TOKENS or tag in {"inkvibe", "gift", "gift idea"})]
                if evictable:
                    drop = evictable[0]
                    tags = [tag for tag in tags if tag != drop]
                    seen.discard(drop)
            _push_tag(selected_theme, allow_generic=True)

    required = set(content_engine.family_tags(template))
    if required and not required.intersection(set(tags)):
        for tag in content_engine.family_tags(template):
            _push_tag(tag, allow_generic=True)
            if len(tags) >= max_tags:
                break
    if len(tags) < 8:
        for row in optional_bucket:
            _push_tag(row, allow_generic=True)
            if len(tags) >= 8:
                break
    for row in taxonomy_tags:
        if len(tags) >= max_tags:
            break
        _push_tag(row, allow_generic=True)
    return tags


def render_product_description(template: ProductTemplate, artwork: Artwork) -> str:
    context = build_seo_context(template, artwork)
    ai_copy = _maybe_render_ai_product_copy(template, artwork, context)
    if ai_copy and ai_copy.long_description:
        return f"<p>{sanitize_description_text(ai_copy.long_description)}</p>"
    generated = content_engine.build_branded_description(
        artwork_title=context["artwork_title"],
        short_description=str((artwork.metadata or {}).get("description", "")).strip(),
        template=template,
    )
    pattern = (template.description_pattern or "").strip()
    metadata = artwork.metadata or {}
    metadata_description = str(metadata.get("description", "")).strip()

    def _sanitize_html(html: str) -> str:
        cleaned = html
        cleaned = re.sub(r"(?is)<p>\s*(Style notes?|Keywords?)\s*:.*?</p>", "", cleaned)
        cleaned = re.sub(r"(?i)\b(Style notes?|Keywords?)\s*:\s*", "", cleaned)
        cleaned = re.sub(r"(?i)\bMade for general\b", "", cleaned)
        cleaned = re.sub(r"(?i)\ba dynamic scene of a\b", "", cleaned)
        cleaned = re.sub(r"\s{2,}", " ", cleaned)
        return cleaned.strip()

    def _is_sticker_template() -> bool:
        hint = " ".join(
            [
                str(template.key or ""),
                str(template.product_type_label or ""),
                str(template.shopify_product_type or ""),
            ]
        ).lower()
        return "sticker" in hint or "kiss-cut" in hint or "kiss cut" in hint

    def _with_terminal_punctuation(text: str) -> str:
        cleaned_text = sanitize_description_text(text)
        if not cleaned_text:
            return ""
        if cleaned_text[-1] in ".!?":
            return cleaned_text
        return f"{cleaned_text}."

    def _build_sticker_description() -> str:
        artwork_title = str(context.get("artwork_title") or "").strip() or "This design"
        normalized_metadata = _with_terminal_punctuation(metadata_description)
        opener = f"{artwork_title} by InkVibe."

        sentences: List[str] = [opener]
        if normalized_metadata:
            if normalized_metadata.lower() != opener.lower():
                sentences.append(normalized_metadata)
        else:
            sentences.append("A distinctive art-forward sticker design with everyday display appeal.")

        lowered_blob = " ".join(sentences).lower()
        if "gift" not in lowered_blob:
            sentences.append("An easy gift-ready choice for anyone who loves expressive art.")
        if "sticker" not in lowered_blob:
            sentences.append("Made for stickers and everyday display.")

        stitched = " ".join(
            row.strip()
            for row in sentences
            if row and row.strip()
        )
        return _sanitize_html(f"<p>{stitched}</p>")

    if _is_sticker_template():
        return _build_sticker_description()

    if not pattern or pattern in {"{artwork_title}", "<p>{artwork_title}</p>"}:
        if metadata_description:
            return _sanitize_html(generated.strip())
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
        return _sanitize_html(f"{generated}{''.join(details)}".strip())

    return _sanitize_html(template.description_pattern.format(
        **context,
        generated_description=generated,
    ).strip())


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


def _is_catalog_discovery_path(path: str) -> bool:
    normalized = str(path or "").strip().lower()
    return normalized.startswith("/catalog/")


def _cap_catalog_retry_sleep(path: str, requested_backoff: float) -> float:
    if not _is_catalog_discovery_path(path):
        return requested_backoff
    return min(requested_backoff, CATALOG_RETRY_BACKOFF_CAP_SECONDS)


def _retry_policy_bucket(path: str, *, mutating: bool) -> str:
    if _is_catalog_discovery_path(path):
        return "catalog"
    if mutating:
        return "mutation"
    return "default"


def _reason_code_for_retry_limit(path: str, *, mutating: bool) -> str:
    normalized = str(path or "").strip().lower()
    if "/publish.json" in normalized:
        return "publish_rate_limited"
    if mutating:
        return "mutation_rate_limited"
    if _is_catalog_discovery_path(path):
        return "catalog_rate_limited"
    return "request_retry_exhausted"


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
    if variant.get("__sale_price_minor") is not None:
        return int(variant["__sale_price_minor"])
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


def _shipping_minor_for_variant(variant: Dict[str, Any], template: ProductTemplate) -> int:
    def _row_first_item_minor(row: Dict[str, Any]) -> Optional[int]:
        for key in ("first_item", "first_item_price", "first_item_cost", "shipping", "price", "cost"):
            if row.get(key) is not None:
                try:
                    return normalize_printify_price(row.get(key))
                except Exception:
                    return None
        return None

    def _is_us_row(row: Dict[str, Any]) -> bool:
        country = str(row.get("country") or row.get("country_code") or row.get("region") or "").strip().upper()
        if country in {"US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"}:
            return True
        countries = row.get("countries")
        if isinstance(countries, list):
            normalized = {str(v).strip().upper() for v in countries if str(v).strip()}
            if normalized.intersection({"US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"}):
                return True
        return False

    def _extract_rows(payload: Any) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            rows: List[Dict[str, Any]] = []
            for key in ("shipping", "shipping_rows", "rows", "rates", "profiles"):
                value = payload.get(key)
                if isinstance(value, list):
                    rows.extend([row for row in value if isinstance(row, dict)])
            if rows:
                return rows
        return []

    shipping = variant.get("shipping") or variant.get("shipping_cost") or variant.get("shipping_price")
    shipping_rows = _extract_rows(shipping)
    if shipping_rows:
        us_rows = [row for row in shipping_rows if _is_us_row(row)]
        candidate_rows = us_rows or shipping_rows
        first_item_values = [value for value in (_row_first_item_minor(row) for row in candidate_rows) if value is not None]
        if first_item_values:
            return min(first_item_values)
    if shipping is None:
        family = content_engine.infer_product_family(template)
        defaults = {
            "hoodie": 799,
            "sweatshirt": 799,
            "tshirt": 599,
            "longsleeve": 699,
            "mug": 499,
            "poster": 499,
            "tote": 599,
        }
        return defaults.get(family, 599)
    return normalize_printify_price(shipping)


def _variant_margin_after_shipping_minor(template: ProductTemplate, variant: Dict[str, Any], sale_price_minor: int) -> int:
    if variant.get("__printify_cost_minor") is not None:
        cost_minor = int(variant.get("__printify_cost_minor", 0))
    else:
        cost_source = variant.get("cost") if variant.get("cost") is not None else variant.get("price")
        if cost_source is None:
            cost_source = template.base_price if template.base_price is not None else template.default_price
        cost_minor = normalize_printify_price(cost_source)
    return sale_price_minor - cost_minor - _shipping_minor_for_variant(variant, template)


def _variant_price_ceiling_minor(template: ProductTemplate, variant: Dict[str, Any]) -> Optional[int]:
    for key in ("max_allowed_price", "price_ceiling", "max_price", "ceiling_price"):
        value = variant.get(key)
        if value is not None:
            return normalize_printify_price(value)
    if template.max_allowed_price is not None:
        return normalize_printify_price(template.max_allowed_price)
    return None


def apply_variant_margin_guardrails(template: ProductTemplate, variant_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    min_profit_config = template.min_profit_after_shipping if template.min_profit_after_shipping is not None else template.min_margin_after_shipping
    min_margin_minor = int((_decimal_from_value(min_profit_config, default="0") * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    target_margin_minor = int((_decimal_from_value(template.target_margin_after_shipping, default=str(min_profit_config or "0")) * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    adjusted: List[Dict[str, Any]] = []
    disabled: List[int] = []
    repriced: List[int] = []
    failed_variant_reasons: Dict[int, str] = {}
    failure_reason_counts: Dict[str, int] = {}
    variant_diagnostics: List[Dict[str, Any]] = []
    selected_count = len(variant_rows)
    for variant in variant_rows:
        variant_copy = dict(variant)
        sale_minor = compute_sale_price_minor(template, variant_copy)
        original_sale_minor = sale_minor
        margin_before_reprice_minor = _variant_margin_after_shipping_minor(template, variant_copy, sale_minor)
        margin_minor = margin_before_reprice_minor
        cost_source = variant_copy.get("cost") if variant_copy.get("cost") is not None else variant_copy.get("price")
        cost_minor = normalize_printify_price(cost_source if cost_source is not None else template.default_price)
        variant_copy["__printify_cost_minor"] = cost_minor
        shipping_minor = _shipping_minor_for_variant(variant_copy, template)
        ceiling_minor = _variant_price_ceiling_minor(template, variant_copy)
        failure_reason = ""
        if margin_minor < min_margin_minor and template.reprice_variants_to_margin_floor:
            repriced_minor = cost_minor + shipping_minor + target_margin_minor
            if ceiling_minor is not None:
                repriced_minor = min(repriced_minor, ceiling_minor)
            if repriced_minor > sale_minor:
                variant_copy["price"] = repriced_minor
                variant_copy["__sale_price_minor"] = repriced_minor
                repriced.append(int(variant_copy.get("id", 0)))
                sale_minor = repriced_minor
            margin_minor = _variant_margin_after_shipping_minor(template, variant_copy, sale_minor)
            if margin_minor < min_margin_minor and ceiling_minor is not None and sale_minor >= ceiling_minor:
                failure_reason = "required_price_exceeds_max_allowed_price"

        if margin_minor < min_margin_minor and template.disable_variants_below_margin_floor:
            if not failure_reason:
                failure_reason = "margin_below_floor_after_reprice" if template.reprice_variants_to_margin_floor else "margin_below_floor_no_reprice"
            disabled.append(int(variant_copy.get("id", 0)))
            failed_variant_reasons[int(variant_copy.get("id", 0))] = failure_reason
            failure_reason_counts[failure_reason] = failure_reason_counts.get(failure_reason, 0) + 1
            variant_diagnostics.append({
                "variant_id": int(variant_copy.get("id", 0)),
                "original_sale_price_minor": original_sale_minor,
                "repriced_sale_price_minor": sale_minor,
                "printify_cost_minor": cost_minor,
                "shipping_basis_used": template.shipping_basis_for_margin,
                "shipping_minor": shipping_minor,
                "target_margin_after_shipping_minor": target_margin_minor,
                "min_margin_after_shipping_minor": min_margin_minor,
                "after_shipping_margin_before_reprice_minor": margin_before_reprice_minor,
                "after_shipping_margin_after_reprice_minor": margin_minor,
                "max_allowed_price_minor": ceiling_minor or 0,
                "failure_reason": failure_reason,
            })
            continue
        if margin_minor < 0:
            if template.disable_variants_below_margin_floor:
                failure_reason = failure_reason or "negative_margin_after_shipping"
                disabled.append(int(variant_copy.get("id", 0)))
                failed_variant_reasons[int(variant_copy.get("id", 0))] = failure_reason
                failure_reason_counts[failure_reason] = failure_reason_counts.get(failure_reason, 0) + 1
                variant_diagnostics.append({
                    "variant_id": int(variant_copy.get("id", 0)),
                    "original_sale_price_minor": original_sale_minor,
                    "repriced_sale_price_minor": sale_minor,
                    "printify_cost_minor": cost_minor,
                    "shipping_basis_used": template.shipping_basis_for_margin,
                    "shipping_minor": shipping_minor,
                    "target_margin_after_shipping_minor": target_margin_minor,
                    "min_margin_after_shipping_minor": min_margin_minor,
                    "after_shipping_margin_before_reprice_minor": margin_before_reprice_minor,
                    "after_shipping_margin_after_reprice_minor": margin_minor,
                    "max_allowed_price_minor": ceiling_minor or 0,
                    "failure_reason": failure_reason,
                })
                continue
        variant_copy["__sale_price_minor"] = sale_minor
        variant_diagnostics.append({
            "variant_id": int(variant_copy.get("id", 0)),
            "original_sale_price_minor": original_sale_minor,
            "repriced_sale_price_minor": sale_minor,
            "printify_cost_minor": cost_minor,
            "shipping_basis_used": template.shipping_basis_for_margin,
            "shipping_minor": shipping_minor,
            "target_margin_after_shipping_minor": target_margin_minor,
            "min_margin_after_shipping_minor": min_margin_minor,
            "after_shipping_margin_before_reprice_minor": margin_before_reprice_minor,
            "after_shipping_margin_after_reprice_minor": margin_minor,
            "max_allowed_price_minor": ceiling_minor or 0,
            "failure_reason": failure_reason,
        })
        adjusted.append(variant_copy)
    viable = bool(adjusted)
    report = {
        "disabled_variant_ids": disabled,
        "repriced_variant_ids": repriced,
        "viable": viable,
        "selected_count": selected_count,
        "repriced_count": len(repriced),
        "disabled_count_after_reprice": len(disabled),
        "final_enabled_count": len(adjusted),
        "skip_reason": "" if viable else "disabled_by_guardrails_after_reprice",
        "failed_variant_reasons": failed_variant_reasons,
        "failure_reason_counts": failure_reason_counts,
        "variant_diagnostics": variant_diagnostics,
    }
    logger.info(
        "Variant guardrails template=%s selected_count=%s repriced_count=%s disabled_count_after_reprice=%s final_enabled_count=%s min_profit_after_shipping_minor=%s target_profit_after_shipping_minor=%s skip_reason=%s",
        template.key,
        report["selected_count"],
        report["repriced_count"],
        report["disabled_count_after_reprice"],
        report["final_enabled_count"],
        min_margin_minor,
        target_margin_minor,
        report["skip_reason"] or "-",
    )
    if template.key == "tote_basic":
        first_diag = variant_diagnostics[0] if variant_diagnostics else {}
        logger.info(
            "Tote pricing diagnostics template=%s original_sale_minor=%s repriced_sale_minor=%s cost_minor=%s shipping_basis=%s shipping_minor=%s target_margin_minor=%s min_margin_minor=%s margin_before_minor=%s margin_after_minor=%s max_allowed_minor=%s failure_reason=%s",
            template.key,
            int(first_diag.get("original_sale_price_minor", 0)),
            int(first_diag.get("repriced_sale_price_minor", 0)),
            int(first_diag.get("printify_cost_minor", 0)),
            str(first_diag.get("shipping_basis_used", "")) or "-",
            int(first_diag.get("shipping_minor", 0)),
            int(first_diag.get("target_margin_after_shipping_minor", 0)),
            int(first_diag.get("min_margin_after_shipping_minor", 0)),
            int(first_diag.get("after_shipping_margin_before_reprice_minor", 0)),
            int(first_diag.get("after_shipping_margin_after_reprice_minor", 0)),
            int(first_diag.get("max_allowed_price_minor", 0)),
            str(first_diag.get("failure_reason", "")) or "-",
        )
    return adjusted, report


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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_publish_queue(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    queue = state.setdefault("publish_queue", [])
    if not isinstance(queue, list):
        queue = []
        state["publish_queue"] = queue
    return queue


def enqueue_publish_pending(
    *,
    state: Dict[str, Any],
    artwork_key: str,
    template_key: str,
    shop_id: int,
    product_id: str,
    publish_status: str,
    reason_code: str = "",
    last_error: str = "",
    next_eligible_publish_at: str = "",
) -> Dict[str, Any]:
    queue = get_publish_queue(state)
    now = _utc_now_iso()
    for row in queue:
        if not isinstance(row, dict):
            continue
        if row.get("artwork_key") == artwork_key and row.get("template_key") == template_key and str(row.get("shop_id")) == str(shop_id):
            row["product_id"] = product_id
            row["publish_status"] = publish_status
            row["reason_code"] = reason_code
            row["last_error"] = last_error
            row["updated_at"] = now
            if next_eligible_publish_at:
                row["next_eligible_publish_at"] = next_eligible_publish_at
            row.setdefault("created_at", now)
            row.setdefault("publish_attempts", 0)
            return row
    new_row = {
        "artwork_key": artwork_key,
        "template_key": template_key,
        "shop_id": int(shop_id),
        "product_id": product_id,
        "created_at": now,
        "updated_at": now,
        "publish_status": publish_status,
        "publish_attempts": 0,
        "last_error": last_error,
        "reason_code": reason_code,
        "next_eligible_publish_at": next_eligible_publish_at,
    }
    queue.append(new_row)
    return new_row


def summarize_publish_queue(state: Dict[str, Any]) -> Dict[str, int]:
    queue = get_publish_queue(state)
    counts = {"total": 0, "pending": 0, "completed": 0, "failed": 0}
    for row in queue:
        if not isinstance(row, dict):
            continue
        counts["total"] += 1
        status = str(row.get("publish_status") or "").strip().lower()
        if status.startswith("pending"):
            counts["pending"] += 1
        elif status == "completed":
            counts["completed"] += 1
        elif status == "failed":
            counts["failed"] += 1
    return counts


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


def export_preflight_report(path: pathlib.Path, rows: List[TemplatePreflightReportRow]) -> None:
    write_csv_report(path, [row.__dict__ for row in rows])


def _min_profit_minor_for_template(template: ProductTemplate) -> int:
    min_profit_config = template.min_profit_after_shipping if template.min_profit_after_shipping is not None else template.min_margin_after_shipping
    return int((_decimal_from_value(min_profit_config, default="0") * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def run_free_shipping_profit_audit(
    *,
    printify: PrintifyClient,
    templates: List[ProductTemplate],
    free_shipping_min_profit_minor: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for template in templates:
        if not template.active:
            continue
        resolved_template, resolution_diag = _resolve_template_catalog_mapping(
            printify=printify,
            template=template,
            discovery_mode="normal",
        )
        try:
            catalog_variants = printify.list_variants(
                resolved_template.printify_blueprint_id,
                resolved_template.printify_print_provider_id,
            )
        except Exception as exc:
            rows.append(
                {
                    "template_key": template.key,
                    "active": True,
                    "blueprint_id": resolved_template.printify_blueprint_id,
                    "provider_id": resolved_template.printify_print_provider_id,
                    "template_hint_blueprint_id": template.printify_blueprint_id,
                    "template_hint_provider_id": template.printify_print_provider_id,
                    "catalog_discovery_used": resolution_diag.discovery_used,
                    "fallback_discovery_triggered": resolution_diag.fallback_discovery_triggered,
                    "fallback_discovery_reason": resolution_diag.fallback_discovery_reason,
                    "selected_count": 0,
                    "final_enabled_count": 0,
                    "lowest_profit_after_shipping_minor": 0,
                    "lowest_profit_variant_id": 0,
                    "min_profit_after_shipping_minor": _min_profit_minor_for_template(template),
                    "free_shipping_min_profit_minor": free_shipping_min_profit_minor,
                    "meets_guardrail_floor": False,
                    "meets_free_shipping_policy": False,
                    "policy_gap_minor": free_shipping_min_profit_minor,
                    "too_thin_for_free_shipping_policy": True,
                    "status": "catalog_error",
                    "message": str(exc),
                }
            )
            continue

        selected, diagnostics = choose_variants_from_catalog_with_diagnostics(catalog_variants, template)
        guarded, margin_report = apply_variant_margin_guardrails(template, selected)
        variant_diagnostics = margin_report.get("variant_diagnostics", [])
        lowest = min(
            variant_diagnostics,
            key=lambda row: int(row.get("after_shipping_margin_after_reprice_minor", 0)),
            default={},
        )
        lowest_profit_minor = int(lowest.get("after_shipping_margin_after_reprice_minor", 0))
        template_guardrail_min_minor = int(lowest.get("min_margin_after_shipping_minor", _min_profit_minor_for_template(template)))
        meets_guardrail_floor = bool(guarded) and lowest_profit_minor >= template_guardrail_min_minor
        meets_policy = bool(guarded) and lowest_profit_minor >= free_shipping_min_profit_minor
        rows.append(
            {
                "template_key": template.key,
                "active": True,
                "blueprint_id": resolved_template.printify_blueprint_id,
                "provider_id": resolved_template.printify_print_provider_id,
                "template_hint_blueprint_id": template.printify_blueprint_id,
                "template_hint_provider_id": template.printify_print_provider_id,
                "catalog_discovery_used": resolution_diag.discovery_used,
                "fallback_discovery_triggered": resolution_diag.fallback_discovery_triggered,
                "fallback_discovery_reason": resolution_diag.fallback_discovery_reason,
                "selected_count": len(selected),
                "final_enabled_count": len(guarded),
                "option_names": "|".join(diagnostics.option_names),
                "requested_option_filters": json.dumps(diagnostics.requested_option_filters, ensure_ascii=False, sort_keys=True),
                "lowest_profit_after_shipping_minor": lowest_profit_minor,
                "lowest_profit_variant_id": int(lowest.get("variant_id", 0)),
                "min_profit_after_shipping_minor": template_guardrail_min_minor,
                "free_shipping_min_profit_minor": free_shipping_min_profit_minor,
                "meets_guardrail_floor": meets_guardrail_floor,
                "meets_free_shipping_policy": meets_policy,
                "policy_gap_minor": max(0, free_shipping_min_profit_minor - lowest_profit_minor),
                "too_thin_for_free_shipping_policy": bool(guarded) and not meets_policy,
                "status": "ok" if guarded else "nonviable_after_guardrails",
                "message": margin_report.get("skip_reason", ""),
            }
        )
    return rows


def export_free_shipping_profit_audit(
    *,
    csv_path: pathlib.Path,
    json_path: pathlib.Path,
    rows: List[Dict[str, Any]],
) -> None:
    write_csv_report(csv_path, rows)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(rows, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


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
    elif template_key == "tote_basic":
        tote_orientation_scale = {
            "portrait": 0.84,
            "square": 0.82,
            "landscape": 0.79,
        }
        scale = max(scale, tote_orientation_scale.get(orientation, scale))
        logger.info(
            "Tote transform strategy template=%s placement=%s orientation=%s strategy=front_fill_boost_orientation_tuned scale=%.3f",
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
            metadata_keys = {
                "collection_handle",
                "collection_title",
                "collection_description",
                "launch_name",
                "campaign",
                "merch_theme",
                "collection_image_src",
                "collection_sort_order",
                "secondary_collection_handles",
            }
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
                collection_image_src=str(row.get("collection_image_src") or "").strip(),
                collection_sort_order=str(row.get("collection_sort_order") or "").strip(),
                secondary_collection_handles=str(row.get("secondary_collection_handles") or "").strip(),
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
                reason_code=type(exc).__name__,
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
    def __init__(
        self,
        base_url: str,
        headers: Dict[str, str],
        dry_run: bool = False,
        *,
        interactive_retry_policy: bool = True,
        interactive_retry_cap_seconds: float = INTERACTIVE_RETRY_CAP_SECONDS,
        max_retry_sleep_seconds: float = MAX_RETRY_SLEEP_SECONDS,
        catalog_request_spacing_ms: int = 0,
    ):
        self.base_url = base_url.rstrip("/")
        self.dry_run = dry_run
        self.interactive_retry_policy = bool(interactive_retry_policy)
        self.interactive_retry_cap_seconds = max(0.0, float(interactive_retry_cap_seconds))
        self.max_retry_sleep_seconds = max(0.0, float(max_retry_sleep_seconds))
        self.catalog_request_spacing_ms = max(0, int(catalog_request_spacing_ms))
        self.last_catalog_request_at = 0.0
        self.consecutive_catalog_rate_limits = 0
        self.rate_limit_events: Counter[str] = Counter()
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
        interactive_retry_policy = bool(getattr(self, "interactive_retry_policy", True))
        interactive_retry_cap_seconds = max(0.0, float(getattr(self, "interactive_retry_cap_seconds", INTERACTIVE_RETRY_CAP_SECONDS)))
        max_retry_sleep_seconds = max(0.0, float(getattr(self, "max_retry_sleep_seconds", MAX_RETRY_SLEEP_SECONDS)))
        catalog_request_spacing_ms = max(0, int(getattr(self, "catalog_request_spacing_ms", 0) or 0))
        last_catalog_request_at = float(getattr(self, "last_catalog_request_at", 0.0) or 0.0)
        consecutive_catalog_rate_limits = int(getattr(self, "consecutive_catalog_rate_limits", 0) or 0)
        rate_limit_events = getattr(self, "rate_limit_events", None)
        if rate_limit_events is None:
            rate_limit_events = Counter()
            self.rate_limit_events = rate_limit_events

        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                if method.upper() == "GET" and _is_catalog_discovery_path(path) and catalog_request_spacing_ms > 0:
                    elapsed = (time.time() - last_catalog_request_at) * 1000.0
                    if elapsed < catalog_request_spacing_ms:
                        wait_ms = catalog_request_spacing_ms - elapsed
                        jitter_ms = random.uniform(0, max(1.0, catalog_request_spacing_ms * 0.15))
                        time.sleep((wait_ms + jitter_ms) / 1000.0)
                response = self.session.request(method=method.upper(), url=url, params=params, json=payload, timeout=120)
                if method.upper() == "GET" and _is_catalog_discovery_path(path):
                    last_catalog_request_at = time.time()
                    self.last_catalog_request_at = last_catalog_request_at
                if response.status_code in expected_statuses:
                    if _is_catalog_discovery_path(path):
                        consecutive_catalog_rate_limits = 0
                        self.consecutive_catalog_rate_limits = 0
                    return response.json() if response.content else {}

                if response.status_code in {429, 500, 502, 503, 504}:
                    requested_sleep_seconds = _retry_after_seconds(response.headers.get("Retry-After"), attempt)
                    retry_bucket = _retry_policy_bucket(path, mutating=mutating)
                    bucket_cap = (
                        CATALOG_RETRY_BACKOFF_CAP_SECONDS
                        if retry_bucket == "catalog"
                        else MUTATION_RETRY_BACKOFF_CAP_SECONDS if retry_bucket == "mutation" else max_retry_sleep_seconds
                    )
                    applied_cap = min(bucket_cap, max_retry_sleep_seconds)
                    if interactive_retry_policy and retry_bucket == "mutation":
                        applied_cap = min(applied_cap, interactive_retry_cap_seconds)
                    sleep_seconds = min(requested_sleep_seconds, applied_cap)
                    logger.warning(
                        "Request retry endpoint=%s method=%s status=%s retry=%s/%s policy_bucket=%s policy_mode=%s requested=%.2fs capped=%.2fs",
                        path,
                        method.upper(),
                        response.status_code,
                        attempt,
                        RETRY_MAX_ATTEMPTS,
                        retry_bucket,
                        "interactive" if interactive_retry_policy else "automated",
                        requested_sleep_seconds,
                        sleep_seconds,
                    )
                    if response.status_code == 429:
                        endpoint_label = "catalog_other"
                        if "variants.json" in path:
                            endpoint_label = "catalog_variants"
                        elif "print_providers" in path:
                            endpoint_label = "catalog_providers"
                        elif "blueprints.json" in path:
                            endpoint_label = "catalog_blueprints"
                        rate_limit_events[endpoint_label] += 1
                        if _is_catalog_discovery_path(path):
                            consecutive_catalog_rate_limits += 1
                            self.consecutive_catalog_rate_limits = consecutive_catalog_rate_limits
                            sleep_seconds = min(
                                applied_cap,
                                sleep_seconds + min(8.0, consecutive_catalog_rate_limits * 1.25),
                            )
                    if attempt >= RETRY_MAX_ATTEMPTS:
                        raise RetryLimitExceededError(
                            method=method,
                            path=path,
                            policy_bucket=retry_bucket,
                            status_code=response.status_code,
                            attempts=attempt,
                            reason_code=_reason_code_for_retry_limit(path, mutating=mutating),
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
    def __init__(
        self,
        api_token: str,
        dry_run: bool = False,
        *,
        interactive_retry_policy: bool = True,
        interactive_retry_cap_seconds: float = INTERACTIVE_RETRY_CAP_SECONDS,
        max_retry_sleep_seconds: float = MAX_RETRY_SLEEP_SECONDS,
        catalog_request_spacing_ms: int = 0,
        catalog_cache: Optional[CatalogCache] = None,
    ):
        super().__init__(
            base_url=PRINTIFY_API_BASE,
            headers={
                "Authorization": f"Bearer {api_token}",
                "User-Agent": USER_AGENT,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            dry_run=dry_run,
            interactive_retry_policy=interactive_retry_policy,
            interactive_retry_cap_seconds=interactive_retry_cap_seconds,
            max_retry_sleep_seconds=max_retry_sleep_seconds,
            catalog_request_spacing_ms=catalog_request_spacing_ms,
        )
        self.catalog_cache = catalog_cache

    def _catalog_cache_get(self, endpoint: str, *, blueprint_id: int = 0, provider_id: int = 0, extra: str = "") -> Optional[Any]:
        if not self.catalog_cache:
            return None
        key = f"{endpoint}:blueprint={blueprint_id}:provider={provider_id}:extra={extra}"
        return self.catalog_cache.get(key)

    def _catalog_cache_set(self, endpoint: str, value: Any, *, blueprint_id: int = 0, provider_id: int = 0, extra: str = "") -> None:
        if not self.catalog_cache:
            return
        key = f"{endpoint}:blueprint={blueprint_id}:provider={provider_id}:extra={extra}"
        self.catalog_cache.set(key, value)

    def list_shops(self) -> List[Dict[str, Any]]:
        return self.get("/shops.json")

    def list_blueprints(self) -> List[Dict[str, Any]]:
        cached = self._catalog_cache_get("blueprints")
        if cached is not None:
            return cached
        payload = self.get("/catalog/blueprints.json")
        self._catalog_cache_set("blueprints", payload)
        return payload

    def list_print_providers(self, blueprint_id: int) -> List[Dict[str, Any]]:
        cached = self._catalog_cache_get("providers", blueprint_id=blueprint_id)
        if cached is not None:
            return cached
        payload = self.get(f"/catalog/blueprints/{blueprint_id}/print_providers.json")
        self._catalog_cache_set("providers", payload, blueprint_id=blueprint_id)
        return payload

    def list_variants(self, blueprint_id: int, print_provider_id: int, show_out_of_stock: bool = True) -> List[Dict[str, Any]]:
        cache_extra = f"show_oos={1 if show_out_of_stock else 0}"
        cached = self._catalog_cache_get("variants", blueprint_id=blueprint_id, provider_id=print_provider_id, extra=cache_extra)
        if cached is not None:
            return normalize_catalog_variants_response(cached)
        response = self.get(
            f"/catalog/blueprints/{blueprint_id}/print_providers/{print_provider_id}/variants.json",
            **{"show-out-of-stock": 1 if show_out_of_stock else 0},
        )
        normalized = normalize_catalog_variants_response(response)
        self._catalog_cache_set("variants", normalized, blueprint_id=blueprint_id, provider_id=print_provider_id, extra=cache_extra)
        return normalized

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

    def update_custom_collection_merchandising(
        self,
        *,
        collection_id: int,
        sort_order: str = "",
        image_src: str = "",
    ) -> Dict[str, Any]:
        custom_collection: Dict[str, Any] = {"id": collection_id}
        if sort_order.strip():
            custom_collection["sort_order"] = sort_order.strip()
        if image_src.strip():
            custom_collection["image"] = {"src": image_src.strip()}
        if len(custom_collection) == 1:
            return {}
        response = self.put(
            f"/admin/api/{SHOPIFY_API_VERSION}/custom_collections/{collection_id}.json",
            {"custom_collection": custom_collection},
        )
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

    def list_product_collects(self, *, product_id: int) -> List[Dict[str, Any]]:
        return self.list_collects(product_id=product_id, limit=250)

    def delete_collect(self, *, collect_id: int) -> None:
        self.delete(f"/admin/api/{SHOPIFY_API_VERSION}/collects/{collect_id}.json")


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
    secondary_collection_handles: Optional[List[str]] = None,
    enforce_family_collection_membership: bool = False,
    collection_removal_mode: str = "conservative",
    family_collection_handle: str = "",
    allowed_family_collection_handles: Optional[List[str]] = None,
    collection_sort_order: str = "",
    collection_image_src: str = "",
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
        "family_collection_handle": family_collection_handle.strip(),
        "collection_image_source": collection_image_src.strip(),
        "collection_sort_strategy": collection_sort_order.strip(),
        "removed_collection_ids": [],
    }
    normalized_handle = collection_handle.strip()
    normalized_title = collection_title.strip()
    normalized_description = collection_description.strip()
    secondary_handles = [str(v).strip() for v in (secondary_collection_handles or []) if str(v).strip()]

    if not normalized_handle and not normalized_title and not secondary_handles:
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
        target_handles = [resolved_handle, *secondary_handles]
        target_handles = list(dict.fromkeys(h for h in target_handles if h))
        target_collections: List[Tuple[str, str]] = []
        for handle in target_handles:
            title = resolved_title if handle == resolved_handle else handle.replace("-", " ").title()
            target_collections.append((handle, title))
        created_any = False
        target_collection_ids: List[int] = []
        for idx, (target_handle, target_title) in enumerate(target_collections):
            collection = shopify.find_custom_collection(handle=target_handle, title=target_title)
            if not collection:
                try:
                    collection = shopify.create_custom_collection(handle=target_handle, title=target_title, description=normalized_description)
                except DryRunMutationSkipped:
                    result["collection_sync_status"] = "dry-run"
                    return result
                created_any = True
            collection_id = _extract_numeric_shopify_id(collection.get("id")) if isinstance(collection, dict) else None
            if collection_id is None:
                raise RuntimeError(f"Resolved collection is missing id: {collection}")
            target_collection_ids.append(collection_id)
            if idx == 0:
                result["collection_id"] = str(collection_id)
                result["collection_handle"] = str(collection.get("handle") or target_handle)
                result["collection_title"] = str(collection.get("title") or target_title)
            existing_title = str(collection.get("title") or "").strip()
            existing_description = str(collection.get("body_html") or "").strip()
            if (target_title and existing_title != target_title) or (normalized_description and existing_description != normalized_description):
                try:
                    shopify.update_custom_collection(collection_id=collection_id, title=target_title, description=normalized_description)
                except DryRunMutationSkipped:
                    result["collection_sync_status"] = "dry-run"
                    return result
            if not shopify.is_product_in_collection(collection_id=collection_id, product_id=numeric_product_id):
                try:
                    shopify.add_product_to_collection(collection_id=collection_id, product_id=numeric_product_id)
                except DryRunMutationSkipped:
                    result["collection_sync_status"] = "dry-run"
                    return result
            if collection_sort_order.strip() or collection_image_src.strip():
                try:
                    shopify.update_custom_collection_merchandising(
                        collection_id=collection_id,
                        sort_order=collection_sort_order,
                        image_src=collection_image_src,
                    )
                except DryRunMutationSkipped:
                    result["collection_sync_status"] = "dry-run"
                    return result
            if verify_membership:
                result["collection_membership_verified"] = shopify.is_product_in_collection(collection_id=collection_id, product_id=numeric_product_id)

        if enforce_family_collection_membership and not shopify.dry_run:
            target_ids = set(target_collection_ids)
            allowed_handles = {h.casefold() for h in (allowed_family_collection_handles or []) if h}
            for collect in shopify.list_product_collects(product_id=numeric_product_id):
                collect_id = _extract_numeric_shopify_id(collect.get("id"))
                collection_id = _extract_numeric_shopify_id(collect.get("collection_id"))
                if collect_id is None or collection_id is None or collection_id in target_ids:
                    continue
                if collection_removal_mode == "strict":
                    shopify.delete_collect(collect_id=collect_id)
                    result["removed_collection_ids"].append(str(collection_id))
                elif allowed_handles:
                    handle_hint = str(collect.get("collection_handle") or "").strip().casefold()
                    if handle_hint in allowed_handles:
                        shopify.delete_collect(collect_id=collect_id)
                        result["removed_collection_ids"].append(str(collection_id))

        result["collection_sync_status"] = "created" if created_any else "synced"
    except Exception as exc:
        result["collection_sync_status"] = "error"
        result["collection_error"] = str(exc)
        logger.warning("Collection sync failed product_id=%s handle=%s error=%s", numeric_product_id, resolved_handle, exc)
    return result


def _variant_option_value(variant: Dict[str, Any], key: str) -> str:
    options = variant.get("options") or {}
    if isinstance(options, dict):
        direct = str(options.get(key, "")).strip()
        if direct:
            return direct
        canonical_key = _canonical_option_token(key)
        for option_key, option_value in options.items():
            if _canonical_option_token(str(option_key)) == canonical_key:
                return str(option_value or "").strip()
    return str(variant.get(key, "")).strip()


def _canonical_option_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _canonical_phone_model_token(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    raw = re.sub(r"\bplus\b", "+", raw)
    return re.sub(r"[^a-z0-9+]+", "", raw)


def _choose_phone_model_fallback_values(available_values: List[str], *, limit: int = 4) -> List[str]:
    if not available_values:
        return []
    preferred_prefixes = ("iphone", "samsung galaxy")
    preferred = [value for value in available_values if str(value).strip().lower().startswith(preferred_prefixes)]
    pool = preferred or available_values
    return pool[: max(1, min(limit, len(pool)))]


def _extract_size_tokens(value: str) -> List[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    normalized = raw.lower()
    normalized = normalized.replace("″", '"').replace("“", '"').replace("”", '"').replace("′", "'")
    normalized = normalized.replace("×", "x")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    tokens: List[str] = []
    fallback = _canonical_option_token(normalized)
    if fallback:
        tokens.append(fallback)
    match = re.search(r"(\d{1,3}(?:\.\d+)?)\s*(?:\"|in|inch|inches)?\s*x\s*(\d{1,3}(?:\.\d+)?)", normalized)
    if not match:
        return tokens
    width = match.group(1).rstrip("0").rstrip(".")
    height = match.group(2).rstrip("0").rstrip(".")
    orientation_match = re.search(r"\((vertical|horizontal)\)", normalized)
    orientation = orientation_match.group(1) if orientation_match else ""
    base = f"{width}x{height}"
    tokens.append(base)
    if orientation:
        tokens.append(f"{base}:{orientation}")
    return list(dict.fromkeys(tokens))


def _canonical_variant_options(variant: Dict[str, Any]) -> Dict[str, str]:
    options = variant.get("options") or {}
    normalized: Dict[str, str] = {}
    if isinstance(options, dict):
        source_items = options.items()
    else:
        source_items = []
    for key, value in source_items:
        key_name = str(key or "").strip()
        if not key_name:
            continue
        canonical = _canonical_option_token(key_name)
        if not canonical:
            continue
        normalized[canonical] = str(value or "").strip()
    return normalized


def _analyze_variant_filtering(catalog_variants: List[Dict[str, Any]], template: ProductTemplate) -> Tuple[List[Dict[str, Any]], VariantFilterDiagnostics]:
    alias_hints = {
        "color": ["color", "colour", "casecolor", "finish", "surface"],
        "size": ["size", "sizes", "dimension", "dimensions", "format"],
        "model": ["model", "device", "phonemodel", "devicemodel", "compatibility", "size"],
    }
    schema_names: Dict[str, str] = {}
    schema_values: Dict[str, Counter[str]] = {}
    canonical_rows: List[Tuple[Dict[str, Any], Dict[str, str]]] = []
    for variant in catalog_variants:
        canonical_options = _canonical_variant_options(variant)
        canonical_rows.append((variant, canonical_options))
        for canonical_key, value in canonical_options.items():
            schema_names.setdefault(canonical_key, str(next((k for k, v in (variant.get("options") or {}).items() if _canonical_option_token(str(k)) == canonical_key), canonical_key)))
            schema_values.setdefault(canonical_key, Counter())[str(value or "").strip()] += 1

    effective_filters: Dict[str, List[str]] = {}
    if template.enabled_colors or template.expanded_enabled_colors:
        base_colors = [str(v).strip() for v in template.enabled_colors if str(v).strip()]
        expanded_colors = [str(v).strip() for v in template.expanded_enabled_colors if str(v).strip()]
        effective_filters["color"] = list(dict.fromkeys([*base_colors, *expanded_colors]))
    if template.enabled_sizes:
        effective_filters["size"] = [str(v).strip() for v in template.enabled_sizes if str(v).strip()]
    for key, values in (template.enabled_variant_option_filters or {}).items():
        vals = [str(v).strip() for v in values if str(v).strip()]
        if vals:
            effective_filters[str(key).strip()] = vals

    diagnostics = VariantFilterDiagnostics(
        option_names=sorted({schema_names[k] for k in schema_names}),
        option_values_summary={
            schema_names.get(key, key): [item[0] for item in schema_values.get(key, Counter()).most_common(12)]
            for key in sorted(schema_values)
        },
        requested_option_filters={k: list(v) for k, v in effective_filters.items()},
    )

    def _resolve_schema_key(requested_key: str) -> Optional[str]:
        canonical_requested = _canonical_option_token(requested_key)
        if canonical_requested in schema_names:
            return canonical_requested
        for hint in alias_hints.get(canonical_requested, []):
            canonical_hint = _canonical_option_token(hint)
            if canonical_hint in schema_names:
                return canonical_hint
        for canonical_key in schema_names:
            if canonical_requested and (canonical_requested in canonical_key or canonical_key in canonical_requested):
                return canonical_key
        return None

    filtered_rows = list(canonical_rows)
    before_available_count = sum(1 for variant, _ in canonical_rows if variant.get("is_available", True))
    diagnostics.filter_counts.append({"dimension": "__available__", "before": len(canonical_rows), "after": before_available_count, "matched_values": []})
    filtered_rows = [(variant, opts) for variant, opts in filtered_rows if variant.get("is_available", True)]

    for requested_key, requested_values in effective_filters.items():
        before_count = len(filtered_rows)
        resolved_schema_key = _resolve_schema_key(requested_key)
        if not resolved_schema_key:
            canonical_requested = _canonical_option_token(requested_key)
            if canonical_requested in {"color", "size"}:
                logger.warning(
                    "Template %s specifies %s, but blueprint %s/provider %s exposes no matching option dimension; ignoring %s filter.",
                    template.key,
                    f"enabled_{canonical_requested}s",
                    template.printify_blueprint_id,
                    template.printify_print_provider_id,
                    canonical_requested,
                )
                diagnostics.filter_counts.append(
                    {
                        "dimension": requested_key,
                        "resolved_dimension": "",
                        "before": before_count,
                        "after": before_count,
                        "matched_values": [],
                        "missing_dimension_ignored": True,
                    }
                )
                continue
            diagnostics.filter_counts.append(
                {"dimension": requested_key, "resolved_dimension": "", "before": before_count, "after": 0, "matched_values": [], "missing_dimension": True}
            )
            diagnostics.zero_selection_reason = (
                f"Requested filter dimension '{requested_key}' not present in provider schema."
            )
            filtered_rows = []
            break

        available_values = [
            str(value).strip()
            for value in schema_values.get(resolved_schema_key, Counter()).keys()
            if str(value).strip()
        ]
        available_value_map = {_canonical_option_token(value): value for value in available_values}
        if _canonical_option_token(requested_key) == "size":
            for value in available_values:
                for token in _extract_size_tokens(value):
                    available_value_map.setdefault(token, value)
        if _canonical_option_token(requested_key) == "model":
            diagnostics.resolved_model_dimension = schema_names.get(resolved_schema_key, resolved_schema_key)
            for value in available_values:
                phone_token = _canonical_phone_model_token(value)
                if phone_token and phone_token not in available_value_map:
                    available_value_map[phone_token] = value
        resolved_values: List[str] = []
        missing_values: List[str] = []
        for requested_value in requested_values:
            canonical_value = _canonical_option_token(requested_value)
            candidate_tokens = [canonical_value]
            if _canonical_option_token(requested_key) == "size":
                candidate_tokens = _extract_size_tokens(requested_value) or [canonical_value]
            matched_value = next((available_value_map.get(token) for token in candidate_tokens if token), None)
            if matched_value is None and _canonical_option_token(requested_key) == "model":
                matched_value = available_value_map.get(_canonical_phone_model_token(requested_value))
            if matched_value:
                resolved_values.append(matched_value)
            else:
                missing_values.append(requested_value)
        if _canonical_option_token(requested_key) == "model":
            diagnostics.requested_model_overlap_count = len(set(resolved_values))
            if not resolved_values and _template_intended_family(template) == "phone_case":
                resolved_values = _choose_phone_model_fallback_values(available_values, limit=4)
                diagnostics.fallback_model_set_applied = bool(resolved_values)
                missing_values = []
            diagnostics.final_selected_models = sorted(set(resolved_values))
        if _canonical_option_token(requested_key) == "color" and template.expanded_enabled_colors:
            base_tokens = {_canonical_option_token(v) for v in template.enabled_colors}
            expanded_tokens = {_canonical_option_token(v) for v in template.expanded_enabled_colors}
            resolved_tokens = {_canonical_option_token(v) for v in resolved_values}
            missing_tokens = {_canonical_option_token(v) for v in missing_values}
            diagnostics.selected_additional_colors = sorted(
                {v for v in resolved_values if _canonical_option_token(v) in (expanded_tokens - base_tokens)}
            )
            diagnostics.unavailable_additional_colors = sorted(
                {v for v in template.expanded_enabled_colors if _canonical_option_token(v) in missing_tokens}
            )
        resolved_set = set(resolved_values)
        filtered_rows = [(variant, opts) for variant, opts in filtered_rows if str(opts.get(resolved_schema_key, "")).strip() in resolved_set]
        diagnostics.filter_counts.append(
            {
                "dimension": requested_key,
                "resolved_dimension": schema_names.get(resolved_schema_key, resolved_schema_key),
                "before": before_count,
                "after": len(filtered_rows),
                "matched_values": sorted(resolved_set),
                "missing_values": missing_values,
            }
        )
        if before_count > 0 and len(filtered_rows) == 0 and not diagnostics.zero_selection_reason:
            if missing_values and not resolved_set:
                diagnostics.zero_selection_reason = (
                    f"No requested values for '{requested_key}' matched provider values."
                )
            else:
                diagnostics.zero_selection_reason = (
                    f"Resolved values for '{requested_key}' produced zero intersections across dimensions."
                )

    if not filtered_rows and not diagnostics.zero_selection_reason:
        diagnostics.zero_selection_reason = "No available variants remained after filters."

    return [variant for variant, _ in filtered_rows], diagnostics


def _classify_zero_selection(template: ProductTemplate, diagnostics: VariantFilterDiagnostics, *, intended_family: str = "") -> str:
    family = intended_family or _template_intended_family(template)
    if family != "canvas":
        return "zero_variants_selected"
    for row in diagnostics.filter_counts:
        dimension = _canonical_option_token(str(row.get("dimension", "")))
        if dimension != "size":
            continue
        before = int(row.get("before", 0) or 0)
        after = int(row.get("after", 0) or 0)
        matched = row.get("matched_values") or []
        missing = row.get("missing_values") or []
        if before > 0 and after == 0 and missing and not matched:
            return "canvas_size_filter_mismatch"
    return "zero_variants_selected"


def _shopify_money_string_from_minor(minor_units: int) -> str:
    return f"{Decimal(minor_units) / Decimal('100'):.2f}"


def _qa_option_dimension_labels(template: ProductTemplate, variant_rows: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    family = _template_intended_family(template)
    preferred_dimensions: List[Tuple[str, str]]
    if family == "sticker":
        preferred_dimensions = [("shape", "Shape"), ("size", "Size"), ("quantity", "Quantity")]
    elif family == "phone_case":
        preferred_dimensions = [("model", "Model"), ("surface", "Finish"), ("size", "Size"), ("color", "Color")]
    elif family in {"tumbler", "travel_mug"}:
        preferred_dimensions = [("size", "Size"), ("capacity", "Capacity"), ("color", "Color"), ("finish", "Finish")]
    else:
        preferred_dimensions = [("color", "Color"), ("size", "Size")]

    dimensions: List[Tuple[str, str]] = []
    for key, label in preferred_dimensions:
        if any(_variant_option_value(variant, key) for variant in variant_rows):
            dimensions.append((key, label))
    return dimensions


def build_shopify_product_options(template: ProductTemplate, variant_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    selected_dimensions = _qa_option_dimension_labels(template, variant_rows)
    product_options: List[Dict[str, Any]] = []
    for option_key, option_label in selected_dimensions:
        values = sorted({_variant_option_value(variant, option_key) for variant in variant_rows if _variant_option_value(variant, option_key)})
        if values:
            product_options.append({"name": option_label, "values": [{"name": value} for value in values]})
    if not product_options:
        product_options.append({"name": "Title", "values": [{"name": "Default Title"}]})

    variants: List[Dict[str, Any]] = []
    for variant in variant_rows:
        option_values: List[Dict[str, str]] = []
        for option_key, option_label in selected_dimensions:
            option_value = _variant_option_value(variant, option_key)
            if option_value:
                option_values.append({"optionName": option_label, "name": option_value})
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
    organization = build_normalized_shopify_organization(template, artwork)
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


def _apply_inline_metadata_generation(
    *,
    artwork: Artwork,
    metadata_source: str,
    metadata_match_key: str,
    auto_generate_missing_metadata: bool,
    metadata_inline_only_when_weak: bool,
    auto_write_generated_sidecars: bool,
    metadata_inline_overwrite_weak_sidecars: bool,
    metadata_inline_generator: str,
    metadata_openai_model: str,
    metadata_openai_timeout: float,
) -> Artwork:
    artwork.metadata_resolution_source = metadata_source
    artwork.metadata = sanitize_metadata_for_publish(artwork.metadata or {})
    should_generate, weak_reasons = metadata_is_missing_or_weak(
        artwork.metadata,
        artwork_path=artwork.src_path,
        metadata_source=metadata_source,
        only_when_weak=metadata_inline_only_when_weak,
    )
    artwork.weak_metadata_detected = weak_reasons
    if weak_reasons:
        logger.info(
            "Weak metadata detected artwork=%s source=%s reasons=%s",
            artwork.src_path.name,
            metadata_source,
            ",".join(weak_reasons),
        )
    if not auto_generate_missing_metadata or not should_generate:
        return artwork

    logger.info(
        "Inline metadata generation requested artwork=%s source=%s key=%s reasons=%s generator=%s",
        artwork.src_path.name,
        metadata_source,
        metadata_match_key,
        ",".join(weak_reasons) or "metadata_missing",
        metadata_inline_generator,
    )
    try:
        generator = select_artwork_metadata_generator(
            mode=metadata_inline_generator,
            openai_model=metadata_openai_model,
            openai_timeout_seconds=metadata_openai_timeout,
        )
        candidate = generator.generate_metadata_for_artwork(artwork.src_path)
    except Exception as exc:
        logger.warning("Inline metadata generation failed artwork=%s error=%s", artwork.src_path.name, exc)
        return artwork
    generated_payload = sanitize_metadata_for_publish(candidate.metadata.as_sidecar_dict())
    artwork.metadata = generated_payload
    artwork.title = str(generated_payload.get("title") or artwork.title).strip() or artwork.title
    artwork.metadata_generated_inline = True
    inline_resolution_source = "inline_heuristic"
    if "openai" in candidate.generator:
        inline_resolution_source = "inline_openai"
    elif "vision" in candidate.generator:
        inline_resolution_source = "inline_vision"
    artwork.metadata_resolution_source = inline_resolution_source

    wrote_sidecar = False
    sidecar_reason = "auto_write_disabled"
    if auto_write_generated_sidecars:
        wrote_sidecar, sidecar_reason = persist_inline_metadata_sidecar(
            artwork=artwork,
            candidate_metadata=generated_payload,
            generator_name=candidate.generator,
            weak_reasons=weak_reasons,
            metadata_source=metadata_source,
            inline_resolution_source=inline_resolution_source,
        )
    artwork.metadata_sidecar_written = wrote_sidecar
    logger.info(
        "Inline metadata generated artwork=%s generator=%s title=%s sidecar_written=%s reason=%s",
        artwork.src_path.name,
        candidate.generator,
        artwork.title,
        wrote_sidecar,
        sidecar_reason,
    )
    return artwork


def discover_artworks(
    image_dir: pathlib.Path,
    *,
    candidate_paths: Optional[List[pathlib.Path]] = None,
    source_hygiene: Optional[SourceHygieneOptions] = None,
    auto_generate_missing_metadata: bool = True,
    auto_write_generated_sidecars: bool = True,
    metadata_inline_generator: str = MetadataGeneratorMode.AUTO.value,
    metadata_inline_only_when_weak: bool = True,
    metadata_inline_overwrite_weak_sidecars: bool = False,
    metadata_openai_model: str = "",
    metadata_openai_timeout: float = 30.0,
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
            "Content metadata match artwork=%s source=%s key=%s reason=%s weak_fallback_reason=%s stronger_source_won_over=%s",
            path.name,
            match_info.get("source", "unknown"),
            match_info.get("key", ""),
            match_info.get("reason", ""),
            match_info.get("weak_fallback_reason", ""),
            match_info.get("stronger_source_won_over", ""),
        )
        title = str(metadata.get("title", "")).strip() or filename_slug_to_title(path.stem)
        artwork = Artwork(
            slug=slug,
            src_path=path,
            title=title,
            description_html=f"<p>{title}</p>",
            tags=[],
            image_width=width,
            image_height=height,
            metadata=sanitize_metadata_for_publish(metadata),
        )
        artwork = _apply_inline_metadata_generation(
            artwork=artwork,
            metadata_source=match_info.get("source", "fallback"),
            metadata_match_key=match_info.get("key", ""),
            auto_generate_missing_metadata=auto_generate_missing_metadata,
            metadata_inline_only_when_weak=metadata_inline_only_when_weak,
            auto_write_generated_sidecars=auto_write_generated_sidecars,
            metadata_inline_overwrite_weak_sidecars=metadata_inline_overwrite_weak_sidecars,
            metadata_inline_generator=metadata_inline_generator,
            metadata_openai_model=metadata_openai_model,
            metadata_openai_timeout=metadata_openai_timeout,
        )
        artworks.append(artwork)

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


def _template_routing_family(
    template: ProductTemplate,
    *,
    template_family_map: Dict[str, str],
    mug_tote_master: str,
) -> str:
    explicit_family = str(template.artwork_routing_family or "").strip().lower()
    if explicit_family:
        return explicit_family
    return template_family_map.get(template.key) or classify_template_family(
        template.key,
        mug_tote_master=mug_tote_master,
    )


def _collect_prompt_family_target_minimums(
    *,
    templates: List[ProductTemplate],
    template_family_map: Dict[str, str],
    family_aware: bool,
    mug_tote_master: str,
) -> Dict[str, Tuple[int, int]]:
    target_by_family: Dict[str, Tuple[int, int]] = {}
    for template in templates:
        if family_aware:
            family = _template_routing_family(
                template,
                template_family_map=template_family_map,
                mug_tote_master=mug_tote_master,
            )
        else:
            family = "single"
        placement_width = max([int(p.width_px) for p in template.placements] or [0])
        placement_height = max([int(p.height_px) for p in template.placements] or [0])
        existing = target_by_family.get(family, (0, 0))
        target_by_family[family] = (
            max(existing[0], placement_width),
            max(existing[1], placement_height),
        )
    return target_by_family


def _resolve_split_routing_family_map(
    *,
    templates: List[ProductTemplate],
    base_family_map: Dict[str, str],
    family_aware: bool,
    family_mode: str,
    mug_tote_master: str,
) -> Dict[str, str]:
    resolved = dict(base_family_map or {})
    forced_poster_templates = {"canvas_basic", "blanket_basic"}
    if not family_aware or (family_mode or "").strip().lower() != "split":
        return resolved
    for template in templates:
        key = template.key
        if key in forced_poster_templates:
            resolved[key] = POSTER_FAMILY
            continue
        template_family = _template_routing_family(
            template,
            template_family_map=resolved,
            mug_tote_master=mug_tote_master,
        )
        min_cover_ratio = float(template.min_effective_cover_ratio or 0.0)
        requires_high_res_cover = bool(template.high_resolution_family) or min_cover_ratio >= 1.0
        if requires_high_res_cover and template_family in {APPAREL_FAMILY, BLANKET_FAMILY, POSTER_FAMILY}:
            resolved[key] = POSTER_FAMILY
    return resolved


def _build_blanket_safe_derived_asset(
    *,
    asset: GeneratedArtworkAsset,
    target_width: int,
    target_height: int,
) -> Tuple[GeneratedArtworkAsset, str]:
    blanket_safe_margin_ratio = 0.08
    with Image.open(asset.path) as opened:
        source = ImageOps.exif_transpose(opened).convert("RGBA")
        src_w, src_h = source.size
        target_w = max(1, int(target_width))
        target_h = max(1, int(target_height))
        target_ratio = target_w / target_h
        source_ratio = (src_w / src_h) if src_h > 0 else target_ratio
        ratio_delta = abs(source_ratio - target_ratio) / target_ratio if target_ratio > 0 else 0.0
        if ratio_delta <= 0.08:
            composition_mode = "subject_safe_cover_crop"
            scale = max(target_w / src_w, target_h / src_h) if src_w > 0 and src_h > 0 else 1.0
            resized_w = max(1, int(round(src_w * scale)))
            resized_h = max(1, int(round(src_h * scale)))
            resized = source.resize((resized_w, resized_h), _upscale_filter("lanczos"))
            left = max(0, (resized_w - target_w) // 2)
            top = max(0, (resized_h - target_h) // 2)
            derived_image = resized.crop((left, top, left + target_w, top + target_h))
            resized.close()
        else:
            composition_mode = "subject_safe_contain_padded"
            safe_w = max(1, int(round(target_w * (1.0 - blanket_safe_margin_ratio * 2.0))))
            safe_h = max(1, int(round(target_h * (1.0 - blanket_safe_margin_ratio * 2.0))))
            requested_scale = min(
                (safe_w / src_w) if src_w > 0 else 1.0,
                (safe_h / src_h) if src_h > 0 else 1.0,
            )
            applied_scale = min(max(0.01, requested_scale), 2.0)
            resized_w = max(1, int(round(src_w * applied_scale)))
            resized_h = max(1, int(round(src_h * applied_scale)))
            resized = source.resize((resized_w, resized_h), _upscale_filter("lanczos"))
            derived_image = Image.new("RGBA", (target_w, target_h), (0, 0, 0, 0))
            offset_x = max(0, (target_w - resized_w) // 2)
            offset_y = max(0, (target_h - resized_h) // 2)
            derived_image.alpha_composite(resized, (offset_x, offset_y))
            resized.close()
    derived_path = asset.path.with_name(f"{asset.path.stem}-blanket-safe-derived.png")
    derived_path.parent.mkdir(parents=True, exist_ok=True)
    derived_w, derived_h = derived_image.size
    derived_image.save(derived_path)
    derived_image.close()
    source.close()
    logger.info(
        "Prompt blanket-safe derived asset family=%s concept=%s source=%sx%s target=%sx%s mode=%s path=%s",
        asset.family or "single",
        asset.concept_index,
        src_w,
        src_h,
        target_w,
        target_h,
        composition_mode,
        derived_path,
    )
    return (
        replace(
            asset,
            path=derived_path,
            width=derived_w,
            height=derived_h,
        ),
        composition_mode,
    )


def _build_prompt_derived_masters(
    *,
    assets: List[GeneratedArtworkAsset],
    family_targets: Dict[str, Tuple[int, int]],
    request: ArtworkGenerationRequest,
) -> List[GeneratedArtworkAsset]:
    derived_assets: List[GeneratedArtworkAsset] = []
    blanket_safe_margin_ratio = 0.08
    for asset in assets:
        with Image.open(asset.path) as opened:
            source = ImageOps.exif_transpose(opened).convert("RGBA")
            src_w, src_h = source.size
            family = asset.family or "single"
            target_w, target_h = family_targets.get(family, (0, 0))
            composition_mode = "cover_fill_upscale"
            if family == BLANKET_FAMILY and target_w > 0 and target_h > 0:
                composition_mode = "contain_padded_blanket_safe"
                canvas_w = max(1, int(target_w))
                canvas_h = max(1, int(target_h))
                safe_w = max(1, int(round(canvas_w * (1.0 - blanket_safe_margin_ratio * 2.0))))
                safe_h = max(1, int(round(canvas_h * (1.0 - blanket_safe_margin_ratio * 2.0))))
                requested_scale = min(
                    (safe_w / src_w) if src_w > 0 else 1.0,
                    (safe_h / src_h) if src_h > 0 else 1.0,
                )
                upscale_cap = 2.0
                applied_scale = min(max(0.01, requested_scale), upscale_cap)
                resized_w = max(1, int(round(src_w * applied_scale)))
                resized_h = max(1, int(round(src_h * applied_scale)))
                if resized_w == src_w and resized_h == src_h:
                    resized = source.copy()
                    upscaled = False
                else:
                    resized = source.resize((resized_w, resized_h), _upscale_filter("lanczos"))
                    upscaled = True
                derived_image = Image.new("RGBA", (canvas_w, canvas_h), (0, 0, 0, 0))
                offset_x = max(0, (canvas_w - resized_w) // 2)
                offset_y = max(0, (canvas_h - resized_h) // 2)
                derived_image.alpha_composite(resized, (offset_x, offset_y))
                resized.close()
            else:
                requested_scale = max(
                    (target_w / src_w) if src_w > 0 and target_w > 0 else 1.0,
                    (target_h / src_h) if src_h > 0 and target_h > 0 else 1.0,
                )
                upscale_cap = WALLART_AUTO_MASTER_MAX_UPSCALE_FACTOR if family in {"poster", "single"} else 2.0
                applied_scale = min(max(1.0, requested_scale), upscale_cap)
                resized_w = max(1, int(round(src_w * applied_scale)))
                resized_h = max(1, int(round(src_h * applied_scale)))
                if resized_w == src_w and resized_h == src_h:
                    derived_image = source.copy()
                    upscaled = False
                else:
                    derived_image = source.resize((resized_w, resized_h), _upscale_filter("lanczos"))
                    upscaled = True

        derived_suffix = "derived"
        if family and family != "single":
            derived_suffix = f"{family}-derived"
        derived_path = asset.path.with_name(f"{asset.path.stem}-{derived_suffix}.png")
        derived_path.parent.mkdir(parents=True, exist_ok=True)
        derived_w, derived_h = derived_image.size
        derived_image.save(derived_path)
        derived_image.close()
        source.close()

        logger.info(
            "Prompt derived master family=%s concept=%s raw_path=%s raw_dims=%sx%s derived_path=%s derived_dims=%sx%s upscale_applied=%s requested_scale=%.3f applied_scale=%.3f target_min=%sx%s",
            family,
            asset.concept_index,
            asset.path,
            src_w,
            src_h,
            derived_path,
            derived_w,
            derived_h,
            str(upscaled).lower(),
            requested_scale,
            applied_scale,
            target_w,
            target_h,
        )
        if family == BLANKET_FAMILY:
            logger.info(
                "Prompt blanket-safe composition family=%s concept=%s mode=%s padding_applied=%s margin_ratio=%.3f source=%sx%s derived=%sx%s",
                family,
                asset.concept_index,
                composition_mode,
                "true",
                blanket_safe_margin_ratio,
                src_w,
                src_h,
                derived_w,
                derived_h,
            )
        derived_assets.append(
            replace(
                asset,
                path=derived_path,
                width=derived_w,
                height=derived_h,
            )
        )
    return derived_assets


def run_prompt_artwork_generation(
    *,
    request: ArtworkGenerationRequest,
    templates: List[ProductTemplate],
) -> PromptArtworkGenerationResult:
    template_keys = [template.key for template in templates]
    strict_family_templates = {
        template.key
        for template in templates
        if str(template.artwork_routing_family or "").strip()
    }
    logger.info("Prompt-art active templates loaded count=%s templates=%s", len(template_keys), ",".join(template_keys) or "-")
    if request.family_aware:
        plan = plan_family_artwork_targets(
            template_keys=template_keys,
            family_mode=request.family_mode,
            generate_poster_master=request.generate_poster_master,
            generate_apparel_master=request.generate_apparel_master,
            mug_tote_master=request.mug_tote_master,
            openai_size=request.openai_size,
            openai_portrait_size=request.openai_portrait_size,
            openai_square_size=request.openai_square_size,
            openai_landscape_size=request.openai_landscape_size,
        )
    else:
        plan = plan_generated_artwork_targets(
            template_keys=template_keys,
            target_mode=request.target_mode,
            openai_size=request.openai_size,
            openai_portrait_size=request.openai_portrait_size,
            openai_square_size=request.openai_square_size,
            openai_landscape_size=request.openai_landscape_size,
        )
    for reason in plan.rationale:
        logger.info("Artwork generation plan: %s", reason)
    explicit_family_overrides = {
        template.key: str(template.artwork_routing_family or "").strip().lower()
        for template in templates
        if str(template.artwork_routing_family or "").strip()
    }
    if explicit_family_overrides:
        merged_map = dict(plan.template_family_map or {})
        merged_map.update(explicit_family_overrides)
        plan.template_family_map = merged_map
        logger.info("Prompt-art explicit routing family overrides=%s", json.dumps(explicit_family_overrides, sort_keys=True))
    if plan.template_family_map:
        logger.info("Prompt-art family classification map=%s", json.dumps(plan.template_family_map, sort_keys=True))
    routing_family_map = _resolve_split_routing_family_map(
        templates=templates,
        base_family_map=plan.template_family_map,
        family_aware=plan.family_aware,
        family_mode=request.family_mode,
        mug_tote_master=request.mug_tote_master,
    )
    if routing_family_map and routing_family_map != plan.template_family_map:
        logger.info("Prompt-art split routing family override map=%s", json.dumps(routing_family_map, sort_keys=True))
    for target in plan.targets:
        logger.info("Artwork generation target family=%s mode=%s openai_size=%s", target.family, target.mode, target.openai_size)

    if request.dry_run_plan:
        return PromptArtworkGenerationResult()

    try:
        generated_assets = generate_artwork_with_openai(request=request, plan=plan)
    except Exception as exc:
        raise RuntimeError(f"Prompt artwork generation failed cleanly: {exc}") from None
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
    family_targets = _collect_prompt_family_target_minimums(
        templates=templates,
        template_family_map=routing_family_map,
        family_aware=plan.family_aware,
        mug_tote_master=request.mug_tote_master,
    )
    preferred_assets = _build_prompt_derived_masters(
        assets=preferred_assets,
        family_targets=family_targets,
        request=request,
    )
    kept_paths = [asset.path for asset in preferred_assets]
    dropped_paths = sorted({asset.path for asset in hydrated_assets} - set(kept_paths))
    for dropped in dropped_paths:
        logger.info("Skipping generated source image=%s reason=duplicate_concept_preferred_master_selected", dropped.name)
    routing: List[TemplateAssetRouting] = []
    if request.family_aware and preferred_assets:
        routing = route_templates_to_generated_assets(
            template_keys=template_keys,
            assets=preferred_assets,
            template_family_map=routing_family_map,
            mug_tote_master=request.mug_tote_master,
            strict_family_templates=sorted(strict_family_templates),
        )
        if strict_family_templates:
            concepts = sorted({int(asset.concept_index) for asset in preferred_assets}) or [1]
            routed_pairs = {(row.template_key, int(row.concept_index)) for row in routing}
            missing_pairs = [
                f"{template_key}@c{concept_index:02d}"
                for template_key in sorted(strict_family_templates)
                for concept_index in concepts
                if (template_key, concept_index) not in routed_pairs
            ]
            if missing_pairs:
                raise RuntimeError(
                    "Prompt-art strict family routing missing required family master for templates: "
                    + ", ".join(missing_pairs)
                )
        blanket_templates = {
            template.key: template
            for template in templates
            if "blanket" in template.key.lower() or "throw" in template.key.lower()
        }
        blanket_derived_assets: Dict[Tuple[pathlib.Path, int, int], GeneratedArtworkAsset] = {}
        blanket_composition_by_template: Dict[str, str] = {}
        updated_routing: List[TemplateAssetRouting] = []
        for row in routing:
            blanket_template = blanket_templates.get(row.template_key)
            if blanket_template is None:
                updated_routing.append(row)
                continue
            target_w = max([int(p.width_px) for p in blanket_template.placements] or [6000])
            target_h = max([int(p.height_px) for p in blanket_template.placements] or [4800])
            cache_key = (row.asset_path, target_w, target_h)
            derived_asset = blanket_derived_assets.get(cache_key)
            if derived_asset is None:
                source_asset = next((asset for asset in preferred_assets if asset.path == row.asset_path), None)
                if source_asset is None:
                    updated_routing.append(row)
                    continue
                derived_asset, composition_mode = _build_blanket_safe_derived_asset(
                    asset=source_asset,
                    target_width=target_w,
                    target_height=target_h,
                )
                blanket_derived_assets[cache_key] = derived_asset
                blanket_composition_by_template[row.template_key] = composition_mode
            updated_routing.append(
                replace(
                    row,
                    asset_path=derived_asset.path,
                )
            )
        routing = updated_routing
        for derived in blanket_derived_assets.values():
            kept_paths.append(derived.path)
        logger.info("Family routing planned mappings=%s", len(routing))
        for row in routing:
            logger.info(
                "Template routing template=%s family=%s concept=%s asset=%s asset_family=%s strategy=%s reason=%s",
                row.template_key,
                row.family,
                row.concept_index,
                row.asset_path.name,
                row.asset_family or "-",
                row.routing_strategy,
                row.routing_reason,
            )
            if row.template_key in blanket_composition_by_template:
                logger.info(
                    "Template routing blanket composition template=%s concept=%s mode=%s",
                    row.template_key,
                    row.concept_index,
                    blanket_composition_by_template[row.template_key],
                )
    if request.family_aware:
        covered_template_keys = {row.template_key for row in routing}
        for key in template_keys:
            if key not in covered_template_keys:
                logger.warning(
                    "Prompt-art template excluded from routing template=%s reason=no_generated_asset_for_family family=%s",
                    key,
                    plan.template_family_map.get(key) or "unknown",
                )
    return PromptArtworkGenerationResult(generated_paths=kept_paths, template_routing=routing)


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


def _resolve_template_placements(template: ProductTemplate, *, for_publish: bool = False) -> List[PlacementRequirement]:
    placements = list(template.placements)
    if not placements:
        return placements
    active = [name.strip().lower() for name in (template.active_placements or []) if str(name).strip()]
    if active:
        active_set = set(active)
        placements = [placement for placement in placements if placement.placement_name.lower() in active_set]
    primary = (template.preferred_primary_placement or "").strip().lower()
    if primary:
        placements = sorted(placements, key=lambda placement: 0 if placement.placement_name.lower() == primary else 1)
    if for_publish and template.publish_only_primary_placement and primary:
        placements = [placement for placement in placements if placement.placement_name.lower() == primary]
    return placements


def evaluate_artwork_eligibility_for_template(
    *,
    artwork: Artwork,
    template: ProductTemplate,
    placement: PlacementRequirement,
) -> ArtworkEligibilityResult:
    source_w = int(artwork.image_width or 0)
    source_h = int(artwork.image_height or 0)
    required_w = int(placement.width_px or 0)
    required_h = int(placement.height_px or 0)
    fit_mode = str(placement.artwork_fit_mode or "contain").strip().lower() or "contain"
    result = ArtworkEligibilityResult(
        eligible=True,
        source_size=(source_w, source_h),
        required_size=(required_w, required_h),
        fit_mode=fit_mode,
        high_resolution_family=bool(template.high_resolution_family),
    )
    if not template.skip_if_artwork_below_threshold:
        return result

    checks: List[Tuple[bool, str]] = []
    if template.min_source_width is not None:
        checks.append((source_w >= int(template.min_source_width), "min_source_width"))
    if template.min_source_height is not None:
        checks.append((source_h >= int(template.min_source_height), "min_source_height"))
    short_edge = min(source_w, source_h)
    long_edge = max(source_w, source_h)
    if template.min_source_short_edge is not None:
        checks.append((short_edge >= int(template.min_source_short_edge), "min_source_short_edge"))
    if template.min_source_long_edge is not None:
        checks.append((long_edge >= int(template.min_source_long_edge), "min_source_long_edge"))
    if fit_mode == "cover":
        required_ratio = float(template.min_effective_cover_ratio) if template.min_effective_cover_ratio is not None else 1.0
        cover_ratio = min(source_w / required_w, source_h / required_h) if required_w > 0 and required_h > 0 else 0.0
        checks.append((cover_ratio >= required_ratio, "min_effective_cover_ratio"))

    for passed, rule_name in checks:
        if not passed:
            result.eligible = False
            result.reason_code = "insufficient_artwork_resolution"
            result.rule_failed = rule_name
            return result
    return result


def list_eligible_templates_for_artwork(artwork: Artwork, templates: List[ProductTemplate]) -> Dict[str, ArtworkEligibilityResult]:
    results: Dict[str, ArtworkEligibilityResult] = {}
    for template in templates:
        resolved_placements = _resolve_template_placements(template, for_publish=True) or list(template.placements)
        if not resolved_placements:
            continue
        results[template.key] = evaluate_artwork_eligibility_for_template(
            artwork=artwork,
            template=template,
            placement=resolved_placements[0],
        )
    return results


def _resolve_poster_enhancement_settings(template: ProductTemplate) -> Tuple[float, float, float, bool]:
    max_upscale = (
        float(template.poster_safe_max_upscale_factor)
        if template.poster_safe_max_upscale_factor is not None
        else POSTER_SAFE_ENHANCEMENT_MAX_UPSCALE_FACTOR
    )
    min_ratio = (
        float(template.poster_safe_min_source_ratio)
        if template.poster_safe_min_source_ratio is not None
        else POSTER_SAFE_ENHANCEMENT_MIN_SOURCE_RATIO
    )
    fill_target_pct = (
        float(template.poster_fill_target_pct)
        if template.poster_fill_target_pct is not None
        else POSTER_FILL_TARGET_PCT_DEFAULT
    )
    return max_upscale, min_ratio, fill_target_pct, bool(template.poster_trim_fill_optimization)


def resolve_artwork_for_placement(
    artwork: Artwork,
    placement: PlacementRequirement,
    *,
    template: Optional[ProductTemplate] = None,
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
    auto_wallart_master: bool = False,
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
    poster_enhancement_tier = "none"
    poster_source_ratio = 0.0
    poster_requested_upscale_factor = 1.0
    poster_applied_upscale_factor = 1.0
    poster_trim_fill_optimization_applied = False
    poster_enhancement_status = "not_considered"
    wallart_derivation_reason = ""
    effective_allow_upscale = allow_upscale
    effective_max_upscale_factor = max_upscale_factor
    if template_key == "poster_basic":
        poster_max_upscale, poster_min_source_ratio, poster_fill_target_pct, poster_trim_fill_optimization = (
            _resolve_poster_enhancement_settings(template) if template is not None else (
                POSTER_SAFE_ENHANCEMENT_MAX_UPSCALE_FACTOR,
                POSTER_SAFE_ENHANCEMENT_MIN_SOURCE_RATIO,
                POSTER_FILL_TARGET_PCT_DEFAULT,
                False,
            )
        )
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
            poster_enhancement_status = "cover_eligible"
            logger.info(
                "Poster strategy selected template=%s placement=%s strategy=cover reason=eligible_resolution",
                template_key,
                placement.placement_name,
            )
        else:
            fit_mode = "contain"
            poster_strategy_path = "contain_fallback"
            poster_enhancement_considered = True
            poster_enhancement_status = "considered"
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
            poster_source_ratio = min_source_ratio
            source_aspect = image.width / max(1, image.height)
            is_small_source_portrait = source_aspect <= POSTER_SMALL_SOURCE_PORTRAIT_ASPECT_MAX
            standard_eligible = (
                poster_requested_upscale_factor > 1.0
                and poster_requested_upscale_factor <= poster_max_upscale
                and min_source_ratio >= poster_min_source_ratio
            )
            small_source_eligible = (
                poster_requested_upscale_factor > 1.0
                and template is not None
                and is_small_source_portrait
                and min_source_ratio >= POSTER_SMALL_SOURCE_MIN_SOURCE_RATIO
            )
            if poster_trim_fill_optimization and min_source_ratio >= poster_min_source_ratio:
                poster_trim_fill_optimization_applied = True
                _ = min(1.0, max(0.5, poster_fill_target_pct))
            elif poster_trim_fill_optimization and small_source_eligible:
                poster_trim_fill_optimization_applied = True

            if standard_eligible:
                effective_allow_upscale = True
                effective_max_upscale_factor = (
                    min(max_upscale_factor, poster_max_upscale)
                    if max_upscale_factor is not None and max_upscale_factor > 0
                    else poster_max_upscale
                )
                poster_enhancement_applied = True
                poster_enhancement_status = "applied"
                poster_enhancement_tier = "bounded_standard"
                poster_applied_upscale_factor = min(
                    poster_requested_upscale_factor,
                    effective_max_upscale_factor if effective_max_upscale_factor else poster_requested_upscale_factor,
                )
                logger.info(
                    "Poster enhancement applied placement=%s tier=%s strategy=bounded_contain_upscale requested_upscale_factor=%.3f applied_upscale_factor=%.3f max_allowed=%.3f source_ratio=%.3f trim_fill_optimization_applied=%s",
                    placement.placement_name,
                    poster_enhancement_tier,
                    poster_requested_upscale_factor,
                    poster_applied_upscale_factor,
                    effective_max_upscale_factor if effective_max_upscale_factor is not None else 0.0,
                    min_source_ratio,
                    poster_trim_fill_optimization_applied,
                )
            elif small_source_eligible:
                effective_allow_upscale = True
                small_source_max = max(poster_max_upscale, POSTER_SMALL_SOURCE_SAFE_MAX_UPSCALE_FACTOR)
                effective_max_upscale_factor = (
                    min(max_upscale_factor, small_source_max)
                    if max_upscale_factor is not None and max_upscale_factor > 0
                    else small_source_max
                )
                poster_enhancement_applied = True
                poster_enhancement_status = "applied"
                poster_enhancement_tier = "bounded_small_source"
                poster_applied_upscale_factor = min(
                    poster_requested_upscale_factor,
                    effective_max_upscale_factor if effective_max_upscale_factor else poster_requested_upscale_factor,
                )
                logger.info(
                    "Poster enhancement applied placement=%s tier=%s strategy=bounded_small_source_poster_upscale requested_upscale_factor=%.3f applied_upscale_factor=%.3f max_allowed=%.3f source_ratio=%.3f aspect=%.3f trim_fill_optimization_applied=%s",
                    placement.placement_name,
                    poster_enhancement_tier,
                    poster_requested_upscale_factor,
                    poster_applied_upscale_factor,
                    effective_max_upscale_factor if effective_max_upscale_factor is not None else 0.0,
                    min_source_ratio,
                    source_aspect,
                    poster_trim_fill_optimization_applied,
                )
            else:
                effective_allow_upscale = False
                poster_enhancement_status = "skipped_outside_safe_limits"
                logger.info(
                    "Poster enhancement skipped placement=%s reason=outside_safe_limits requested_upscale_factor=%.3f max_safe_upscale_factor=%.3f min_source_ratio=%.3f min_required_source_ratio=%.3f small_source_portrait=%s trim_fill_optimization_applied=%s",
                    placement.placement_name,
                    poster_requested_upscale_factor,
                    poster_max_upscale,
                    min_source_ratio,
                    poster_min_source_ratio,
                    is_small_source_portrait,
                    poster_trim_fill_optimization_applied,
                )

    too_small = fit_mode == "cover" and (image.width < placement.width_px or image.height < placement.height_px)
    wallart_auto_master_eligible = (
        auto_wallart_master
        and allow_upscale
        and template_key in WALLART_AUTO_MASTER_TEMPLATE_KEYS
        and fit_mode == "cover"
        and too_small
    )
    if wallart_auto_master_eligible:
        wallart_derivation_reason = "insufficient_artwork_resolution_cover_mode"
        source_before_trim = (image.width, image.height)
        trim_result = _trim_artwork_bounds(
            image,
            min_alpha=max(1, trim_min_alpha),
            padding_pct=max(0.005, trim_padding_pct),
            min_reduction_pct=min(trim_min_reduction_pct, 0.005),
        )
        image = trim_result.image
        if trim_result.applied:
            trimmed_size = trim_result.trimmed_size
            trim_applied = True
            trim_skip_reason = None
        else:
            trim_skip_reason = trim_result.skip_reason or trim_skip_reason
        requested_wallart_upscale = max(placement.width_px / image.width, placement.height_px / image.height)
        source_ratio = min(image.width / placement.width_px, image.height / placement.height_px)
        cap_applied = requested_wallart_upscale > WALLART_AUTO_MASTER_MAX_UPSCALE_FACTOR
        if source_ratio >= WALLART_AUTO_MASTER_MIN_SOURCE_RATIO and requested_wallart_upscale <= WALLART_AUTO_MASTER_MAX_UPSCALE_FACTOR:
            effective_allow_upscale = True
            effective_max_upscale_factor = WALLART_AUTO_MASTER_MAX_UPSCALE_FACTOR
            logger.info(
                "Wall-art derived master enabled template=%s placement=%s fallback_reason=%s source=%sx%s trimmed=%s required=%sx%s requested_upscale_factor=%.3f applied_upscale_factor=%.3f upscale_cap_applied=%s max_allowed=%.3f path=cover_crop preserve_transparency=%s",
                template_key,
                placement.placement_name,
                wallart_derivation_reason,
                source_before_trim[0],
                source_before_trim[1],
                f"{image.width}x{image.height}",
                placement.width_px,
                placement.height_px,
                requested_wallart_upscale,
                min(requested_wallart_upscale, WALLART_AUTO_MASTER_MAX_UPSCALE_FACTOR),
                cap_applied,
                WALLART_AUTO_MASTER_MAX_UPSCALE_FACTOR,
                str(artwork.src_path.suffix.lower() == ".png").lower(),
            )
            too_small = False
        else:
            effective_allow_upscale = False
            logger.warning(
                "Wall-art derived master skipped template=%s placement=%s fallback_reason=%s source=%sx%s trimmed=%sx%s required=%sx%s requested_upscale_factor=%.3f max_allowed=%.3f min_source_ratio=%.3f required_min_ratio=%.3f reason=outside_safe_limits final_action=skip_insufficient_resolution",
                template_key,
                placement.placement_name,
                wallart_derivation_reason,
                source_before_trim[0],
                source_before_trim[1],
                image.width,
                image.height,
                placement.width_px,
                placement.height_px,
                requested_wallart_upscale,
                WALLART_AUTO_MASTER_MAX_UPSCALE_FACTOR,
                source_ratio,
                WALLART_AUTO_MASTER_MIN_SOURCE_RATIO,
            )

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
        raise InsufficientArtworkResolutionError(
            template_key=template_key,
            placement_name=placement.placement_name,
            source_size=original_size,
            required_size=required_size,
            fit_mode=fit_mode,
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
    if wallart_auto_master_eligible and upscaled:
        action = "derived_wallart_master_upscale"

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
            "Poster final resolution path chosen strategy=%s tier=%s fit_mode=%s action=%s upscaled=%s upscale_capped=%s enhancement_considered=%s enhancement_applied=%s source_ratio=%.3f requested_upscale_factor=%.3f applied_upscale_factor=%.3f trim_fill_optimization_applied=%s",
            poster_strategy_path or fit_mode,
            poster_enhancement_tier,
            fit_mode,
            action,
            upscaled,
            upscale_capped,
            poster_enhancement_considered,
            poster_enhancement_applied,
            poster_source_ratio,
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
        poster_cover_eligible=poster_cover_eligible if template_key == "poster_basic" else None,
        poster_enhancement_status=poster_enhancement_status if template_key == "poster_basic" else "",
        poster_enhancement_tier=poster_enhancement_tier if template_key == "poster_basic" else "",
        poster_source_ratio=poster_source_ratio if template_key == "poster_basic" else 0.0,
        poster_requested_upscale_factor=poster_requested_upscale_factor if template_key == "poster_basic" else 1.0,
        poster_applied_upscale_factor=applied_upscale_factor if template_key == "poster_basic" else 1.0,
        poster_fill_optimization_used=poster_trim_fill_optimization_applied if template_key == "poster_basic" else False,
        wallart_derivation_reason=wallart_derivation_reason,
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
        template=template,
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
        auto_wallart_master=options.auto_wallart_master,
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
        poster_cover_eligible=resolution.poster_cover_eligible,
        poster_enhancement_status=resolution.poster_enhancement_status,
        poster_enhancement_tier=resolution.poster_enhancement_tier,
        poster_source_ratio=resolution.poster_source_ratio,
        poster_requested_upscale_factor=resolution.poster_requested_upscale_factor,
        poster_applied_upscale_factor=resolution.poster_applied_upscale_factor,
        poster_fill_optimization_used=resolution.poster_fill_optimization_used,
        wallart_derivation_reason=resolution.wallart_derivation_reason,
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
    if row.get("active_placements") is not None and not isinstance(row.get("active_placements"), list):
        raise TemplateValidationError(f"Template[{index}] active_placements must be a list when provided")
    if isinstance(row.get("active_placements"), list):
        declared = {str(p.get("placement_name", "")).strip().lower() for p in row["placements"]}
        for placement_name in row.get("active_placements", []):
            if str(placement_name).strip().lower() not in declared:
                raise TemplateValidationError(
                    f"Template[{index}] active_placements references unknown placement '{placement_name}'"
                )
    if row.get("preferred_primary_placement") is not None and not str(row.get("preferred_primary_placement")).strip():
        raise TemplateValidationError(f"Template[{index}] preferred_primary_placement cannot be blank when provided")
    if row.get("publish_only_primary_placement") is not None and not isinstance(row.get("publish_only_primary_placement"), bool):
        raise TemplateValidationError(f"Template[{index}] publish_only_primary_placement must be boolean when provided")
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
    for margin_key in ("min_margin_after_shipping", "min_profit_after_shipping", "target_margin_after_shipping"):
        if row.get(margin_key) is not None and _decimal_from_value(row.get(margin_key), default="0") < 0:
            raise TemplateValidationError(f"Template[{index}] {margin_key} must be >= 0 when provided")
    if row.get("shipping_basis_for_margin") is not None:
        basis = str(row.get("shipping_basis_for_margin", "cost")).strip().lower()
        if basis not in {"cost"}:
            raise TemplateValidationError(f"Template[{index}] shipping_basis_for_margin must be 'cost'")
    for bool_field in ("disable_variants_below_margin_floor", "reprice_variants_to_margin_floor", "mark_template_nonviable_if_needed"):
        if row.get(bool_field) is not None and not isinstance(row.get(bool_field), bool):
            raise TemplateValidationError(f"Template[{index}] {bool_field} must be boolean when provided")
    for bool_field in ("active", "fallback_provider_allowed"):
        if row.get(bool_field) is not None and not isinstance(row.get(bool_field), bool):
            raise TemplateValidationError(f"Template[{index}] {bool_field} must be boolean when provided")
    if row.get("provider_selection_strategy") is not None:
        strategy = str(row.get("provider_selection_strategy", "")).strip().lower()
        if strategy not in {"pinned_then_printify_choice_then_lowest_cost", "prefer_printify_choice_then_ranked"}:
            raise TemplateValidationError(
                f"Template[{index}] provider_selection_strategy must be pinned_then_printify_choice_then_lowest_cost|prefer_printify_choice_then_ranked"
            )
    for int_field in ("pinned_provider_id", "pinned_blueprint_id"):
        if row.get(int_field) is not None and int(row.get(int_field)) <= 0:
            raise TemplateValidationError(f"Template[{index}] {int_field} must be > 0 when provided")
    if row.get("provider_preference_order") is not None:
        if not isinstance(row.get("provider_preference_order"), list):
            raise TemplateValidationError(f"Template[{index}] provider_preference_order must be a list when provided")
    if row.get("max_upscale_factor") is not None and float(row.get("max_upscale_factor", 0)) <= 0:
        raise TemplateValidationError(f"Template[{index}] max_upscale_factor must be > 0 when provided")
    if row.get("poster_safe_max_upscale_factor") is not None and float(row.get("poster_safe_max_upscale_factor", 0)) <= 0:
        raise TemplateValidationError(f"Template[{index}] poster_safe_max_upscale_factor must be > 0 when provided")
    if row.get("poster_safe_min_source_ratio") is not None:
        ratio = float(row.get("poster_safe_min_source_ratio", 0))
        if ratio <= 0 or ratio > 1:
            raise TemplateValidationError(f"Template[{index}] poster_safe_min_source_ratio must be > 0 and <= 1 when provided")
    if row.get("poster_fill_target_pct") is not None:
        target = float(row.get("poster_fill_target_pct", 0))
        if target <= 0 or target > 1:
            raise TemplateValidationError(f"Template[{index}] poster_fill_target_pct must be > 0 and <= 1 when provided")
    if row.get("poster_trim_fill_optimization") is not None and not isinstance(row.get("poster_trim_fill_optimization"), bool):
        raise TemplateValidationError(f"Template[{index}] poster_trim_fill_optimization must be boolean when provided")
    if row.get("high_resolution_family") is not None and not isinstance(row.get("high_resolution_family"), bool):
        raise TemplateValidationError(f"Template[{index}] high_resolution_family must be boolean when provided")
    if row.get("skip_if_artwork_below_threshold") is not None and not isinstance(row.get("skip_if_artwork_below_threshold"), bool):
        raise TemplateValidationError(f"Template[{index}] skip_if_artwork_below_threshold must be boolean when provided")
    for int_field in ("min_source_width", "min_source_height", "min_source_short_edge", "min_source_long_edge"):
        if row.get(int_field) is not None and int(row.get(int_field, 0)) <= 0:
            raise TemplateValidationError(f"Template[{index}] {int_field} must be > 0 when provided")
    if row.get("min_effective_cover_ratio") is not None:
        ratio = float(row.get("min_effective_cover_ratio", 0))
        if ratio <= 0:
            raise TemplateValidationError(f"Template[{index}] min_effective_cover_ratio must be > 0 when provided")
    if row.get("preferred_featured_image_strategy") is not None:
        strategy = str(row.get("preferred_featured_image_strategy", "")).strip().lower()
        if strategy and strategy not in {"variant_color_then_mockup_type", "mockup_type_then_variant_color"}:
            raise TemplateValidationError(
                f"Template[{index}] preferred_featured_image_strategy must be variant_color_then_mockup_type|mockup_type_then_variant_color"
            )
    option_filters = row.get("enabled_variant_option_filters")
    if option_filters is not None and not isinstance(option_filters, dict):
        raise TemplateValidationError(f"Template[{index}] enabled_variant_option_filters must be an object")
    if row.get("artwork_routing_family") is not None:
        routing_family = str(row.get("artwork_routing_family", "")).strip().lower()
        if routing_family not in {APPAREL_FAMILY, POSTER_FAMILY, BLANKET_FAMILY, SQUARE_FAMILY, "single"}:
            raise TemplateValidationError(
                f"Template[{index}] artwork_routing_family must be one of {[APPAREL_FAMILY, POSTER_FAMILY, BLANKET_FAMILY, SQUARE_FAMILY, 'single']}"
            )
    fit_policy = str(row.get("artwork_fit_policy", "placement_defined")).strip().lower() or "placement_defined"
    if fit_policy not in TEMPLATE_FIT_POLICY_VALUES:
        raise TemplateValidationError(
            f"Template[{index}] artwork_fit_policy must be one of {sorted(TEMPLATE_FIT_POLICY_VALUES)}"
        )
    cover_behavior = str(row.get("cover_behavior", "unspecified")).strip().lower() or "unspecified"
    if cover_behavior not in TEMPLATE_COVER_BEHAVIOR_VALUES:
        raise TemplateValidationError(
            f"Template[{index}] cover_behavior must be one of {sorted(TEMPLATE_COVER_BEHAVIOR_VALUES)}"
        )
    high_res_intent = str(row.get("high_resolution_intent", "standard")).strip().lower() or "standard"
    if high_res_intent not in TEMPLATE_HIGH_RES_INTENT_VALUES:
        raise TemplateValidationError(
            f"Template[{index}] high_resolution_intent must be one of {sorted(TEMPLATE_HIGH_RES_INTENT_VALUES)}"
        )
    crop_tolerance = str(row.get("crop_tolerance", "unspecified")).strip().lower() or "unspecified"
    if crop_tolerance not in TEMPLATE_CROP_TOLERANCE_VALUES:
        raise TemplateValidationError(
            f"Template[{index}] crop_tolerance must be one of {sorted(TEMPLATE_CROP_TOLERANCE_VALUES)}"
        )
    if row.get("requires_certification") is not None and not isinstance(row.get("requires_certification"), bool):
        raise TemplateValidationError(f"Template[{index}] requires_certification must be boolean when provided")
    certification_stage = str(row.get("certification_stage", "none")).strip().lower() or "none"
    if certification_stage not in TEMPLATE_CERTIFICATION_STAGE_VALUES:
        raise TemplateValidationError(
            f"Template[{index}] certification_stage must be one of {sorted(TEMPLATE_CERTIFICATION_STAGE_VALUES)}"
        )
    placement_fit_modes = {
        str(placement.get("artwork_fit_mode", "contain")).strip().lower() or "contain"
        for placement in row.get("placements", [])
    }
    if fit_policy == "contain_required" and "cover" in placement_fit_modes:
        raise TemplateValidationError(
            f"Template[{index}] artwork_fit_policy=contain_required is incompatible with cover placements"
        )
    if fit_policy == "cover_required" and "contain" in placement_fit_modes:
        raise TemplateValidationError(
            f"Template[{index}] artwork_fit_policy=cover_required is incompatible with contain placements"
        )
    has_cover_placement = "cover" in placement_fit_modes
    if cover_behavior in {"allow_safe_crop", "require_full_cover"} and not has_cover_placement:
        raise TemplateValidationError(
            f"Template[{index}] cover_behavior={cover_behavior} requires at least one cover placement"
        )
    if cover_behavior == "require_full_cover":
        ratio = float(row.get("min_effective_cover_ratio", 0))
        if ratio < 1.0:
            raise TemplateValidationError(
                f"Template[{index}] cover_behavior=require_full_cover requires min_effective_cover_ratio >= 1.0"
            )
    if high_res_intent == "high_resolution_required" and not bool(row.get("high_resolution_family", False)):
        raise TemplateValidationError(
            f"Template[{index}] high_resolution_intent=high_resolution_required requires high_resolution_family=true"
        )
    if crop_tolerance == "none" and has_cover_placement:
        raise TemplateValidationError(
            f"Template[{index}] crop_tolerance=none is incompatible with cover placements"
        )
    if bool(row.get("requires_certification", False)) and certification_stage != "production_ready":
        raise TemplateValidationError(
            f"Template[{index}] requires_certification=true requires certification_stage=production_ready"
        )


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
                max_allowed_price=str(row["max_allowed_price"]) if row.get("max_allowed_price") is not None else None,
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
                active_placements=[str(v) for v in row.get("active_placements", [])],
                preferred_primary_placement=str(row.get("preferred_primary_placement", "")).strip() or None,
                publish_only_primary_placement=bool(row.get("publish_only_primary_placement", False)),
                poster_safe_max_upscale_factor=float(row["poster_safe_max_upscale_factor"]) if row.get("poster_safe_max_upscale_factor") is not None else None,
                poster_safe_min_source_ratio=float(row["poster_safe_min_source_ratio"]) if row.get("poster_safe_min_source_ratio") is not None else None,
                poster_fill_target_pct=float(row["poster_fill_target_pct"]) if row.get("poster_fill_target_pct") is not None else None,
                poster_trim_fill_optimization=bool(row.get("poster_trim_fill_optimization", False)),
                preferred_mockup_colors=[str(v) for v in row.get("preferred_mockup_colors", [])],
                preferred_default_variant_color=str(row.get("preferred_default_variant_color", "")).strip() or None,
                preferred_mockup_types=[str(v) for v in row.get("preferred_mockup_types", [])],
                preferred_featured_image_strategy=str(row.get("preferred_featured_image_strategy", "variant_color_then_mockup_type")).strip().lower() or "variant_color_then_mockup_type",
                preferred_mockup_position=str(row.get("preferred_mockup_position", "")).strip() or None,
                secondary_collection_handles=[str(v) for v in row.get("secondary_collection_handles", [])],
                min_margin_after_shipping=str(row["min_margin_after_shipping"]) if row.get("min_margin_after_shipping") is not None else None,
                min_profit_after_shipping=str(row["min_profit_after_shipping"]) if row.get("min_profit_after_shipping") is not None else None,
                target_margin_after_shipping=str(row["target_margin_after_shipping"]) if row.get("target_margin_after_shipping") is not None else None,
                shipping_basis_for_margin=str(row.get("shipping_basis_for_margin", "cost")).strip().lower() or "cost",
                disable_variants_below_margin_floor=bool(row.get("disable_variants_below_margin_floor", False)),
                reprice_variants_to_margin_floor=bool(row.get("reprice_variants_to_margin_floor", True)),
                mark_template_nonviable_if_needed=bool(row.get("mark_template_nonviable_if_needed", False)),
                active=bool(row.get("active", True)),
                provider_selection_strategy=str(
                    row.get("provider_selection_strategy", "pinned_then_printify_choice_then_lowest_cost")
                ).strip().lower() or "pinned_then_printify_choice_then_lowest_cost",
                pinned_provider_id=int(row["pinned_provider_id"]) if row.get("pinned_provider_id") is not None else None,
                pinned_blueprint_id=int(row["pinned_blueprint_id"]) if row.get("pinned_blueprint_id") is not None else None,
                provider_preference_order=[int(v) for v in row.get("provider_preference_order", [])],
                fallback_provider_allowed=bool(row.get("fallback_provider_allowed", True)),
                high_resolution_family=bool(row.get("high_resolution_family", False)),
                skip_if_artwork_below_threshold=bool(row.get("skip_if_artwork_below_threshold", False)),
                min_source_width=int(row["min_source_width"]) if row.get("min_source_width") is not None else None,
                min_source_height=int(row["min_source_height"]) if row.get("min_source_height") is not None else None,
                min_source_short_edge=int(row["min_source_short_edge"]) if row.get("min_source_short_edge") is not None else None,
                min_source_long_edge=int(row["min_source_long_edge"]) if row.get("min_source_long_edge") is not None else None,
                min_effective_cover_ratio=float(row["min_effective_cover_ratio"]) if row.get("min_effective_cover_ratio") is not None else None,
                expanded_enabled_colors=[str(v) for v in row.get("expanded_enabled_colors", [])],
                storefront_display_color_priority=[str(v) for v in row.get("storefront_display_color_priority", [])],
                storefront_display_color_rotation_seed=str(row.get("storefront_display_color_rotation_seed", "")).strip() or None,
                storefront_default_color_candidates=[str(v) for v in row.get("storefront_default_color_candidates", [])],
                artwork_routing_family=str(row.get("artwork_routing_family", "")).strip().lower() or None,
                artwork_fit_policy=str(row.get("artwork_fit_policy", "placement_defined")).strip().lower() or "placement_defined",
                cover_behavior=str(row.get("cover_behavior", "unspecified")).strip().lower() or "unspecified",
                high_resolution_intent=str(row.get("high_resolution_intent", "standard")).strip().lower() or "standard",
                crop_tolerance=str(row.get("crop_tolerance", "unspecified")).strip().lower() or "unspecified",
                requires_certification=bool(row.get("requires_certification", False)),
                certification_stage=str(row.get("certification_stage", "none")).strip().lower() or "none",
                placements=[PlacementRequirement(**p) for p in row.get("placements", [])],
            )
        )

    return templates


def choose_variants_from_catalog_with_diagnostics(catalog_variants: Any, template: ProductTemplate) -> Tuple[List[Dict[str, Any]], VariantFilterDiagnostics]:
    catalog_variants = normalize_catalog_variants_response(catalog_variants)
    chosen, diagnostics = _analyze_variant_filtering(catalog_variants, template)
    def _is_apparel_recovery_key(key: str) -> bool:
        return key in {"tshirt_gildan", "hoodie_gildan", "sweatshirt_gildan"}

    if _is_apparel_recovery_key(template.key):
        color_rank = {"Black": 0, "White": 1, "Navy": 2, "Sport Grey": 3, "Sand": 4, "Dark Heather": 5, "Maroon": 6, "Military Green": 7, "Charcoal": 8, "Ash": 9, "Carolina Blue": 10}
        size_rank = {"S": 0, "M": 1, "L": 2, "XL": 3, "2XL": 4, "3XL": 5}

        def _variant_sort_key(variant: Dict[str, Any]) -> Tuple[int, int, int]:
            color = _variant_option_value(variant, "color")
            size = _variant_option_value(variant, "size")
            cost_source = variant.get("cost") if variant.get("cost") is not None else variant.get("price")
            cost_minor = normalize_printify_price(cost_source if cost_source is not None else template.default_price)
            return (
                color_rank.get(color, 99),
                size_rank.get(size, 99),
                cost_minor,
            )

        chosen.sort(key=_variant_sort_key)

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
        "Variant selection template=%s selected=%s available=%s option_names=%s option_values=%s requested_filters=%s filter_counts=%s zero_reason=%s max_enabled_variants=%s",
        template.key,
        len(chosen),
        len(catalog_variants),
        diagnostics.option_names,
        diagnostics.option_values_summary,
        diagnostics.requested_option_filters,
        diagnostics.filter_counts,
        diagnostics.zero_selection_reason,
        effective_limit,
    )
    if diagnostics.selected_additional_colors or diagnostics.unavailable_additional_colors:
        logger.info(
            "Color expansion template=%s selected_additional_colors=%s unavailable_additional_colors=%s",
            template.key,
            diagnostics.selected_additional_colors,
            diagnostics.unavailable_additional_colors,
        )
    return chosen, diagnostics


def choose_variants_from_catalog(catalog_variants: Any, template: ProductTemplate) -> List[Dict[str, Any]]:
    chosen, _ = choose_variants_from_catalog_with_diagnostics(catalog_variants, template)
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


def _is_printify_update_incompatible_error(exc: Exception) -> bool:
    if not isinstance(exc, NonRetryableRequestError):
        return False
    message = str(exc)
    if "HTTP 400" not in message:
        return False
    lowered = message.lower()
    return ("code" in lowered and "8251" in lowered) or (
        "variants do not match selected blueprint and print provider" in lowered
    )


def _is_printify_product_edit_disabled_error(exc: Exception) -> bool:
    if not isinstance(exc, NonRetryableRequestError):
        return False
    message = str(exc)
    if "HTTP 400" not in message:
        return False
    lowered = message.lower()
    return ("code" in lowered and "8252" in lowered) or ("product is disabled for editing" in lowered)


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


def _preflight_template(
    *,
    printify: PrintifyClient,
    template: ProductTemplate,
    blueprint_ids: set[int],
    blueprint_titles: Optional[Dict[int, str]] = None,
) -> Tuple[Optional[TemplatePreflightIssue], TemplatePreflightReportRow]:
    resolved_template, resolution_diag = _resolve_template_catalog_mapping(
        printify=printify,
        template=template,
        discovery_mode="normal",
    )
    blueprint_id = int(resolved_template.printify_blueprint_id)
    provider_id = int(resolved_template.printify_print_provider_id)
    template_hint_blueprint_id = int(template.printify_blueprint_id)
    template_hint_provider_id = int(template.printify_print_provider_id)
    runtime_mapping_overrode_hint = (
        template_hint_blueprint_id != blueprint_id
        or template_hint_provider_id != provider_id
    )
    is_apparel_diag_template = template.key in {"tshirt_gildan", "hoodie_gildan"}

    def _recommended_action_for(classification: str) -> str:
        if classification == "zero_enabled_after_guardrails":
            return "fix_pricing"
        if classification == "invalid_template_config":
            return "fix_blueprint_mapping" if blueprint_id <= 0 or blueprint_id not in blueprint_ids else "fix_provider_selection"
        if classification == "wrong_catalog_family":
            return "fix_blueprint_mapping"
        if classification == "zero_variants_selected":
            return "deactivate_pending_validation"
        if classification == "canvas_size_filter_mismatch":
            return "adjust_size_curation"
        return "deactivate_pending_validation"

    def _failure(
        classification: str,
        message: str,
        *,
        selected_count: int = 0,
        repriced_count: int = 0,
        disabled_count_after_reprice: int = 0,
        final_enabled_count: int = 0,
        tote_diag: Optional[Dict[str, Any]] = None,
        apparel_diag: Optional[Dict[str, Any]] = None,
        failed_variant_reasons: Optional[Dict[int, str]] = None,
        failure_reason_counts: Optional[Dict[str, int]] = None,
        option_diagnostics: Optional[VariantFilterDiagnostics] = None,
        intended_family: str = "",
        resolved_blueprint_title: str = "",
        resolved_provider_title: str = "",
        family_mismatch_reason: str = "",
    ) -> Tuple[TemplatePreflightIssue, TemplatePreflightReportRow]:
        tote_diag = tote_diag or {}
        apparel_diag = apparel_diag or {}
        failed_variant_reasons = failed_variant_reasons or {}
        failure_reason_counts = failure_reason_counts or {}
        option_diagnostics = option_diagnostics or VariantFilterDiagnostics()
        issue = TemplatePreflightIssue(
            template_key=template.key,
            classification=classification,
            message=message,
            blueprint_id=blueprint_id,
            provider_id=provider_id,
            selected_count=selected_count,
            repriced_count=repriced_count,
            disabled_count_after_reprice=disabled_count_after_reprice,
            final_enabled_count=final_enabled_count,
            recommended_action=_recommended_action_for(classification),
        )
        row = TemplatePreflightReportRow(
            template_key=template.key,
            requested_explicitly=False,
            preflight_status="failed",
            classification=classification,
            message=message,
            blueprint_id=blueprint_id,
            provider_id=provider_id,
            selected_count=selected_count,
            repriced_count=repriced_count,
            disabled_count_after_reprice=disabled_count_after_reprice,
            final_enabled_count=final_enabled_count,
            recommended_action=issue.recommended_action,
            tote_original_sale_price_minor=int(tote_diag.get("original_sale_price_minor", 0)),
            tote_repriced_sale_price_minor=int(tote_diag.get("repriced_sale_price_minor", 0)),
            tote_printify_cost_minor=int(tote_diag.get("printify_cost_minor", 0)),
            tote_shipping_basis_used=str(tote_diag.get("shipping_basis_used", "")),
            tote_target_margin_after_shipping_minor=int(tote_diag.get("target_margin_after_shipping_minor", 0)),
            tote_min_margin_after_shipping_minor=int(tote_diag.get("min_margin_after_shipping_minor", 0)),
            tote_margin_before_reprice_minor=int(tote_diag.get("after_shipping_margin_before_reprice_minor", 0)),
            tote_margin_after_reprice_minor=int(tote_diag.get("after_shipping_margin_after_reprice_minor", 0)),
            tote_max_allowed_price_minor=int(tote_diag.get("max_allowed_price_minor", 0)),
            tote_failure_reason=str(tote_diag.get("failure_reason", "")),
            apparel_original_sale_price_minor=int(apparel_diag.get("original_sale_price_minor", 0)),
            apparel_repriced_sale_price_minor=int(apparel_diag.get("repriced_sale_price_minor", 0)),
            apparel_printify_cost_minor=int(apparel_diag.get("printify_cost_minor", 0)),
            apparel_shipping_basis_used=str(apparel_diag.get("shipping_basis_used", "")),
            apparel_shipping_minor=int(apparel_diag.get("shipping_minor", 0)),
            apparel_target_margin_after_shipping_minor=int(apparel_diag.get("target_margin_after_shipping_minor", 0)),
            apparel_min_margin_after_shipping_minor=int(apparel_diag.get("min_margin_after_shipping_minor", 0)),
            apparel_margin_before_reprice_minor=int(apparel_diag.get("after_shipping_margin_before_reprice_minor", 0)),
            apparel_margin_after_reprice_minor=int(apparel_diag.get("after_shipping_margin_after_reprice_minor", 0)),
            apparel_max_allowed_price_minor=int(apparel_diag.get("max_allowed_price_minor", 0)),
            apparel_failed_variant_reasons=json.dumps(failed_variant_reasons, sort_keys=True),
            apparel_failure_reason_counts=json.dumps(failure_reason_counts, sort_keys=True),
            option_names=json.dumps(option_diagnostics.option_names, sort_keys=True),
            option_values_summary=json.dumps(option_diagnostics.option_values_summary, sort_keys=True),
            requested_option_filters=json.dumps(option_diagnostics.requested_option_filters, sort_keys=True),
            filter_counts=json.dumps(option_diagnostics.filter_counts, sort_keys=True),
            zero_selection_reason=str(option_diagnostics.zero_selection_reason or ""),
            intended_family=intended_family,
            resolved_blueprint_title=resolved_blueprint_title,
            resolved_provider_title=resolved_provider_title,
            family_mismatch_reason=family_mismatch_reason,
            template_hint_blueprint_id=template_hint_blueprint_id,
            template_hint_provider_id=template_hint_provider_id,
            runtime_mapping_overrode_hint=runtime_mapping_overrode_hint,
            catalog_discovery_used=bool(resolution_diag.discovery_used),
            pinned_mapping_attempted_first=bool(resolution_diag.pinned_attempted_first),
            fallback_discovery_triggered=bool(resolution_diag.fallback_discovery_triggered),
            fallback_discovery_reason=str(resolution_diag.fallback_discovery_reason or ""),
            catalog_resolution_mode=str(resolution_diag.discovery_mode or "normal"),
            resolved_model_dimension=str(option_diagnostics.resolved_model_dimension or ""),
            requested_model_overlap_count=int(option_diagnostics.requested_model_overlap_count or 0),
            fallback_model_set_applied=bool(option_diagnostics.fallback_model_set_applied),
            final_selected_models=json.dumps(option_diagnostics.final_selected_models, sort_keys=True),
            high_resolution_family=bool(template.high_resolution_family),
        )
        return issue, row

    try:
        if blueprint_id not in blueprint_ids:
            return _failure("invalid_template_config", f"Blueprint {blueprint_id} is not available.")
        providers = printify.list_print_providers(blueprint_id)
        provider_titles = {
            int(provider.get("id", 0)): str(provider.get("title") or provider.get("name") or "")
            for provider in providers
        }
        provider_ids = {int(provider.get("id", 0)) for provider in providers}
        if provider_id not in provider_ids:
            return _failure(
                "invalid_template_config",
                f"Provider {provider_id} is not available for blueprint {blueprint_id}.",
            )
        variants = printify.list_variants(blueprint_id, provider_id)
        normalized_variants = normalize_catalog_variants_response(variants)
        resolved_blueprint_title = str((blueprint_titles or {}).get(blueprint_id, ""))
        resolved_provider_title = str(provider_titles.get(provider_id, ""))
        family_validation = validate_catalog_family_schema(
            template=resolved_template,
            variants=normalized_variants,
            blueprint_title=resolved_blueprint_title,
            provider_title=resolved_provider_title,
        )
        if not family_validation.plausible:
            option_names, option_values = _collect_option_names_and_values(normalized_variants)
            return _failure(
                "wrong_catalog_family",
                "Resolved catalog schema is inconsistent with intended product family. "
                f"intended_family={family_validation.intended_family} "
                f"blueprint_title={resolved_blueprint_title or '-'} provider_title={resolved_provider_title or '-'} "
                f"available_option_names={option_names} reason={family_validation.reason}",
                option_diagnostics=VariantFilterDiagnostics(
                    option_names=option_names,
                    option_values_summary=option_values,
                    zero_selection_reason=family_validation.reason,
                ),
                intended_family=family_validation.intended_family,
                resolved_blueprint_title=resolved_blueprint_title,
                resolved_provider_title=resolved_provider_title,
                family_mismatch_reason=family_validation.reason,
            )

        selected, option_diagnostics = _analyze_variant_filtering(normalized_variants, resolved_template)
        if not selected:
            zero_classification = _classify_zero_selection(
                resolved_template,
                option_diagnostics,
                intended_family=family_validation.intended_family,
            )
            diagnostic_message = (
                "Curated option filters selected zero variants. "
                f"blueprint_id={blueprint_id} provider_id={provider_id} "
                f"available_option_names={option_diagnostics.option_names} "
                f"available_option_values={option_diagnostics.option_values_summary} "
                f"requested_filters={option_diagnostics.requested_option_filters} "
                f"filter_counts={option_diagnostics.filter_counts} "
                f"reason={option_diagnostics.zero_selection_reason or 'no_intersection'}"
            )
            return _failure(
                zero_classification,
                diagnostic_message,
                option_diagnostics=option_diagnostics,
                intended_family=family_validation.intended_family,
                resolved_blueprint_title=resolved_blueprint_title,
                resolved_provider_title=resolved_provider_title,
            )
        guarded, report = apply_variant_margin_guardrails(resolved_template, selected)
        tote_diag = (report.get("variant_diagnostics") or [])[0] if template.key == "tote_basic" else {}
        selected_count = int(report.get("selected_count", 0))
        repriced_count = int(report.get("repriced_count", 0))
        disabled_count = int(report.get("disabled_count_after_reprice", 0))
        final_enabled_count = len(guarded)
        apparel_diag = (report.get("variant_diagnostics") or [])[0] if is_apparel_diag_template else {}
        failed_variant_reasons = {int(k): str(v) for k, v in (report.get("failed_variant_reasons") or {}).items()}
        failure_reason_counts = {str(k): int(v) for k, v in (report.get("failure_reason_counts") or {}).items()}
        if not guarded:
            tote_diag_suffix = ""
            if template.key == "tote_basic" and tote_diag:
                tote_diag_suffix = (
                    " "
                    f"Tote economics: original_sale_minor={int(tote_diag.get('original_sale_price_minor', 0))} "
                    f"repriced_sale_minor={int(tote_diag.get('repriced_sale_price_minor', 0))} "
                    f"cost_minor={int(tote_diag.get('printify_cost_minor', 0))} "
                    f"shipping_basis={str(tote_diag.get('shipping_basis_used', '')) or '-'} "
                    f"margin_before_minor={int(tote_diag.get('after_shipping_margin_before_reprice_minor', 0))} "
                    f"margin_after_minor={int(tote_diag.get('after_shipping_margin_after_reprice_minor', 0))} "
                    f"min_margin_minor={int(tote_diag.get('min_margin_after_shipping_minor', 0))} "
                    f"target_margin_minor={int(tote_diag.get('target_margin_after_shipping_minor', 0))} "
                    f"max_allowed_minor={int(tote_diag.get('max_allowed_price_minor', 0))} "
                    f"reason={str(tote_diag.get('failure_reason', 'margin_below_floor_after_reprice'))}"
                )
            apparel_diag_suffix = ""
            if is_apparel_diag_template and apparel_diag:
                apparel_diag_suffix = (
                    " "
                    f"Apparel economics: original_sale_minor={int(apparel_diag.get('original_sale_price_minor', 0))} "
                    f"repriced_sale_minor={int(apparel_diag.get('repriced_sale_price_minor', 0))} "
                    f"cost_minor={int(apparel_diag.get('printify_cost_minor', 0))} "
                    f"shipping_basis={str(apparel_diag.get('shipping_basis_used', '')) or '-'} "
                    f"shipping_minor={int(apparel_diag.get('shipping_minor', 0))} "
                    f"margin_before_minor={int(apparel_diag.get('after_shipping_margin_before_reprice_minor', 0))} "
                    f"margin_after_minor={int(apparel_diag.get('after_shipping_margin_after_reprice_minor', 0))} "
                    f"min_margin_minor={int(apparel_diag.get('min_margin_after_shipping_minor', 0))} "
                    f"target_margin_minor={int(apparel_diag.get('target_margin_after_shipping_minor', 0))} "
                    f"max_allowed_minor={int(apparel_diag.get('max_allowed_price_minor', 0))} "
                    f"failure_reason_counts={json.dumps(failure_reason_counts, sort_keys=True)}"
                )
            return _failure(
                "zero_enabled_after_guardrails",
                "All selected variants were disabled after pricing guardrails "
                f"(selected={selected_count} repriced={repriced_count}).{tote_diag_suffix}{apparel_diag_suffix}",
                selected_count=selected_count,
                repriced_count=repriced_count,
                disabled_count_after_reprice=disabled_count,
                final_enabled_count=final_enabled_count,
                tote_diag=tote_diag,
                apparel_diag=apparel_diag,
                failed_variant_reasons=failed_variant_reasons,
                failure_reason_counts=failure_reason_counts,
                option_diagnostics=option_diagnostics,
                intended_family=family_validation.intended_family,
                resolved_blueprint_title=resolved_blueprint_title,
                resolved_provider_title=resolved_provider_title,
            )
        return None, TemplatePreflightReportRow(
            template_key=template.key,
            requested_explicitly=False,
            preflight_status="passed",
            classification="",
            message="Template preflight passed.",
            blueprint_id=blueprint_id,
            provider_id=provider_id,
            selected_count=selected_count,
            repriced_count=repriced_count,
            disabled_count_after_reprice=disabled_count,
            final_enabled_count=final_enabled_count,
            recommended_action="keep_active",
            tote_original_sale_price_minor=int(tote_diag.get("original_sale_price_minor", 0)),
            tote_repriced_sale_price_minor=int(tote_diag.get("repriced_sale_price_minor", 0)),
            tote_printify_cost_minor=int(tote_diag.get("printify_cost_minor", 0)),
            tote_shipping_basis_used=str(tote_diag.get("shipping_basis_used", "")),
            tote_target_margin_after_shipping_minor=int(tote_diag.get("target_margin_after_shipping_minor", 0)),
            tote_min_margin_after_shipping_minor=int(tote_diag.get("min_margin_after_shipping_minor", 0)),
            tote_margin_before_reprice_minor=int(tote_diag.get("after_shipping_margin_before_reprice_minor", 0)),
            tote_margin_after_reprice_minor=int(tote_diag.get("after_shipping_margin_after_reprice_minor", 0)),
            tote_max_allowed_price_minor=int(tote_diag.get("max_allowed_price_minor", 0)),
            tote_failure_reason=str(tote_diag.get("failure_reason", "")),
            apparel_original_sale_price_minor=int(apparel_diag.get("original_sale_price_minor", 0)),
            apparel_repriced_sale_price_minor=int(apparel_diag.get("repriced_sale_price_minor", 0)),
            apparel_printify_cost_minor=int(apparel_diag.get("printify_cost_minor", 0)),
            apparel_shipping_basis_used=str(apparel_diag.get("shipping_basis_used", "")),
            apparel_shipping_minor=int(apparel_diag.get("shipping_minor", 0)),
            apparel_target_margin_after_shipping_minor=int(apparel_diag.get("target_margin_after_shipping_minor", 0)),
            apparel_min_margin_after_shipping_minor=int(apparel_diag.get("min_margin_after_shipping_minor", 0)),
            apparel_margin_before_reprice_minor=int(apparel_diag.get("after_shipping_margin_before_reprice_minor", 0)),
            apparel_margin_after_reprice_minor=int(apparel_diag.get("after_shipping_margin_after_reprice_minor", 0)),
            apparel_max_allowed_price_minor=int(apparel_diag.get("max_allowed_price_minor", 0)),
            apparel_failed_variant_reasons=json.dumps(failed_variant_reasons, sort_keys=True),
            apparel_failure_reason_counts=json.dumps(failure_reason_counts, sort_keys=True),
            option_names=json.dumps(option_diagnostics.option_names, sort_keys=True),
            option_values_summary=json.dumps(option_diagnostics.option_values_summary, sort_keys=True),
            requested_option_filters=json.dumps(option_diagnostics.requested_option_filters, sort_keys=True),
            filter_counts=json.dumps(option_diagnostics.filter_counts, sort_keys=True),
            zero_selection_reason=str(option_diagnostics.zero_selection_reason or ""),
            intended_family=family_validation.intended_family,
            resolved_blueprint_title=resolved_blueprint_title,
            resolved_provider_title=resolved_provider_title,
            template_hint_blueprint_id=template_hint_blueprint_id,
            template_hint_provider_id=template_hint_provider_id,
            runtime_mapping_overrode_hint=runtime_mapping_overrode_hint,
            catalog_discovery_used=bool(resolution_diag.discovery_used),
            pinned_mapping_attempted_first=bool(resolution_diag.pinned_attempted_first),
            fallback_discovery_triggered=bool(resolution_diag.fallback_discovery_triggered),
            fallback_discovery_reason=str(resolution_diag.fallback_discovery_reason or ""),
            catalog_resolution_mode=str(resolution_diag.discovery_mode or "normal"),
            resolved_model_dimension=str(option_diagnostics.resolved_model_dimension or ""),
            requested_model_overlap_count=int(option_diagnostics.requested_model_overlap_count or 0),
            fallback_model_set_applied=bool(option_diagnostics.fallback_model_set_applied),
            final_selected_models=json.dumps(option_diagnostics.final_selected_models, sort_keys=True),
            high_resolution_family=bool(template.high_resolution_family),
        )
    except TemplateValidationError as exc:
        return _failure("invalid_template_config", str(exc))
    except NonRetryableRequestError as exc:
        classification = "invalid_template_config" if "HTTP 404" in str(exc) else "runtime_api_failure"
        return _failure(classification, str(exc))
    except Exception as exc:
        return _failure("runtime_api_failure", str(exc))


def preflight_active_templates(
    *,
    printify: PrintifyClient,
    templates: List[ProductTemplate],
    explicit_template_keys: Optional[List[str]] = None,
) -> Tuple[List[ProductTemplate], List[TemplatePreflightIssue], List[TemplatePreflightReportRow]]:
    if not all(hasattr(printify, method) for method in ("list_blueprints", "list_print_providers", "list_variants")):
        logger.warning("Template preflight skipped because Printify client does not expose catalog inspection methods.")
        rows = [
            TemplatePreflightReportRow(
                template_key=template.key,
                requested_explicitly=False,
                preflight_status="skipped",
                classification="preflight_skipped",
                message="Printify client does not expose catalog inspection methods.",
                blueprint_id=template.printify_blueprint_id,
                provider_id=template.printify_print_provider_id,
                selected_count=0,
                repriced_count=0,
                disabled_count_after_reprice=0,
                final_enabled_count=0,
                recommended_action="deactivate_pending_validation",
            )
            for template in templates
        ]
        return templates, [], rows
    explicit = {key.strip() for key in (explicit_template_keys or []) if key.strip()}
    blueprints = printify.list_blueprints()
    blueprint_ids = {int(blueprint.get("id", 0)) for blueprint in blueprints}
    blueprint_titles = {int(blueprint.get("id", 0)): str(blueprint.get("title") or "") for blueprint in blueprints}
    passed: List[ProductTemplate] = []
    issues: List[TemplatePreflightIssue] = []
    report_rows: List[TemplatePreflightReportRow] = []
    for template in templates:
        issue, row = _preflight_template(
            printify=printify,
            template=template,
            blueprint_ids=blueprint_ids,
            blueprint_titles=blueprint_titles,
        )
        row.requested_explicitly = template.key in explicit
        report_rows.append(row)
        if issue is None:
            passed.append(template)
            continue
        issue.requested_explicitly = template.key in explicit
        issues.append(issue)
        logger.warning(
            "Template preflight failed template=%s classification=%s requested_explicitly=%s message=%s",
            template.key,
            issue.classification,
            issue.requested_explicitly,
            issue.message,
        )
    return passed, issues, report_rows


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
    guarded_variants, margin_report = apply_variant_margin_guardrails(template, variant_rows)
    if margin_report["repriced_variant_ids"]:
        logger.info(
            "Variant margin guardrails repriced template=%s variants=%s",
            template.key,
            margin_report["repriced_variant_ids"],
        )
    if margin_report["disabled_variant_ids"]:
        logger.warning(
            "Variant margin guardrails disabled template=%s variants=%s",
            template.key,
            margin_report["disabled_variant_ids"],
        )
    if not guarded_variants:
        skip_status = "skipped_nonviable" if template.mark_template_nonviable_if_needed else "disabled_by_guardrails"
        skip_reason = "longsleeve_nonviable_after_shipping" if template.key == "longsleeve_gildan" else f"no_enabled_variants_after_guardrails:{template.key}"
        logger.warning(
            "Skipping payload build template=%s status=%s reason=%s selected_count=%s repriced_count=%s disabled_count_after_reprice=%s final_enabled_count=%s",
            template.key,
            skip_status,
            skip_reason,
            margin_report.get("selected_count", 0),
            margin_report.get("repriced_count", 0),
            margin_report.get("disabled_count_after_reprice", 0),
            margin_report.get("final_enabled_count", 0),
        )
        raise TemplateSkipGuardrail(skip_status, skip_reason, margin_report)

    variants_payload: List[Dict[str, Any]] = []
    enabled_variant_ids: List[int] = []
    for variant in guarded_variants:
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
    resolved_placements = _resolve_template_placements(template, for_publish=True) or list(template.placements)
    for placement in resolved_placements:
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


def _stable_json_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _build_rerun_fingerprints(
    *,
    artwork: Artwork,
    template: ProductTemplate,
    payload: Dict[str, Any],
) -> Dict[str, str]:
    enabled_variant_ids = sorted(
        int(variant.get("id"))
        for variant in payload.get("variants", [])
        if isinstance(variant, dict) and variant.get("is_enabled", True) and variant.get("id") is not None
    )
    print_area_rows: List[Dict[str, Any]] = []
    for area in payload.get("print_areas", []) if isinstance(payload.get("print_areas", []), list) else []:
        if not isinstance(area, dict):
            continue
        area_variant_ids = sorted(int(variant_id) for variant_id in (area.get("variant_ids") or []))
        placeholders: List[Dict[str, Any]] = []
        for placeholder in area.get("placeholders", []) if isinstance(area.get("placeholders", []), list) else []:
            if not isinstance(placeholder, dict):
                continue
            normalized_images: List[Dict[str, Any]] = []
            for image in placeholder.get("images", []) if isinstance(placeholder.get("images", []), list) else []:
                if not isinstance(image, dict):
                    continue
                normalized_images.append(
                    {
                        "id": str(image.get("id") or ""),
                        "x": float(image.get("x") or 0.0),
                        "y": float(image.get("y") or 0.0),
                        "scale": float(image.get("scale") or 0.0),
                        "angle": float(image.get("angle") or 0.0),
                    }
                )
            placeholders.append({"position": str(placeholder.get("position") or ""), "images": normalized_images})
        print_area_rows.append({"variant_ids": area_variant_ids, "placeholders": placeholders})

    material_shape = {
        "template_key": template.key,
        "artwork_slug": artwork.slug,
        "blueprint_id": int(payload.get("blueprint_id") or 0),
        "print_provider_id": int(payload.get("print_provider_id") or 0),
        "enabled_variant_ids": enabled_variant_ids,
        "print_areas": print_area_rows,
    }
    mutable_listing = {
        "title": str(payload.get("title") or ""),
        "description": str(payload.get("description") or ""),
        "tags": sorted(str(tag) for tag in (payload.get("tags") or [])),
        "variants": sorted(
            (
                int(variant.get("id") or 0),
                int(variant.get("price") or 0),
                int(variant.get("compare_at_price") or 0),
            )
            for variant in payload.get("variants", [])
            if isinstance(variant, dict)
        ),
    }
    return {
        "material_fingerprint": _stable_json_hash(material_shape),
        "update_fingerprint": _stable_json_hash({"material_shape": material_shape, "mutable_listing": mutable_listing}),
    }


def _extract_prior_rerun_fingerprints(prior_state_row: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not isinstance(prior_state_row, dict):
        return {"material_fingerprint": "", "update_fingerprint": ""}
    candidates: List[Dict[str, Any]] = []
    if isinstance(prior_state_row.get("rerun_fingerprints"), dict):
        candidates.append(prior_state_row.get("rerun_fingerprints", {}))
    row_result = prior_state_row.get("result", {}) if isinstance(prior_state_row.get("result"), dict) else {}
    printify_result = row_result.get("printify", {}) if isinstance(row_result.get("printify"), dict) else {}
    if isinstance(printify_result.get("rerun_fingerprints"), dict):
        candidates.append(printify_result.get("rerun_fingerprints", {}))
    for candidate in candidates:
        material_fingerprint = str(candidate.get("material_fingerprint") or "").strip()
        update_fingerprint = str(candidate.get("update_fingerprint") or "").strip()
        if material_fingerprint or update_fingerprint:
            return {
                "material_fingerprint": material_fingerprint,
                "update_fingerprint": update_fingerprint,
            }
    return {"material_fingerprint": "", "update_fingerprint": ""}

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
    required_signal = _title_product_signal_label(template)
    if required_signal and not title_semantically_includes_product_label(normalized_title, required_signal):
        family = content_engine.infer_product_family(template)
        if family in {"tshirt", "long_sleeve", "hoodie", "sweatshirt", "mug", "poster", "tote"}:
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
    if len(tags) >= 15:
        warnings.append("tag_count_high")
    elif len(tags) >= 13:
        warnings.append("tag_count_near_limit")
    generic = {"print-on-demand", "printify", "gift", "style", "inkvibe", "apparel", "shirt", "clothing"}
    non_generic = [tag for tag in deduped if tag not in generic]
    if not non_generic:
        warnings.append("tags_generic_only")
    family_tags = set(content_engine.family_tags(template))
    if family_tags and not family_tags.intersection(deduped):
        warnings.append("tags_missing_family_signal")
    if not _tags_contain_theme_signal(tags=deduped, artwork=artwork, template=template):
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
    expected_dimensions = _qa_option_dimension_labels(template, variant_rows)
    for _, expected_label in expected_dimensions:
        if expected_label not in option_names:
            errors.append(f"options_missing_{expected_label.lower()}_name")
    if expected_dimensions and "Title" in option_names:
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
    return warnings, errors


def _derive_copy_provenance_for_qa(*, artwork: Artwork, template: ProductTemplate) -> Tuple[str, str]:
    metadata = artwork.metadata or {}
    configured_reason = str(metadata.get("ai_product_copy_cache_reason") or "").strip()
    bucket = metadata.get("ai_product_copy")
    if not isinstance(bucket, dict) or not bucket:
        return "deterministic_fallback", configured_reason or "cache_bucket_missing"
    template_key_fragment = f":{template.key}:"
    for key in bucket.keys():
        if isinstance(key, str) and template_key_fragment in key:
            return "ai_product_copy_cache", configured_reason or "cache_entry_present"
    return "ai_product_copy_cache_other_template", configured_reason or "cache_entry_not_template_scoped"


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
    organization = build_normalized_shopify_organization(template, artwork)
    product_options, variant_payloads = build_shopify_product_options(template, variant_rows)
    publish_payload = build_printify_publish_payload(template)
    placement_bits: List[str] = []
    for placement in (_resolve_template_placements(template, for_publish=True) or list(template.placements)):
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
    copy_provenance, copy_cache_reason = _derive_copy_provenance_for_qa(artwork=artwork, template=template)

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
        family=str(organization.get("family_key") or ""),
        product_type=resolve_shopify_product_type(template),
        department_key=str(organization.get("department_key") or ""),
        department_label=str(organization.get("department_label") or ""),
        primary_collection_handle=str(organization.get("primary_collection_handle") or ""),
        primary_collection_title=str(organization.get("primary_collection_title") or ""),
        recommended_manual_collections=", ".join(organization.get("recommended_manual_collections", [])),
        recommended_smart_collection_tags=", ".join(organization.get("recommended_smart_collection_tags", [])),
        normalized_theme_keys=", ".join(organization.get("normalized_theme_keys", [])),
        normalized_audience_keys=", ".join(organization.get("normalized_audience_keys", [])),
        normalized_season_keys=", ".join(organization.get("normalized_season_keys", [])),
        metadata_resolution_source=artwork.metadata_resolution_source,
        metadata_generated_inline=artwork.metadata_generated_inline,
        metadata_sidecar_written=artwork.metadata_sidecar_written,
        copy_provenance=copy_provenance,
        ai_product_copy_cache_reason=copy_cache_reason,
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
                metadata=sanitize_metadata_for_publish(metadata),
            )
            template = build_resolved_template(template_by_key[launch_row.template_key], launch_row.overrides)
            resolved_template = select_provider_for_template(printify=printify, template=template)
            variant_rows = choose_variants_from_catalog(
                printify.list_variants(resolved_template.printify_blueprint_id, resolved_template.printify_print_provider_id),
                resolved_template,
            )
            qa_rows.append(
                build_storefront_qa_row(
                    artwork=artwork,
                    template=resolved_template,
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
                resolved_template = select_provider_for_template(printify=printify, template=template)
                variant_rows = choose_variants_from_catalog(
                    printify.list_variants(resolved_template.printify_blueprint_id, resolved_template.printify_print_provider_id),
                    resolved_template,
                )
                qa_rows.append(build_storefront_qa_row(artwork=artwork, template=resolved_template, variant_rows=variant_rows))

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


def log_runtime_skip_diagnostics(diag: RuntimeSkipDiagnostics) -> None:
    logger.warning(
        "Runtime skip diagnostics template=%s blueprint_id=%s provider_id=%s selected_count=%s final_enabled_count=%s available_placements=%s required_placement=%s print_area_available=%s upload_map=%s payload_build_skip_reason=%s resolved_option_dimensions=%s resolved_model_list=%s final_reason_code=%s",
        diag.template_key,
        diag.blueprint_id,
        diag.provider_id,
        diag.selected_count,
        diag.final_enabled_count,
        diag.available_placements,
        diag.required_placement_name or "-",
        diag.print_area_available,
        diag.upload_map,
        diag.payload_build_skip_reason or "-",
        diag.resolved_option_dimensions,
        diag.resolved_model_list,
        diag.final_reason_code or "-",
    )


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
    defer_publish: bool = False,
    state: Optional[Dict[str, Any]] = None,
    auto_rebuild_on_incompatible_update: bool = False,
    prior_state_row: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    def _execute_create() -> Tuple[str, Dict[str, Any]]:
        try:
            created_resp = printify.create_product(shop_id, payload)
        except DryRunMutationSkipped:
            raise
        new_product_id = str(created_resp.get("id") or created_resp.get("data", {}).get("id") or "")
        return new_product_id, {"action": "create", "printify_product_id": new_product_id, "created": created_resp}

    def _execute_rebuild(*, previous_product_id: str) -> Tuple[str, Dict[str, Any]]:
        try:
            printify.delete_product(shop_id, previous_product_id)
        except DryRunMutationSkipped:
            raise
        except Exception:
            logger.warning("Delete existing product failed during rebuild product_id=%s", previous_product_id)
        new_product_id, create_result = _execute_create()
        rebuilt = {
            "action": "rebuild",
            "previous_product_id": previous_product_id,
            "printify_product_id": new_product_id,
            "created": create_result["created"],
        }
        return new_product_id, rebuilt

    try:
        payload = build_printify_product_payload(artwork, template, variant_rows, upload_map)
    except TemplateSkipGuardrail as exc:
        return {
            "status": exc.status,
            "action": "skip",
            "reason": exc.reason,
            "guardrail_report": exc.margin_report,
            "printify_product_id": existing_product_id or "",
        }
    payload_stats = validate_printify_payload_consistency(payload)
    rerun_fingerprints = _build_rerun_fingerprints(artwork=artwork, template=template, payload=payload)
    prior_fingerprints = _extract_prior_rerun_fingerprints(prior_state_row)
    enforce_variant_safety_limit(template=template, enabled_variant_count=payload_stats["enabled_variant_count"])
    logger.info("Mockup/image publish behavior template=%s publish_images=%s publish_mockups_override=%s", template.key, template.publish_images, template.publish_mockups)

    if action == "skip":
        return {
            "status": "skipped",
            "action": "skip",
            "printify_product_id": existing_product_id or "",
            "rerun_fingerprints": rerun_fingerprints,
        }

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
            if prior_fingerprints["update_fingerprint"] and prior_fingerprints["update_fingerprint"] == rerun_fingerprints["update_fingerprint"]:
                logger.info(
                    "Rerun classification template=%s product_id=%s decision=skip_noop reason=unchanged_fingerprint",
                    template.key,
                    existing_product_id,
                )
                return {
                    "status": "skipped",
                    "action": "skip",
                    "reason": "rerun_noop_unchanged_fingerprint",
                    "printify_product_id": existing_product_id,
                    "rerun_fingerprints": rerun_fingerprints,
                    "rerun_decision": {"decision": "skip", "reason": "unchanged_fingerprint"},
                }

            if prior_fingerprints["material_fingerprint"] and prior_fingerprints["material_fingerprint"] == rerun_fingerprints["material_fingerprint"]:
                logger.info(
                    "Rerun classification template=%s product_id=%s decision=update reason=mutable_listing_changed",
                    template.key,
                    existing_product_id,
                )
            elif prior_fingerprints["material_fingerprint"]:
                logger.info(
                    "Rerun classification template=%s product_id=%s decision=update reason=material_shape_changed",
                    template.key,
                    existing_product_id,
                )
            else:
                logger.info(
                    "Rerun classification template=%s product_id=%s decision=update reason=no_prior_fingerprint",
                    template.key,
                    existing_product_id,
                )
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
                disabled_edit_retry_count = 0
                try:
                    while True:
                        try:
                            updated = printify.update_product(shop_id, existing_product_id, payload)
                            break
                        except NonRetryableRequestError as exc:
                            if _is_printify_product_edit_disabled_error(exc):
                                logger.warning(
                                    "Update blocked by Printify edit lock; classifying as temporary disabled-edit state "
                                    "product_id=%s template=%s code_hint=8252 retry=%s/%s",
                                    existing_product_id,
                                    template.key,
                                    disabled_edit_retry_count + 1,
                                    PRINTIFY_UPDATE_DISABLED_RETRY_ATTEMPTS + 1,
                                )
                                if disabled_edit_retry_count < PRINTIFY_UPDATE_DISABLED_RETRY_ATTEMPTS:
                                    disabled_edit_retry_count += 1
                                    logger.info(
                                        "Retrying Printify update after disabled-edit classification product_id=%s template=%s "
                                        "retry_attempt=%s wait_seconds=%.2f",
                                        existing_product_id,
                                        template.key,
                                        disabled_edit_retry_count,
                                        PRINTIFY_UPDATE_DISABLED_RETRY_SLEEP_SECONDS,
                                    )
                                    if PRINTIFY_UPDATE_DISABLED_RETRY_SLEEP_SECONDS > 0:
                                        time.sleep(PRINTIFY_UPDATE_DISABLED_RETRY_SLEEP_SECONDS)
                                    continue
                                logger.warning(
                                    "Disabled-edit update remained blocked after retry; rebuild fallback activated "
                                    "product_id=%s template=%s fallback_action=rebuild retries=%s",
                                    existing_product_id,
                                    template.key,
                                    disabled_edit_retry_count,
                                )
                                action = "rebuild"
                                try:
                                    product_id, result = _execute_rebuild(previous_product_id=existing_product_id)
                                except DryRunMutationSkipped:
                                    return {
                                        "status": "dry-run",
                                        "action": "rebuild",
                                        "payload_preview": payload,
                                        "printify_product_id": existing_product_id,
                                    }
                                break
                            if not _is_printify_update_incompatible_error(exc):
                                raise
                            coverage_gap = max(0, payload_stats["enabled_variant_count"] - payload_stats["print_area_variant_count"])
                            logger.warning(
                                "Update rejected as incompatible by Printify; classifying as update incompatibility "
                                "product_id=%s template=%s code_hint=8251 enabled_variant_count=%s print_area_variant_count=%s coverage_gap=%s",
                                existing_product_id,
                                template.key,
                                payload_stats["enabled_variant_count"],
                                payload_stats["print_area_variant_count"],
                                coverage_gap,
                            )
                            logger.warning(
                                "Incompatible update fallback activated product_id=%s template=%s fallback_action=rebuild",
                                existing_product_id,
                                template.key,
                            )
                            action = "rebuild"
                            try:
                                product_id, result = _execute_rebuild(previous_product_id=existing_product_id)
                            except DryRunMutationSkipped:
                                return {"status": "dry-run", "action": "rebuild", "payload_preview": payload, "printify_product_id": existing_product_id}
                            break
                except NonRetryableRequestError as exc:
                    raise
                except DryRunMutationSkipped:
                    return {"status": "dry-run", "action": "update", "payload_preview": payload, "printify_product_id": existing_product_id}
                else:
                    if action == "update":
                        result = {"action": "update", "printify_product_id": existing_product_id, "updated": updated}
                        product_id = existing_product_id
            else:
                try:
                    product_id, result = _execute_rebuild(previous_product_id=existing_product_id)
                except DryRunMutationSkipped:
                    return {"status": "dry-run", "action": "rebuild", "payload_preview": payload, "printify_product_id": existing_product_id}
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
    should_publish = should_publish and not defer_publish
    result["publish_attempted"] = False
    result["publish_verified"] = False
    result["publish_outcome"] = "create_success_publish_deferred" if defer_publish else "create_success_publish_success"
    result["publish_queue_status"] = ""
    result["rerun_fingerprints"] = rerun_fingerprints

    if defer_publish and product_id and state is not None:
        enqueue_publish_pending(
            state=state,
            artwork_key=artwork.slug,
            template_key=template.key,
            shop_id=shop_id,
            product_id=product_id,
            publish_status="pending",
            reason_code="deferred_publish",
        )
        result["publish_queue_status"] = "pending"
    elif should_publish:
        result["publish_attempted"] = True
        try:
            result["published"] = printify.publish_product(shop_id, product_id, build_printify_publish_payload(template))
            logger.info("Printify publish completed product_id=%s", product_id)
            result["publish_outcome"] = "create_success_publish_success"
            result["publish_queue_status"] = "completed"
        except DryRunMutationSkipped:
            result["published"] = {"status": "dry-run"}
        except RetryLimitExceededError as exc:
            if exc.reason_code == "publish_rate_limited" and state is not None:
                enqueue_publish_pending(
                    state=state,
                    artwork_key=artwork.slug,
                    template_key=template.key,
                    shop_id=shop_id,
                    product_id=product_id,
                    publish_status="pending_retry",
                    reason_code=exc.reason_code,
                    last_error=str(exc),
                )
                result["publish_outcome"] = "create_success_publish_rate_limited"
                result["publish_queue_status"] = "pending_retry"
                result["publish_error"] = str(exc)
            else:
                raise

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


def process_artwork(*, printify: PrintifyClient, shopify: Optional[ShopifyClient], shop_id: Optional[int], artwork: Artwork, templates: List[ProductTemplate], state: Dict[str, Any], force: bool, export_dir: pathlib.Path, state_path: pathlib.Path, artwork_options: ArtworkProcessingOptions, upload_strategy: str, r2_config: Optional[R2Config], create_only: bool = False, update_only: bool = False, rebuild_product: bool = False, publish_mode: str = "default", verify_publish: bool = False, defer_publish: bool = False, auto_rebuild_on_incompatible_update: bool = False, sync_collections: bool = False, verify_collections: bool = False, summary: Optional[RunSummary] = None, failure_rows: Optional[List[FailureReportRow]] = None, run_rows: Optional[List[RunReportRow]] = None, launch_plan_row: str = "", launch_plan_row_id: str = "", collection_handle: str = "", collection_title: str = "", collection_description: str = "", launch_name: str = "", campaign: str = "", merch_theme: str = "", routed_asset_family: str = "", routed_asset_mode: str = "", enforce_family_collection_membership: bool = True, collection_removal_mode: str = "conservative", collection_sort_order: str = "", collection_image_src: str = "", secondary_collection_handles: str = "", progress: Optional[RunProgressTracker] = None) -> None:
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
    blueprint_titles_cache: Dict[int, str] = {}
    try:
        blueprint_titles_cache = {
            int(blueprint.get("id", 0)): str(blueprint.get("title") or "")
            for blueprint in printify.list_blueprints()
            if int(blueprint.get("id", 0)) > 0
        }
    except Exception:
        logger.warning("Runtime blueprint title lookup failed; family validation will rely on option schema only.")
    for template in templates:
        if progress is not None:
            progress.update(artwork=artwork.src_path.name, template=template.key, stage="prepare")
        title_info = resolve_artwork_title(template, artwork)
        artwork.final_title_source = title_info.title_source
        rendered_title = render_product_title(template, artwork)
        state_key = f"{artwork.slug}:{template.key}"
        matching_rows = [row for row in record["products"] if isinstance(row, dict) and row.get("state_key") == state_key]
        existing_product_id = ""
        latest_success_row: Optional[Dict[str, Any]] = None
        for row in reversed(matching_rows):
            row_result = row.get("result", {}) if isinstance(row.get("result"), dict) else {}
            printify_row = row_result.get("printify", {}) if isinstance(row_result.get("printify"), dict) else {}
            candidate = printify_row.get("printify_product_id")
            if isinstance(candidate, str) and candidate.strip() and row_result.get("error") is None:
                existing_product_id = candidate.strip()
                latest_success_row = row if isinstance(row, dict) else None
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
        template_family_report = content_engine.infer_product_family(template)
        family_label_report = content_engine.family_title_suffix(template)
        tote_primary_placement_report = ""
        tote_active_placements_report = ""
        poster_cover_eligible_report = ""
        poster_enhancement_status_report = ""
        poster_enhancement_tier_report = ""
        poster_source_ratio_report = ""
        poster_requested_upscale_factor_report = ""
        poster_applied_upscale_factor_report = ""
        poster_fill_optimization_used_report = False
        try:
            resolved_template = template
            if action == "skip":
                if create_only and existing_product_id:
                    skip_reason_code = "existing_product_create_only_skip"
                elif update_only and not existing_product_id:
                    skip_reason_code = "missing_existing_product_update_only_skip"
                else:
                    skip_reason_code = "action_resolved_skip"
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
                        reason_code=skip_reason_code,
                        launch_plan_row=launch_plan_row,
                        launch_plan_row_id=launch_plan_row_id,
                        routed_asset_family=routed_asset_family,
                        routed_asset_mode=routed_asset_mode,
                        template_family=template_family_report,
                        product_family_label=family_label_report,
                        tote_primary_placement=tote_primary_placement_report,
                        tote_active_placements=tote_active_placements_report,
                        poster_cover_eligible=poster_cover_eligible_report,
                        poster_enhancement_status=poster_enhancement_status_report,
                        poster_enhancement_tier=poster_enhancement_tier_report,
                        poster_source_ratio=poster_source_ratio_report,
                        poster_requested_upscale_factor=poster_requested_upscale_factor_report,
                        poster_applied_upscale_factor=poster_applied_upscale_factor_report,
                        poster_fill_optimization_used=poster_fill_optimization_used_report,
                        metadata_resolution_source=artwork.metadata_resolution_source,
                        metadata_generated_inline=artwork.metadata_generated_inline,
                        metadata_sidecar_written=artwork.metadata_sidecar_written,
                        weak_metadata_detected="|".join(artwork.weak_metadata_detected),
                        final_title_source=title_info.title_source,
                    ))
                log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=True, result=result, blueprint_id=template.printify_blueprint_id, provider_id=template.printify_print_provider_id, action="skip", upload_map=upload_map)
                continue

            provider_title = ""
            if template.key == "tote_basic":
                resolved_template, catalog_variants = resolve_tote_template_catalog_mapping(
                    printify=printify,
                    template=template,
                )
            else:
                resolved_template, resolution_diag = _resolve_template_catalog_mapping(
                    printify=printify,
                    template=template,
                    discovery_mode="normal",
                )
                try:
                    providers = printify.list_print_providers(resolved_template.printify_blueprint_id)
                    provider_title = next(
                        (
                            str(provider.get("title") or provider.get("name") or "")
                            for provider in providers
                            if int(provider.get("id", 0)) == resolved_template.printify_print_provider_id
                        ),
                        "",
                    )
                except Exception:
                    provider_title = ""
                catalog_variants = printify.list_variants(resolved_template.printify_blueprint_id, resolved_template.printify_print_provider_id)
                logger.info(
                    "Runtime catalog resolution template=%s template_hint_blueprint_id=%s template_hint_provider_id=%s resolved_blueprint_id=%s resolved_provider_id=%s discovery_used=%s pinned_attempted_first=%s fallback_discovery_triggered=%s fallback_discovery_reason=%s",
                    template.key,
                    resolution_diag.template_hint_blueprint_id,
                    resolution_diag.template_hint_provider_id,
                    resolution_diag.resolved_blueprint_id,
                    resolution_diag.resolved_provider_id,
                    resolution_diag.discovery_used,
                    resolution_diag.pinned_attempted_first,
                    resolution_diag.fallback_discovery_triggered,
                    resolution_diag.fallback_discovery_reason or "-",
                )
            normalized_catalog_variants = normalize_catalog_variants_response(catalog_variants)
            family_validation = validate_catalog_family_schema(template=resolved_template, variants=normalized_catalog_variants)
            resolved_blueprint_title = str(blueprint_titles_cache.get(resolved_template.printify_blueprint_id, ""))
            if resolved_blueprint_title or provider_title:
                family_validation = validate_catalog_family_schema(
                    template=resolved_template,
                    variants=normalized_catalog_variants,
                    blueprint_title=resolved_blueprint_title,
                    provider_title=provider_title,
                )
            if not family_validation.plausible:
                runtime_diag = RuntimeSkipDiagnostics(
                    template_key=template.key,
                    blueprint_id=resolved_template.printify_blueprint_id,
                    provider_id=resolved_template.printify_print_provider_id,
                    available_placements=summarize_variant_options(normalized_catalog_variants).get("placements", []),
                    required_placement_name=str((resolved_template.preferred_primary_placement or (resolved_template.placements[0].placement_name if resolved_template.placements else ""))),
                    final_reason_code="catalog_family_mismatch_runtime",
                    payload_build_skip_reason=family_validation.reason,
                )
                runtime_diag.print_area_available = bool(
                    runtime_diag.required_placement_name
                    and runtime_diag.required_placement_name in set(runtime_diag.available_placements)
                )
                log_runtime_skip_diagnostics(runtime_diag)
                all_templates_successful = False
                if summary is not None:
                    summary.products_skipped += 1
                result = {
                    "status": "catalog_family_mismatch",
                    "failure_classification": "wrong_catalog_family",
                    "intended_family": family_validation.intended_family,
                    "reason": family_validation.reason,
                    "runtime_skip_reason_code": runtime_diag.final_reason_code,
                    "runtime_skip_diagnostics": asdict(runtime_diag),
                }
                record["products"].append({"template": template.key, "state_key": state_key, "title_source": title_info.title_source, "rendered_title": rendered_title, "result": result})
                if run_rows is not None:
                    run_rows.append(
                        RunReportRow(
                            datetime.now(timezone.utc).isoformat(),
                            artwork.src_path.name,
                            artwork.slug,
                            template.key,
                            "skipped",
                            "skip",
                            resolved_template.printify_blueprint_id,
                            resolved_template.printify_print_provider_id,
                            upload_strategy,
                            "",
                            False,
                            False,
                            rendered_title,
                            reason_code=runtime_diag.final_reason_code or "no_matching_variants_runtime",
                            orientation_bucket=orientation_report,
                            launch_plan_row=launch_plan_row,
                            launch_plan_row_id=launch_plan_row_id,
                            collection_handle=collection_handle,
                            collection_title=collection_title,
                            collection_description=collection_description,
                            launch_name=launch_name,
                            campaign=campaign,
                            merch_theme=merch_theme,
                            eligibility_outcome="ineligible",
                            eligibility_reason_code=runtime_diag.final_reason_code or "no_matching_variants_runtime",
                            eligibility_rule_failed="variant_selection",
                            eligibility_gate_stage="variant_gate",
                        )
                    )
                log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": result}, blueprint_id=resolved_template.printify_blueprint_id, provider_id=resolved_template.printify_print_provider_id, action="skip", upload_map=upload_map)
                continue
            variant_rows, variant_diagnostics = choose_variants_from_catalog_with_diagnostics(normalized_catalog_variants, resolved_template)
            variant_rows = reorder_variants_for_storefront_display(
                template=resolved_template,
                artwork=artwork,
                variant_rows=variant_rows,
            )
            if not variant_rows:
                zero_classification = _classify_zero_selection(
                    resolved_template,
                    variant_diagnostics,
                    intended_family=family_validation.intended_family,
                )
                runtime_diag = RuntimeSkipDiagnostics(
                    template_key=template.key,
                    blueprint_id=resolved_template.printify_blueprint_id,
                    provider_id=resolved_template.printify_print_provider_id,
                    selected_count=0,
                    final_enabled_count=0,
                    available_placements=summarize_variant_options(normalized_catalog_variants).get("placements", []),
                    required_placement_name=str((resolved_template.preferred_primary_placement or (resolved_template.placements[0].placement_name if resolved_template.placements else ""))),
                    final_reason_code="no_matching_variants_runtime",
                    payload_build_skip_reason=variant_diagnostics.zero_selection_reason or "no_matching_variants",
                    resolved_option_dimensions={"model": variant_diagnostics.resolved_model_dimension},
                    resolved_model_list=list(variant_diagnostics.final_selected_models),
                )
                runtime_diag.print_area_available = bool(
                    runtime_diag.required_placement_name
                    and runtime_diag.required_placement_name in set(runtime_diag.available_placements)
                )
                log_runtime_skip_diagnostics(runtime_diag)
                all_templates_successful = False
                if summary is not None:
                    summary.products_skipped += 1
                result = {
                    "status": "no_matching_variants",
                    "failure_classification": zero_classification,
                    "runtime_skip_reason_code": runtime_diag.final_reason_code,
                    "runtime_skip_diagnostics": asdict(runtime_diag),
                }
                record["products"].append({"template": template.key, "state_key": state_key, "title_source": title_info.title_source, "rendered_title": rendered_title, "result": result})
                if run_rows is not None:
                    run_rows.append(
                        RunReportRow(
                            datetime.now(timezone.utc).isoformat(),
                            artwork.src_path.name,
                            artwork.slug,
                            template.key,
                            "skipped",
                            "skip",
                            resolved_template.printify_blueprint_id,
                            resolved_template.printify_print_provider_id,
                            upload_strategy,
                            "",
                            False,
                            False,
                            rendered_title,
                            reason_code=runtime_diag.final_reason_code or "no_matching_variants_runtime",
                            orientation_bucket=orientation_report,
                            launch_plan_row=launch_plan_row,
                            launch_plan_row_id=launch_plan_row_id,
                            collection_handle=collection_handle,
                            collection_title=collection_title,
                            collection_description=collection_description,
                            launch_name=launch_name,
                            campaign=campaign,
                            merch_theme=merch_theme,
                            routed_asset_family=routed_asset_family,
                            routed_asset_mode=routed_asset_mode,
                            template_family=template_family_report,
                            product_family_label=family_label_report,
                            eligibility_outcome="ineligible",
                            eligibility_reason_code=runtime_diag.final_reason_code or "no_matching_variants_runtime",
                            eligibility_rule_failed="variant_selection",
                            eligibility_gate_stage="variant_gate",
                        )
                    )
                log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": result}, blueprint_id=resolved_template.printify_blueprint_id, provider_id=resolved_template.printify_print_provider_id, action="skip", upload_map=upload_map)
                continue

            prepared_assets: List[PreparedArtwork] = []
            skipped_placements: List[str] = []
            insufficient_artwork_error: Optional[InsufficientArtworkResolutionError] = None
            eligibility_result: Optional[ArtworkEligibilityResult] = None
            failed_eligibility_placement_name = ""
            resolved_placements = _resolve_template_placements(resolved_template, for_publish=True) or list(resolved_template.placements)
            if resolved_template.key == "tote_basic":
                tote_primary_placement_report = str(resolved_template.preferred_primary_placement or "")
                tote_active_placements_report = ",".join([p.placement_name for p in resolved_placements])
                logger.info(
                    "Tote placement plan template=%s preferred_primary=%s active=%s publish_only_primary=%s",
                    resolved_template.key,
                    tote_primary_placement_report or "-",
                    tote_active_placements_report or "-",
                    resolved_template.publish_only_primary_placement,
                )
            for placement in resolved_placements:
                eligibility_result = evaluate_artwork_eligibility_for_template(
                    artwork=artwork,
                    template=resolved_template,
                    placement=placement,
                )
                if not eligibility_result.eligible:
                    placement_allows_upscale = artwork_options.allow_upscale or placement.allow_upscale
                    if (
                        artwork_options.auto_wallart_master
                        and placement_allows_upscale
                        and resolved_template.key in WALLART_AUTO_MASTER_TEMPLATE_KEYS
                        and eligibility_result.reason_code == "insufficient_artwork_resolution"
                    ):
                        logger.info(
                            "Bypassing eligibility gate for wall-art derived master template=%s placement=%s source=%sx%s required=%sx%s",
                            resolved_template.key,
                            placement.placement_name,
                            eligibility_result.source_size[0],
                            eligibility_result.source_size[1],
                            eligibility_result.required_size[0],
                            eligibility_result.required_size[1],
                        )
                        eligibility_result = None
                        continue
                    failed_eligibility_placement_name = placement.placement_name
                    break
            if eligibility_result is not None and not eligibility_result.eligible:
                runtime_diag = RuntimeSkipDiagnostics(
                    template_key=template.key,
                    blueprint_id=resolved_template.printify_blueprint_id,
                    provider_id=resolved_template.printify_print_provider_id,
                    selected_count=len(variant_rows),
                    final_enabled_count=len(variant_rows),
                    available_placements=[p.placement_name for p in resolved_placements],
                    required_placement_name=failed_eligibility_placement_name,
                    final_reason_code=eligibility_result.reason_code or "artwork_not_eligible_for_template",
                    payload_build_skip_reason=(
                        f"source={eligibility_result.source_size[0]}x{eligibility_result.source_size[1]} "
                        f"required={eligibility_result.required_size[0]}x{eligibility_result.required_size[1]} "
                        f"fit_mode={eligibility_result.fit_mode} "
                        f"rule_failed={eligibility_result.rule_failed}"
                    ).strip(),
                    resolved_option_dimensions={"model": variant_diagnostics.resolved_model_dimension},
                    resolved_model_list=list(variant_diagnostics.final_selected_models),
                )
                runtime_diag.print_area_available = bool(runtime_diag.required_placement_name)
                log_runtime_skip_diagnostics(runtime_diag)
                all_templates_successful = False
                if summary is not None:
                    summary.products_skipped += 1
                result = {
                    "status": "artwork_not_eligible_for_template",
                    "failure_classification": eligibility_result.reason_code or "artwork_not_eligible_for_template",
                    "runtime_skip_reason_code": runtime_diag.final_reason_code,
                    "runtime_skip_stage": "eligibility_gate",
                    "runtime_skip_diagnostics": asdict(runtime_diag),
                    "required_size": f"{eligibility_result.required_size[0]}x{eligibility_result.required_size[1]}",
                    "source_size": f"{eligibility_result.source_size[0]}x{eligibility_result.source_size[1]}",
                }
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
                    run_rows.append(
                        RunReportRow(
                            timestamp=datetime.now(timezone.utc).isoformat(),
                            artwork_filename=artwork.src_path.name,
                            artwork_slug=artwork.slug,
                            template_key=template.key,
                            status="skipped",
                            action="skip",
                            blueprint_id=resolved_template.printify_blueprint_id,
                            provider_id=resolved_template.printify_print_provider_id,
                            upload_strategy=upload_strategy,
                            product_id="",
                            publish_attempted=False,
                            publish_verified=False,
                            rendered_title=rendered_title,
                            reason_code=eligibility_result.reason_code or "artwork_not_eligible_for_template",
                            source_size=f"{eligibility_result.source_size[0]}x{eligibility_result.source_size[1]}",
                            orientation_bucket=orientation_report,
                            launch_plan_row=launch_plan_row,
                            launch_plan_row_id=launch_plan_row_id,
                            collection_handle=collection_handle,
                            collection_title=collection_title,
                            collection_description=collection_description,
                            launch_name=launch_name,
                            campaign=campaign,
                            merch_theme=merch_theme,
                            routed_asset_family=routed_asset_family,
                            routed_asset_mode=routed_asset_mode,
                            template_family=template_family_report,
                            product_family_label=family_label_report,
                            required_placement_size=f"{eligibility_result.required_size[0]}x{eligibility_result.required_size[1]}",
                            required_fit_mode=eligibility_result.fit_mode,
                            eligibility_high_resolution_family=eligibility_result.high_resolution_family,
                            eligibility_outcome="ineligible",
                            eligibility_reason_code=eligibility_result.reason_code or "artwork_not_eligible_for_template",
                            eligibility_rule_failed=eligibility_result.rule_failed,
                            eligibility_gate_stage="eligibility_gate",
                        )
                    )
                log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": result}, blueprint_id=resolved_template.printify_blueprint_id, provider_id=resolved_template.printify_print_provider_id, action="skip", upload_map=upload_map)
                continue

            for placement in resolved_placements:
                try:
                    prepared = prepare_artwork_export(artwork, resolved_template, placement, export_dir, artwork_options)
                except InsufficientArtworkResolutionError as exc:
                    insufficient_artwork_error = exc
                    break
                if prepared is None:
                    skipped_placements.append(placement.placement_name)
                    continue
                prepared_assets.append(prepared)

            if insufficient_artwork_error is not None:
                runtime_diag = RuntimeSkipDiagnostics(
                    template_key=template.key,
                    blueprint_id=resolved_template.printify_blueprint_id,
                    provider_id=resolved_template.printify_print_provider_id,
                    selected_count=len(variant_rows),
                    final_enabled_count=len(variant_rows),
                    available_placements=[p.placement_name for p in resolved_placements],
                    required_placement_name=insufficient_artwork_error.placement_name,
                    final_reason_code="insufficient_artwork_resolution",
                    payload_build_skip_reason=(
                        f"source={insufficient_artwork_error.source_size[0]}x{insufficient_artwork_error.source_size[1]} "
                        f"required={insufficient_artwork_error.required_size[0]}x{insufficient_artwork_error.required_size[1]} "
                        f"fit_mode={insufficient_artwork_error.fit_mode}"
                    ),
                    resolved_option_dimensions={"model": variant_diagnostics.resolved_model_dimension},
                    resolved_model_list=list(variant_diagnostics.final_selected_models),
                )
                runtime_diag.print_area_available = bool(runtime_diag.required_placement_name)
                log_runtime_skip_diagnostics(runtime_diag)
                all_templates_successful = False
                if summary is not None:
                    summary.products_skipped += 1
                result = {
                    "status": "insufficient_artwork_resolution",
                    "failure_classification": "insufficient_artwork_resolution",
                    "runtime_skip_reason_code": runtime_diag.final_reason_code,
                    "runtime_skip_stage": "runtime_processing",
                    "runtime_skip_diagnostics": asdict(runtime_diag),
                    "required_size": f"{insufficient_artwork_error.required_size[0]}x{insufficient_artwork_error.required_size[1]}",
                    "source_size": f"{insufficient_artwork_error.source_size[0]}x{insufficient_artwork_error.source_size[1]}",
                }
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
                    run_rows.append(
                        RunReportRow(
                            timestamp=datetime.now(timezone.utc).isoformat(),
                            artwork_filename=artwork.src_path.name,
                            artwork_slug=artwork.slug,
                            template_key=template.key,
                            status="skipped",
                            action="skip",
                            blueprint_id=resolved_template.printify_blueprint_id,
                            provider_id=resolved_template.printify_print_provider_id,
                            upload_strategy=upload_strategy,
                            product_id="",
                            publish_attempted=False,
                            publish_verified=False,
                            rendered_title=rendered_title,
                            reason_code="insufficient_artwork_resolution",
                            source_size=f"{insufficient_artwork_error.source_size[0]}x{insufficient_artwork_error.source_size[1]}",
                            orientation_bucket=orientation_report,
                            launch_plan_row=launch_plan_row,
                            launch_plan_row_id=launch_plan_row_id,
                            collection_handle=collection_handle,
                            collection_title=collection_title,
                            collection_description=collection_description,
                            launch_name=launch_name,
                            campaign=campaign,
                            merch_theme=merch_theme,
                            routed_asset_family=routed_asset_family,
                            routed_asset_mode=routed_asset_mode,
                            template_family=template_family_report,
                            product_family_label=family_label_report,
                            required_placement_size=f"{insufficient_artwork_error.required_size[0]}x{insufficient_artwork_error.required_size[1]}",
                            required_fit_mode=insufficient_artwork_error.fit_mode,
                            eligibility_high_resolution_family=bool(resolved_template.high_resolution_family),
                            eligibility_outcome="ineligible",
                            eligibility_reason_code="insufficient_artwork_resolution",
                            eligibility_rule_failed="runtime_resolution_check",
                            eligibility_gate_stage="runtime_processing",
                        )
                    )
                log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": result}, blueprint_id=resolved_template.printify_blueprint_id, provider_id=resolved_template.printify_print_provider_id, action="skip", upload_map=upload_map)
                continue

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
                if resolved_template.key == "poster_basic":
                    poster_cover_eligible_report = (
                        "" if first_prepared.poster_cover_eligible is None else str(first_prepared.poster_cover_eligible).lower()
                    )
                    poster_enhancement_status_report = first_prepared.poster_enhancement_status
                    poster_enhancement_tier_report = first_prepared.poster_enhancement_tier
                    poster_source_ratio_report = f"{first_prepared.poster_source_ratio:.3f}"
                    poster_requested_upscale_factor_report = f"{first_prepared.poster_requested_upscale_factor:.3f}"
                    poster_applied_upscale_factor_report = f"{first_prepared.poster_applied_upscale_factor:.3f}"
                    poster_fill_optimization_used_report = first_prepared.poster_fill_optimization_used

            if skipped_placements:
                runtime_diag = RuntimeSkipDiagnostics(
                    template_key=template.key,
                    blueprint_id=resolved_template.printify_blueprint_id,
                    provider_id=resolved_template.printify_print_provider_id,
                    selected_count=len(variant_rows),
                    final_enabled_count=len(variant_rows),
                    available_placements=[p.placement_name for p in resolved_placements],
                    required_placement_name=skipped_placements[0] if skipped_placements else "",
                    final_reason_code="placement_artwork_resolution_skipped",
                    payload_build_skip_reason="placement_prepare_artwork_returned_none",
                    resolved_option_dimensions={"model": variant_diagnostics.resolved_model_dimension},
                    resolved_model_list=list(variant_diagnostics.final_selected_models),
                )
                runtime_diag.print_area_available = bool(runtime_diag.required_placement_name)
                log_runtime_skip_diagnostics(runtime_diag)
                all_templates_successful = False
                if summary is not None:
                    summary.products_skipped += 1
                result = {
                    "status": "skipped_undersized",
                    "placements": skipped_placements,
                    "runtime_skip_reason_code": runtime_diag.final_reason_code,
                    "runtime_skip_diagnostics": asdict(runtime_diag),
                }
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
                    run_rows.append(
                        RunReportRow(
                            timestamp=datetime.now(timezone.utc).isoformat(),
                            artwork_filename=artwork.src_path.name,
                            artwork_slug=artwork.slug,
                            template_key=template.key,
                            status="skipped",
                            action="skip",
                            blueprint_id=resolved_template.printify_blueprint_id,
                            provider_id=resolved_template.printify_print_provider_id,
                            upload_strategy=upload_strategy,
                            product_id="",
                            publish_attempted=False,
                            publish_verified=False,
                            rendered_title=rendered_title,
                            reason_code=runtime_diag.final_reason_code or "placement_artwork_resolution_skipped",
                            source_size=source_size_report,
                            trimmed_bounds_size=trimmed_bounds_report,
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
                            routed_asset_family=routed_asset_family,
                            routed_asset_mode=routed_asset_mode,
                            template_family=template_family_report,
                            product_family_label=family_label_report,
                            eligibility_outcome="ineligible",
                            eligibility_reason_code=runtime_diag.final_reason_code or "placement_artwork_resolution_skipped",
                            eligibility_rule_failed="placement_prepare_artwork",
                            eligibility_gate_stage="runtime_processing",
                        )
                    )
                log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": result}, blueprint_id=resolved_template.printify_blueprint_id, provider_id=resolved_template.printify_print_provider_id, action="skip", upload_map=upload_map)
                continue

            if progress is not None:
                progress.update(stage="upload")
            upload_map = upload_assets_to_printify(printify, state, artwork, resolved_template, prepared_assets, state_path, upload_strategy, r2_config)

            result: Dict[str, Any] = {}
            if progress is not None:
                progress.update(stage="create/update")
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
                defer_publish=defer_publish,
                state=state,
                auto_rebuild_on_incompatible_update=auto_rebuild_on_incompatible_update,
                prior_state_row=latest_success_row,
            ) if (resolved_template.push_via_printify and shop_id is not None) else {"status": "prepared_only", "action": action, "publish_attempted": False, "publish_verified": False}
            printify_result_for_skip = result.get("printify", {}) if isinstance(result.get("printify"), dict) else {}
            if str(printify_result_for_skip.get("action") or "") == "skip":
                guardrail_report = printify_result_for_skip.get("guardrail_report", {}) if isinstance(printify_result_for_skip.get("guardrail_report"), dict) else {}
                runtime_diag = RuntimeSkipDiagnostics(
                    template_key=template.key,
                    blueprint_id=resolved_template.printify_blueprint_id,
                    provider_id=resolved_template.printify_print_provider_id,
                    selected_count=int(guardrail_report.get("selected_count", len(variant_rows))),
                    final_enabled_count=int(guardrail_report.get("final_enabled_count", 0)),
                    available_placements=[p.placement_name for p in resolved_placements],
                    required_placement_name=str((resolved_template.preferred_primary_placement or (resolved_placements[0].placement_name if resolved_placements else ""))),
                    print_area_available=bool(resolved_placements),
                    upload_map=upload_map,
                    payload_build_skip_reason=str(printify_result_for_skip.get("reason") or ""),
                    resolved_option_dimensions={"model": variant_diagnostics.resolved_model_dimension},
                    resolved_model_list=list(variant_diagnostics.final_selected_models),
                    final_reason_code="payload_build_guardrail_skip",
                )
                printify_result_for_skip["runtime_skip_reason_code"] = runtime_diag.final_reason_code
                printify_result_for_skip["runtime_skip_diagnostics"] = asdict(runtime_diag)
                log_runtime_skip_diagnostics(runtime_diag)
            if template.publish_to_shopify and shopify is not None:
                result["shopify"] = create_in_shopify_only(shopify, artwork, template, variant_rows)

            printify_result = result.get("printify", {}) if isinstance(result.get("printify"), dict) else {}
            if progress is not None and bool(printify_result.get("publish_attempted")):
                progress.update(stage="publish")
            if progress is not None and verify_publish:
                progress.update(stage="verify")
            family_collection = resolve_family_collection_target(resolved_template)
            family_collection_handle = str(family_collection.get("handle") or "").strip()
            family_collection_title = str(family_collection.get("title") or "").strip()
            chosen_collection_handle = collection_handle.strip() or family_collection_handle
            chosen_collection_title = collection_title.strip() or family_collection_title
            preferred_featured_variant_color = choose_preferred_featured_variant_color(template=resolved_template, variant_rows=variant_rows)
            verified_product = printify_result.get("verified_product", {}) if isinstance(printify_result.get("verified_product"), dict) else {}
            preferred_featured_candidate = choose_preferred_featured_mockup_candidate(
                template=resolved_template,
                variant_rows=variant_rows,
                product_images=verified_product.get("images", []) if isinstance(verified_product.get("images"), list) else [],
            )
            logger.info(
                "Featured mockup preference template=%s preferred_color=%s selected_color=%s selected_type=%s selected_position=%s source=%s",
                resolved_template.key,
                preferred_featured_variant_color or "-",
                preferred_featured_candidate.get("selected_featured_mockup_color", "") or "-",
                preferred_featured_candidate.get("selected_featured_mockup_type", "") or "-",
                preferred_featured_candidate.get("selected_featured_mockup_position", "") or "-",
                "verified_product_image_preference" if preferred_featured_candidate.get("selected_featured_mockup_src") else "fallback_recommendation",
            )
            collection_result = {
                "collection_sync_attempted": False,
                "collection_sync_status": "skipped_disabled",
                "collection_id": "",
                "collection_handle": chosen_collection_handle,
                "collection_title": chosen_collection_title,
                "collection_membership_verified": False,
                "collection_warning": "",
                "collection_error": "",
                "family_collection_handle": family_collection_handle,
                "collection_image_source": collection_image_src,
                "collection_sort_strategy": collection_sort_order,
                "removed_collection_ids": [],
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
                    collection_handle=chosen_collection_handle,
                    collection_title=chosen_collection_title,
                    collection_description=collection_description,
                    verify_membership=verify_collections,
                    secondary_collection_handles=[v.strip() for v in secondary_collection_handles.split(",") if v.strip()] + list(resolved_template.secondary_collection_handles or []),
                    enforce_family_collection_membership=enforce_family_collection_membership,
                    collection_removal_mode=collection_removal_mode,
                    family_collection_handle=family_collection_handle,
                    allowed_family_collection_handles=[cfg.get("handle", "") for cfg in FAMILY_COLLECTION_RULES.values()],
                    collection_sort_order=collection_sort_order,
                    collection_image_src=collection_image_src,
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
                "publish_outcome": str(printify_result.get("publish_outcome") or ""),
                "publish_queue_status": str(printify_result.get("publish_queue_status") or ""),
                "last_verified_at": datetime.now(timezone.utc).isoformat() if verification else None,
                "verified_title": verification.get("verified_title"),
                "verified_variant_count": verification.get("verified_variant_count"),
                "title_source": title_info.title_source,
                "rendered_title": rendered_title,
                "launch_plan_row": launch_plan_row,
                "launch_plan_row_id": launch_plan_row_id,
                "collection_handle": chosen_collection_handle,
                "collection_title": chosen_collection_title,
                "collection_description": collection_description,
                "launch_name": launch_name,
                "campaign": campaign,
                    "merch_theme": merch_theme,
                    "routed_asset_family": routed_asset_family,
                    "routed_asset_mode": routed_asset_mode,
                    "collection_sync_attempted": bool(collection_result.get("collection_sync_attempted", False)),
                "collection_sync_status": str(collection_result.get("collection_sync_status") or ""),
                "shopify_collection_id": str(collection_result.get("collection_id") or ""),
                "collection_membership_verified": bool(collection_result.get("collection_membership_verified", False)),
                "collection_warning": str(collection_result.get("collection_warning") or ""),
                "collection_error": str(collection_result.get("collection_error") or ""),
                "family_collection_handle": str(collection_result.get("family_collection_handle") or family_collection_handle),
                "collection_image_source": str(collection_result.get("collection_image_source") or collection_image_src),
                "collection_sort_strategy": str(collection_result.get("collection_sort_strategy") or collection_sort_order),
                "preferred_featured_variant_color": preferred_featured_variant_color,
                "selected_featured_mockup_color": preferred_featured_candidate.get("selected_featured_mockup_color", ""),
                "featured_image_strategy": resolved_template.preferred_featured_image_strategy,
                "featured_image_source": "verified_product_image_preference" if preferred_featured_candidate.get("selected_featured_mockup_src") else "printify_mockup_recommendation",
                "tote_scale_strategy": "front_fill_boost_orientation_tuned" if resolved_template.key == "tote_basic" else "",
                "rerun_fingerprints": printify_result.get("rerun_fingerprints", {}) if isinstance(printify_result.get("rerun_fingerprints", {}), dict) else {},
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
                    publish_outcome=str(row.get("publish_outcome") or ""),
                    publish_queue_status=str(row.get("publish_queue_status") or ""),
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
                    collection_handle=chosen_collection_handle,
                    collection_title=chosen_collection_title,
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
                    routed_asset_family=routed_asset_family,
                    routed_asset_mode=routed_asset_mode,
                    template_family=template_family_report,
                    product_family_label=family_label_report,
                    tote_primary_placement=tote_primary_placement_report,
                    tote_active_placements=tote_active_placements_report,
                    poster_cover_eligible=poster_cover_eligible_report,
                    poster_enhancement_status=poster_enhancement_status_report,
                    poster_enhancement_tier=poster_enhancement_tier_report,
                    poster_source_ratio=poster_source_ratio_report,
                    poster_requested_upscale_factor=poster_requested_upscale_factor_report,
                    poster_applied_upscale_factor=poster_applied_upscale_factor_report,
                    poster_fill_optimization_used=poster_fill_optimization_used_report,
                    family_collection_handle=str(collection_result.get("family_collection_handle") or family_collection_handle),
                    collection_image_source=str(collection_result.get("collection_image_source") or collection_image_src),
                    collection_sort_strategy=str(collection_result.get("collection_sort_strategy") or collection_sort_order),
                    preferred_featured_variant_color=preferred_featured_variant_color,
                    selected_featured_mockup_color=preferred_featured_candidate.get("selected_featured_mockup_color", ""),
                    featured_image_strategy=resolved_template.preferred_featured_image_strategy,
                    featured_image_source="verified_product_image_preference" if preferred_featured_candidate.get("selected_featured_mockup_src") else "printify_mockup_recommendation",
                    tote_scale_strategy="front_fill_boost_orientation_tuned" if resolved_template.key == "tote_basic" else "",
                    metadata_resolution_source=artwork.metadata_resolution_source,
                    metadata_generated_inline=artwork.metadata_generated_inline,
                    metadata_sidecar_written=artwork.metadata_sidecar_written,
                    weak_metadata_detected="|".join(artwork.weak_metadata_detected),
                    final_title_source=title_info.title_source,
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
                if str(row.get("publish_queue_status") or "").startswith("pending"):
                    summary.products_created_but_not_published += 1
                    if row.get("publish_outcome") == "create_success_publish_rate_limited":
                        summary.publish_rate_limit_events += 1
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
                    error_type=classify_failure(exc),
                    reason_code=_failure_reason_code(exc),
                    error_message=str(exc),
                    suggested_next_action="Inspect state and rerun with --resume after fixing template or artwork",
                    launch_plan_row=launch_plan_row,
                    launch_plan_row_id=launch_plan_row_id,
                    source_size=source_size_report,
                ))
            if run_rows is not None:
                run_rows.append(RunReportRow(datetime.now(timezone.utc).isoformat(), artwork.src_path.name, artwork.slug, template.key, "failure", action, resolved_template.printify_blueprint_id, resolved_template.printify_print_provider_id, summarize_upload_strategy(upload_map), "", False, False, rendered_title, source_size_report, trimmed_bounds_report, "", exported_canvas_report, placement_scale_report, effective_upscale_factor_report, requested_upscale_factor_report, applied_upscale_factor_report, upscale_capped_report, orientation_report, launch_plan_row, launch_plan_row_id, collection_handle, collection_title, collection_description, launch_name, campaign, merch_theme))
            log_template_summary(artwork_slug=artwork.slug, template_key=template.key, success=False, result={"printify": error_result}, blueprint_id=resolved_template.printify_blueprint_id, provider_id=resolved_template.printify_print_provider_id, action=action, upload_map=upload_map)
        if progress is not None:
            progress.complete_one()
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
    parser.add_argument(
        "--auto-wallart-master",
        action="store_true",
        help="Opt-in: allow canvas_basic/blanket_basic/framed_poster_basic to derive a bounded upscaled wall-art master instead of immediate cover-mode resolution skip (requires --allow-upscale).",
    )
    parser.add_argument("--skip-undersized", action="store_true", help="Skip undersized artwork/template placements instead of failing")
    parser.add_argument("--templates", default=str(TEMPLATES_CONFIG), help="Path to product_templates.json")
    parser.add_argument("--image-dir", default=str(IMAGE_DIR), help="Image source directory")
    parser.add_argument("--export-dir", default=str(EXPORT_DIR), help="Export output directory")
    parser.add_argument("--state-path", default=str(STATE_PATH), help="State JSON path")
    parser.add_argument("--log-level", default="INFO", help="Logging level: DEBUG/INFO/WARNING/ERROR")
    parser.add_argument("--skip-audit", action="store_true", help="Skip Printify catalog/shop preflight audit")
    parser.add_argument("--max-artworks", type=int, default=0, help="Limit number of discovered artworks (0 = no limit)")
    parser.add_argument("--local-image-batch", type=int, default=0, help="Cheap local-image-first alias for --max-artworks (first N discovered local images)")
    parser.add_argument("--batch-size", type=int, default=0, help="Limit number of artwork/template combinations processed this run (0 = no limit)")
    parser.add_argument("--stop-after-failures", type=int, default=0, help="Stop run after N combination failures (0 = no limit)")
    parser.add_argument("--fail-fast", action="store_true", help="Stop immediately after the first combination failure")
    parser.add_argument("--resume", action="store_true", help="Skip combinations already successful in state and continue pending work")
    parser.add_argument("--resume-only-pending", action="store_true", help="Alias for --resume with explicit pending-only behavior")
    parser.add_argument("--chunk-size", type=int, default=0, help="Process artworks in chunks (0 = no chunking)")
    parser.add_argument("--pause-between-chunks-seconds", type=float, default=0.0, help="Pause between chunks to reduce API pressure")
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
    parser.add_argument("--defer-publish", action="store_true", help="Create/update products now and enqueue publish for later")
    parser.add_argument("--resume-publish-only", action="store_true", help="Skip create/update and only drain persisted publish queue")
    progress_group = parser.add_mutually_exclusive_group()
    progress_group.add_argument("--progress", dest="progress", action="store_true", help="Force-enable single-line terminal progress display")
    progress_group.add_argument("--no-progress", dest="progress", action="store_false", help="Force-disable terminal progress display")
    parser.set_defaults(progress=None)
    parser.add_argument("--publish-batch-size", type=int, default=5, help="Maximum queued publish operations per batch")
    parser.add_argument("--pause-between-publish-batches-seconds", type=float, default=2.0, help="Pause duration between publish batches")
    parser.add_argument("--verify-publish", action="store_true", help="Read back created/updated product and verify basic storefront indicators")
    parser.add_argument(
        "--max-retry-sleep-seconds",
        type=float,
        default=MAX_RETRY_SLEEP_SECONDS,
        help="Absolute max retry sleep for API requests (applies to catalog + mutation retry caps).",
    )
    parser.add_argument(
        "--interactive-retry-cap-seconds",
        type=float,
        default=INTERACTIVE_RETRY_CAP_SECONDS,
        help="Extra retry sleep cap for interactive mutation endpoints (publish/create/update).",
    )
    parser.add_argument("--catalog-cache-dir", default=CATALOG_CACHE_DIR_DEFAULT, help="Persistent catalog cache directory")
    parser.add_argument("--catalog-cache-ttl-hours", type=int, default=CACHE_TTL_HOURS_DEFAULT, help="Catalog cache TTL in hours")
    parser.add_argument("--no-catalog-cache", action="store_true", help="Disable persistent catalog cache")
    parser.add_argument("--catalog-request-spacing-ms", type=int, default=0, help="Minimum spacing between catalog GET requests")
    parser.add_argument("--template-spacing-ms", type=int, default=0, help="Sleep after each template processing step")
    parser.add_argument("--artwork-spacing-ms", type=int, default=0, help="Sleep after each artwork processing step")
    parser.add_argument("--high-volume-mode", action="store_true", help="Enable conservative defaults for high-volume runs")
    parser.add_argument("--inspect-state-key", default="", help="Read-only inspect state entry by key (artwork_slug:template_key)")
    parser.add_argument("--list-state-keys", action="store_true", help="List known state keys from state.json and exit")
    parser.add_argument("--list-failures", action="store_true", help="List failed combinations from state and exit")
    parser.add_argument("--list-pending", action="store_true", help="List combinations not yet successful and exit")
    parser.add_argument("--export-failure-report", default="", help="Optional CSV export path for failed combinations")
    parser.add_argument("--export-run-report", default="", help="Optional CSV export path for all processed combinations")
    parser.add_argument("--export-preflight-report", default="", help="Optional CSV export path for template preflight report rows")
    parser.add_argument("--run-free-shipping-profit-audit", action="store_true", help="Run a per-template free-shipping profitability audit and exit")
    parser.add_argument("--free-shipping-profit-min", default="4.00", help="Minimum profit-after-shipping policy floor (USD) for free-shipping viability audit")
    parser.add_argument("--free-shipping-profit-audit-csv", default="reports/free_shipping_profit_audit.csv", help="CSV path for free-shipping profitability audit output")
    parser.add_argument("--free-shipping-profit-audit-json", default="reports/free_shipping_profit_audit.json", help="JSON path for free-shipping profitability audit output")
    parser.add_argument("--allow-preflight-failures", "--audit-all-templates", dest="allow_preflight_failures", action="store_true", help="Audit mode: continue run even when explicitly requested templates fail preflight")
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
    parser.add_argument(
        "--auto-generate-missing-metadata",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Inline during normal create/publish runs: auto-generate metadata when missing/weak (default on)",
    )
    parser.add_argument(
        "--auto-write-generated-sidecars",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="When inline metadata is generated, write sidecars immediately (default on)",
    )
    parser.add_argument(
        "--metadata-inline-generator",
        choices=[mode.value for mode in MetadataGeneratorMode],
        default=MetadataGeneratorMode.AUTO.value,
        help="Inline metadata generator strategy for normal runs (auto=openai->vision->heuristic)",
    )
    parser.add_argument(
        "--metadata-inline-only-when-weak",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Only run inline generation when metadata is missing/weak (default on)",
    )
    parser.add_argument(
        "--metadata-inline-overwrite-weak-sidecars",
        action="store_true",
        help="Allow inline-generated metadata to overwrite existing weak sidecars (default off)",
    )
    parser.add_argument(
        "--enable-ai-product-copy",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable OpenAI-assisted product copy for hoodie/mug/tshirt/sweatshirt/poster/phone_case families (default from ENABLE_AI_PRODUCT_COPY env var).",
    )
    parser.add_argument(
        "--ai-product-copy-model",
        default="",
        help="Optional model override for AI product copy (defaults to OPENAI_MODEL).",
    )
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
    parser.add_argument("--art-openai-timeout-seconds", type=float, default=180.0, help="OpenAI image request timeout in seconds (per attempt)")
    parser.add_argument("--art-openai-max-retries", type=int, default=4, help="Maximum OpenAI image retries for transient transport/timeout failures")
    parser.add_argument("--art-openai-retry-backoff-seconds", type=float, default=2.0, help="Base retry backoff seconds for OpenAI image generation (exponential backoff)")
    parser.add_argument(
        "--art-openai-size",
        choices=["1024x1024", "1024x1536", "1536x1024", "auto"],
        default="",
        help="Optional global OpenAI size override for all planned art masters",
    )
    parser.add_argument(
        "--art-openai-portrait-size",
        choices=["1024x1024", "1024x1536", "1536x1024", "auto"],
        default="1024x1536",
        help="OpenAI size for portrait master planning (ignored when --art-openai-size is set)",
    )
    parser.add_argument(
        "--art-openai-square-size",
        choices=["1024x1024", "1024x1536", "1536x1024", "auto"],
        default="1024x1024",
        help="OpenAI size for square master planning (ignored when --art-openai-size is set)",
    )
    parser.add_argument(
        "--art-openai-landscape-size",
        choices=["1024x1024", "1024x1536", "1536x1024", "auto"],
        default="1536x1024",
        help="OpenAI size for landscape master planning (ignored when --art-openai-size is set)",
    )
    parser.add_argument("--art-run-metadata", action="store_true", help="Generate sidecar metadata for newly generated artwork before exiting/continuing")
    parser.add_argument("--art-run-storefront-qa", action="store_true", help="After generation, run storefront QA against generated artwork")
    parser.add_argument("--art-publish", action="store_true", help="After generation, run full create/update flow and force publish")
    parser.add_argument("--art-verify-publish", action="store_true", help="After generation, verify publish/readback indicators")
    parser.add_argument("--art-target-mode", choices=["auto", "portrait", "square", "multi"], default="auto", help="Target aspect planning mode for generated artwork")
    parser.add_argument("--art-family-aware", action="store_true", help="Enable family-aware prompt planning/routing for generated masters")
    parser.add_argument("--art-family-mode", choices=["auto", "split", "single"], default="auto", help="Family-aware planning mode")
    parser.add_argument("--art-generate-poster-master", action="store_true", help="Force poster master generation when family-aware mode is enabled")
    parser.add_argument("--art-generate-apparel-master", action="store_true", help="Force apparel master generation when family-aware mode is enabled")
    parser.add_argument("--art-mug-tote-master", choices=["apparel", "square", "auto"], default="apparel", help="Family master preference for mug/tote templates")
    parser.add_argument("--art-apparel-style", default="", help="Optional apparel-family style override appended only to apparel master prompts")
    parser.add_argument("--art-poster-style", default="", help="Optional poster-family style override appended only to poster master prompts")
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
    parser.add_argument("--enforce-family-collection-membership", action="store_true", help="Enforce deterministic family collection routing during collection sync")
    parser.add_argument("--collection-removal-mode", choices=["conservative", "strict"], default="conservative", help="Collection removal scope when family enforcement is active")
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
    else:
        selected = [template for template in templates if template.active]
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


def process_publish_queue(
    *,
    printify: PrintifyClient,
    state: Dict[str, Any],
    templates_by_key: Dict[str, ProductTemplate],
    publish_batch_size: int,
    pause_between_publish_batches_seconds: float,
) -> Dict[str, int]:
    queue = get_publish_queue(state)
    initial_counts = summarize_publish_queue(state)
    stats = {"total": initial_counts["total"], "pending": initial_counts["pending"], "completed": initial_counts["completed"], "failed": initial_counts["failed"], "rate_limited": 0, "processed": 0}
    pending_rows = [row for row in queue if isinstance(row, dict) and str(row.get("publish_status") or "").startswith("pending")]
    logger.info(
        "Resume publish queue start total=%s pending=%s completed=%s failed=%s rows_processed_this_run=%s",
        initial_counts["total"],
        initial_counts["pending"],
        initial_counts["completed"],
        initial_counts["failed"],
        0,
    )
    if not pending_rows:
        logger.info(
            "Resume publish queue end total=%s pending=%s completed=%s failed=%s rows_processed_this_run=%s",
            initial_counts["total"],
            initial_counts["pending"],
            initial_counts["completed"],
            initial_counts["failed"],
            0,
        )
        return stats
    batch_size = max(1, int(publish_batch_size))
    for index in range(0, len(pending_rows), batch_size):
        batch = pending_rows[index:index + batch_size]
        for row in batch:
            template = templates_by_key.get(str(row.get("template_key") or ""))
            if template is None:
                row["publish_status"] = "failed"
                row["last_error"] = "template_not_found_for_publish_resume"
                row["updated_at"] = _utc_now_iso()
                stats["failed"] += 1
                continue
            row["publish_attempts"] = int(row.get("publish_attempts") or 0) + 1
            try:
                printify.publish_product(int(row["shop_id"]), str(row["product_id"]), build_printify_publish_payload(template))
                row["publish_status"] = "completed"
                row["reason_code"] = ""
                row["last_error"] = ""
                row["updated_at"] = _utc_now_iso()
                stats["completed"] += 1
            except RetryLimitExceededError as exc:
                if exc.reason_code == "publish_rate_limited":
                    row["publish_status"] = "pending_retry"
                    row["reason_code"] = exc.reason_code
                    row["last_error"] = str(exc)
                    row["updated_at"] = _utc_now_iso()
                    stats["rate_limited"] += 1
                else:
                    row["publish_status"] = "failed"
                    row["reason_code"] = exc.reason_code
                    row["last_error"] = str(exc)
                    row["updated_at"] = _utc_now_iso()
                    stats["failed"] += 1
            except Exception as exc:
                row["publish_status"] = "failed"
                row["reason_code"] = classify_failure(exc)
                row["last_error"] = str(exc)
                row["updated_at"] = _utc_now_iso()
                stats["failed"] += 1
            stats["processed"] += 1
        if pause_between_publish_batches_seconds > 0 and (index + batch_size) < len(pending_rows):
            time.sleep(max(0.0, float(pause_between_publish_batches_seconds)))
    final_counts = summarize_publish_queue(state)
    stats["total"] = final_counts["total"]
    stats["pending"] = final_counts["pending"]
    stats["completed"] = final_counts["completed"]
    stats["failed"] = final_counts["failed"]
    logger.info(
        "Resume publish queue end total=%s pending=%s completed=%s failed=%s rows_processed_this_run=%s",
        final_counts["total"],
        final_counts["pending"],
        final_counts["completed"],
        final_counts["failed"],
        stats["processed"],
    )
    return stats


def apply_high_volume_mode_defaults(
    *,
    chunk_size: int,
    pause_between_chunks_seconds: float,
    catalog_request_spacing_ms: int,
    template_spacing_ms: int,
    artwork_spacing_ms: int,
    no_catalog_cache: bool,
    publish_batch_size: int,
    pause_between_publish_batches_seconds: float,
    defer_publish: bool,
) -> Dict[str, Any]:
    return {
        "no_catalog_cache": False if no_catalog_cache else no_catalog_cache,
        "chunk_size": chunk_size if chunk_size > 0 else 10,
        "pause_between_chunks_seconds": pause_between_chunks_seconds if pause_between_chunks_seconds > 0 else 2.0,
        "catalog_request_spacing_ms": catalog_request_spacing_ms if catalog_request_spacing_ms > 0 else 150,
        "template_spacing_ms": template_spacing_ms if template_spacing_ms > 0 else 100,
        "artwork_spacing_ms": artwork_spacing_ms if artwork_spacing_ms > 0 else 150,
        "publish_batch_size": publish_batch_size if publish_batch_size > 0 else 5,
        "pause_between_publish_batches_seconds": pause_between_publish_batches_seconds if pause_between_publish_batches_seconds > 0 else 3.0,
        "defer_publish": defer_publish,
    }


def run(config_path: pathlib.Path, *, dry_run: bool = False, force: bool = False, allow_upscale: bool = False, upscale_method: str = "lanczos", auto_wallart_master: bool = False, skip_undersized: bool = False, image_dir: pathlib.Path = IMAGE_DIR, export_dir: pathlib.Path = EXPORT_DIR, state_path: pathlib.Path = STATE_PATH, skip_audit: bool = False, max_artworks: int = 0, batch_size: int = 0, stop_after_failures: int = 0, fail_fast: bool = False, resume: bool = False, upload_strategy: str = "auto", template_keys: Optional[List[str]] = None, limit_templates: int = 0, list_templates: bool = False, list_blueprints: bool = False, search_blueprints_query: str = "", limit_blueprints: int = 25, list_providers: bool = False, blueprint_id: int = 0, provider_id: int = 0, limit_providers: int = 25, inspect_variants: bool = False, recommend_provider: bool = False, template_file: str = "", generate_template_snippet_flag: bool = False, auto_provider: bool = False, snippet_key: str = "", template_output_file: str = "", create_only: bool = False, update_only: bool = False, rebuild_product: bool = False, publish_mode: str = "default", verify_publish: bool = False, auto_rebuild_on_incompatible_update: bool = False, sync_collections: bool = False, skip_collections: bool = False, verify_collections: bool = False, enforce_family_collection_membership: bool = False, collection_removal_mode: str = "conservative", inspect_state_key_value: str = "", list_state_keys_only: bool = False, list_failures_only: bool = False, list_pending_only: bool = False, export_failure_report: str = "", export_run_report: str = "", export_preflight_report_path: str = "", run_free_shipping_profit_audit_only: bool = False, free_shipping_profit_min: str = "4.00", free_shipping_profit_audit_csv_path: str = "reports/free_shipping_profit_audit.csv", free_shipping_profit_audit_json_path: str = "reports/free_shipping_profit_audit.json", allow_preflight_failures: bool = False, preview_listing_copy_only: bool = False, generate_artwork_metadata: bool = False, metadata_preview: bool = False, write_sidecars: bool = False, overwrite_sidecars: bool = False, metadata_max_artworks: int = 0, metadata_output_dir: str = "", metadata_only_missing: bool = True, metadata_generator: str = MetadataGeneratorMode.HEURISTIC.value, metadata_openai_model: str = "", metadata_openai_timeout: float = 30.0, metadata_auto_approve: bool = False, metadata_min_confidence: float = 0.9, metadata_review_report: str = "", metadata_review_json: str = "", metadata_write_auto_approved_only: bool = False, metadata_allow_review_writes: bool = False, auto_generate_missing_metadata: bool = True, auto_write_generated_sidecars: bool = True, metadata_inline_generator: str = MetadataGeneratorMode.AUTO.value, metadata_inline_only_when_weak: bool = True, metadata_inline_overwrite_weak_sidecars: bool = False, enable_ai_product_copy: Optional[bool] = None, ai_product_copy_model: str = "", generate_artwork_from_prompt: bool = False, art_prompt: str = "", art_count: int = 1, art_style: str = "", art_negative_prompt: str = "", art_visible_text: str = "", art_output_dir: str = "", art_base_name: str = "generated-art", art_quality: str = "high", art_background: str = "auto", art_generator: str = "openai", art_openai_model: str = "", art_openai_timeout_seconds: float = 180.0, art_openai_max_retries: int = 4, art_openai_retry_backoff_seconds: float = 2.0, art_openai_size: str = "", art_openai_portrait_size: str = "1024x1536", art_openai_square_size: str = "1024x1024", art_openai_landscape_size: str = "1536x1024", art_run_metadata: bool = False, art_run_storefront_qa: bool = False, art_publish: bool = False, art_verify_publish: bool = False, art_target_mode: str = "auto", art_family_aware: bool = False, art_family_mode: str = "auto", art_generate_poster_master: bool = False, art_generate_apparel_master: bool = False, art_mug_tote_master: str = "apparel", art_apparel_style: str = "", art_poster_style: str = "", art_skip_existing_generated: bool = False, art_dry_run_plan: bool = False, source_min_width: int = 1, source_min_height: int = 1, include_preview_assets: bool = False, launch_plan_path: str = "", export_launch_plan_template: str = "", export_launch_plan_from_images_path: str = "", include_disabled_template_rows: bool = False, launch_plan_default_enabled: bool = True, placement_preview: bool = False, storefront_qa: bool = False, strict_storefront_qa: bool = False, export_storefront_qa_report: str = "", export_storefront_qa_json: str = "", max_retry_sleep_seconds: float = MAX_RETRY_SLEEP_SECONDS, interactive_retry_cap_seconds: float = INTERACTIVE_RETRY_CAP_SECONDS, catalog_cache_dir: str = CATALOG_CACHE_DIR_DEFAULT, catalog_cache_ttl_hours: int = CACHE_TTL_HOURS_DEFAULT, no_catalog_cache: bool = False, chunk_size: int = 0, pause_between_chunks_seconds: float = 0.0, catalog_request_spacing_ms: int = 0, template_spacing_ms: int = 0, artwork_spacing_ms: int = 0, resume_only_pending: bool = False, high_volume_mode: bool = False, publish_batch_size: int = 5, pause_between_publish_batches_seconds: float = 2.0, defer_publish: bool = False, resume_publish_only: bool = False, progress: Optional[bool] = None) -> None:
    max_retry_sleep_seconds = max(0.0, float(max_retry_sleep_seconds))
    interactive_retry_cap_seconds = max(0.0, float(interactive_retry_cap_seconds))
    if resume_only_pending:
        resume = True
    if high_volume_mode:
        hv_defaults = apply_high_volume_mode_defaults(
            chunk_size=chunk_size,
            pause_between_chunks_seconds=pause_between_chunks_seconds,
            catalog_request_spacing_ms=catalog_request_spacing_ms,
            template_spacing_ms=template_spacing_ms,
            artwork_spacing_ms=artwork_spacing_ms,
            no_catalog_cache=no_catalog_cache,
            publish_batch_size=publish_batch_size,
            pause_between_publish_batches_seconds=pause_between_publish_batches_seconds,
            defer_publish=defer_publish,
        )
        no_catalog_cache = bool(hv_defaults["no_catalog_cache"])
        chunk_size = int(hv_defaults["chunk_size"])
        pause_between_chunks_seconds = float(hv_defaults["pause_between_chunks_seconds"])
        catalog_request_spacing_ms = int(hv_defaults["catalog_request_spacing_ms"])
        template_spacing_ms = int(hv_defaults["template_spacing_ms"])
        artwork_spacing_ms = int(hv_defaults["artwork_spacing_ms"])
        publish_batch_size = int(hv_defaults["publish_batch_size"])
        pause_between_publish_batches_seconds = float(hv_defaults["pause_between_publish_batches_seconds"])
    if publish_mode not in {"default", "publish", "skip"}:
        raise RuntimeError(f"Unsupported publish mode: {publish_mode}")
    if auto_wallart_master and not allow_upscale:
        logger.warning("Wall-art auto master requested but --allow-upscale is disabled; auto-wallart-master will remain inactive.")
    configure_ai_product_copy(enabled=enable_ai_product_copy, model=ai_product_copy_model)

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

    if not resume_publish_only and not image_dir.exists():
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
    generated_template_routing: List[TemplateAssetRouting] = []
    if generate_artwork_from_prompt:
        logger.info(
            "Prompt-art templates selected for processing count=%s templates=%s",
            len(templates),
            ",".join(template.key for template in templates) or "-",
        )
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
            openai_timeout_seconds=max(1.0, float(art_openai_timeout_seconds)),
            openai_max_retries=max(0, int(art_openai_max_retries)),
            openai_retry_backoff_seconds=max(0.0, float(art_openai_retry_backoff_seconds)),
            openai_size=art_openai_size,
            openai_portrait_size=art_openai_portrait_size,
            openai_square_size=art_openai_square_size,
            openai_landscape_size=art_openai_landscape_size,
            base_name=slugify(art_base_name or "generated-art"),
            output_dir=output_dir,
            target_mode=art_target_mode,
            family_aware=art_family_aware,
            family_mode=art_family_mode,
            generate_poster_master=art_generate_poster_master,
            generate_apparel_master=art_generate_apparel_master,
            mug_tote_master=art_mug_tote_master,
            apparel_style=art_apparel_style,
            poster_style=art_poster_style,
            dry_run_plan=art_dry_run_plan,
            skip_existing_generated=art_skip_existing_generated,
            min_source_width=source_hygiene.min_source_width,
            min_source_height=source_hygiene.min_source_height,
        )
        generation_result = run_prompt_artwork_generation(request=request, templates=templates)
        generated_paths = generation_result.generated_paths
        generated_template_routing = generation_result.template_routing
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
    artworks: List[Artwork] = []
    if not resume_publish_only:
        try:
            artworks = discover_artworks(
                image_dir,
                candidate_paths=generated_paths,
                source_hygiene=source_hygiene,
                auto_generate_missing_metadata=auto_generate_missing_metadata,
                auto_write_generated_sidecars=auto_write_generated_sidecars,
                metadata_inline_generator=metadata_inline_generator,
                metadata_inline_only_when_weak=metadata_inline_only_when_weak,
                metadata_inline_overwrite_weak_sidecars=metadata_inline_overwrite_weak_sidecars,
                metadata_openai_model=metadata_openai_model,
                metadata_openai_timeout=metadata_openai_timeout,
            )
        except TypeError:
            artworks = discover_artworks(image_dir)
        if max_artworks > 0:
            artworks = artworks[:max_artworks]
        if high_volume_mode and len(artworks) >= 25 and publish_mode != "publish":
            defer_publish = True
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

    interactive_retry_policy = not any([resume, launch_plan_path, batch_size > 0, stop_after_failures > 0])
    catalog_cache = CatalogCache(
        cache_dir=pathlib.Path(catalog_cache_dir),
        ttl_hours=max(1, int(catalog_cache_ttl_hours)),
        enabled=not no_catalog_cache,
    )
    try:
        printify = PrintifyClient(
            PRINTIFY_API_TOKEN,
            dry_run=dry_run,
            interactive_retry_policy=interactive_retry_policy,
            interactive_retry_cap_seconds=interactive_retry_cap_seconds,
            max_retry_sleep_seconds=max_retry_sleep_seconds,
            catalog_request_spacing_ms=catalog_request_spacing_ms,
            catalog_cache=catalog_cache,
        )
    except TypeError:
        # Backward-compatible path for tests/mocks that monkeypatch PrintifyClient with legacy signature.
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

    templates, preflight_issues, preflight_rows = preflight_active_templates(
        printify=printify,
        templates=templates,
        explicit_template_keys=template_keys,
    )
    if generate_artwork_from_prompt:
        logger.info(
            "Prompt-art templates remaining after preflight count=%s templates=%s",
            len(templates),
            ",".join(template.key for template in templates) or "-",
        )
    preflight_report_path = pathlib.Path(export_preflight_report_path) if export_preflight_report_path else None
    if allow_preflight_failures and preflight_report_path is None:
        preflight_report_path = export_dir / "preflight_report.csv"
    if preflight_report_path is not None:
        export_preflight_report(preflight_report_path, preflight_rows)
        logger.info("Preflight report exported path=%s rows=%s", preflight_report_path, len(preflight_rows))
    explicit_failures = [issue for issue in preflight_issues if issue.requested_explicitly]
    summary_catalog_rate_limited = sum(1 for issue in preflight_issues if issue.classification == "catalog_rate_limited")
    if explicit_failures and not allow_preflight_failures:
        rendered = "; ".join(
            f"{issue.template_key}:{issue.classification}:{issue.message}" for issue in explicit_failures
        )
        raise RuntimeError(f"Template preflight failed for explicitly requested template(s): {rendered}")
    if not template_keys and not templates:
        if allow_preflight_failures:
            logger.warning("No runnable active templates after preflight validation; audit mode continuing with report-only completion.")
            return
        raise RuntimeError("No runnable active templates after preflight validation.")
    if run_free_shipping_profit_audit_only:
        free_shipping_min_minor = int((_decimal_from_value(free_shipping_profit_min, default="4.00") * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        audit_rows = run_free_shipping_profit_audit(
            printify=printify,
            templates=templates,
            free_shipping_min_profit_minor=free_shipping_min_minor,
        )
        export_free_shipping_profit_audit(
            csv_path=pathlib.Path(free_shipping_profit_audit_csv_path),
            json_path=pathlib.Path(free_shipping_profit_audit_json_path),
            rows=audit_rows,
        )
        logger.info(
            "Free-shipping profitability audit exported csv=%s json=%s rows=%s policy_min_minor=%s",
            free_shipping_profit_audit_csv_path,
            free_shipping_profit_audit_json_path,
            len(audit_rows),
            free_shipping_min_minor,
        )
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
    summary.templates_skipped_catalog_rate_limited = int(summary_catalog_rate_limited)
    failure_rows: List[FailureReportRow] = []
    run_rows: List[RunReportRow] = []
    templates_by_key = {template.key: template for template in templates}
    if resume_publish_only:
        queue_before = [dict(row) for row in get_publish_queue(state) if isinstance(row, dict)]
        publish_stats = process_publish_queue(
            printify=printify,
            state=state,
            templates_by_key=templates_by_key,
            publish_batch_size=publish_batch_size,
            pause_between_publish_batches_seconds=pause_between_publish_batches_seconds,
        )
        if publish_stats["pending"] == 0:
            logger.info("Publish queue is empty; nothing to resume.")
        queue_after = [row for row in get_publish_queue(state) if isinstance(row, dict)]
        queue_after_lookup = {
            (
                str(row.get("artwork_key") or ""),
                str(row.get("template_key") or ""),
                str(row.get("shop_id") or ""),
            ): row
            for row in queue_after
        }
        for before_row in queue_before:
            key = (
                str(before_row.get("artwork_key") or ""),
                str(before_row.get("template_key") or ""),
                str(before_row.get("shop_id") or ""),
            )
            before_status = str(before_row.get("publish_status") or "")
            if not before_status.startswith("pending"):
                continue
            after_row = queue_after_lookup.get(key, before_row)
            after_status = str(after_row.get("publish_status") or "")
            if after_status == before_status:
                continue
            reason_code = str(after_row.get("reason_code") or "")
            row_status = "success" if after_status == "completed" else "failure"
            action = "resume_publish_queue"
            run_rows.append(
                RunReportRow(
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    artwork_filename="",
                    artwork_slug=str(after_row.get("artwork_key") or ""),
                    template_key=str(after_row.get("template_key") or ""),
                    status=row_status,
                    action=action,
                    blueprint_id=0,
                    provider_id=0,
                    upload_strategy="resume-publish-only",
                    product_id=str(after_row.get("product_id") or ""),
                    publish_attempted=True,
                    publish_verified=False,
                    rendered_title="",
                    publish_outcome=after_status,
                    publish_queue_status=after_status,
                    publish_queue_status_before=before_status,
                    publish_queue_status_after=after_status,
                    reason_code=reason_code,
                    resume_only_queue_processing=True,
                )
            )
            if row_status == "failure":
                failure_rows.append(
                    FailureReportRow(
                        timestamp=datetime.now(timezone.utc).isoformat(),
                        artwork_filename="",
                        artwork_slug=str(after_row.get("artwork_key") or ""),
                        template_key=str(after_row.get("template_key") or ""),
                        action_attempted=action,
                        blueprint_id=0,
                        provider_id=0,
                        upload_strategy="resume-publish-only",
                        error_type="publish_resume_failure",
                        reason_code=reason_code or str(after_row.get("publish_status") or "publish_resume_failure"),
                        error_message=str(after_row.get("last_error") or ""),
                        suggested_next_action="Inspect publish queue failure and retry resume publish-only after remediation",
                    )
                )
        summary.publish_attempts += publish_stats["processed"]
        summary.resumed_combinations += publish_stats["processed"]
        summary.combinations_processed = len(run_rows)
        summary.combinations_success = sum(1 for row in run_rows if row.status == "success")
        summary.combinations_failed = sum(1 for row in run_rows if row.status == "failure")
        summary.combinations_skipped = sum(1 for row in run_rows if row.status == "skipped")
        summary.publish_queue_total_count = publish_stats["total"]
        summary.publish_queue_pending_count = publish_stats["pending"]
        summary.publish_queue_completed_count = publish_stats["completed"]
        summary.publish_queue_failed_count = publish_stats["failed"]
        summary.publish_rate_limit_events = publish_stats["rate_limited"]
        summary.rate_limit_events = dict(getattr(printify, "rate_limit_events", {}))
        if export_failure_report:
            write_csv_report(pathlib.Path(export_failure_report), [row.__dict__ for row in failure_rows])
            logger.info("Failure report exported path=%s rows=%s", export_failure_report, len(failure_rows))
        if export_run_report:
            write_csv_report(pathlib.Path(export_run_report), [row.__dict__ for row in run_rows])
            logger.info("Run report exported path=%s rows=%s", export_run_report, len(run_rows))
        save_json_atomic(state_path, state)
        log_run_summary(summary)
        return
    artwork_options = ArtworkProcessingOptions(
        allow_upscale=allow_upscale,
        upscale_method=upscale_method,
        skip_undersized=skip_undersized,
        placement_preview=placement_preview,
        preview_dir=export_dir / "previews",
        auto_wallart_master=auto_wallart_master,
    )
    combinations_processed = 0
    stop_requested = False
    effective_chunk_size = max(0, int(chunk_size))
    artwork_chunks: List[List[Artwork]] = [artworks]
    if effective_chunk_size > 0 and artworks:
        artwork_chunks = [artworks[i:i + effective_chunk_size] for i in range(0, len(artworks), effective_chunk_size)]
    summary.total_chunks = len(artwork_chunks)
    progress_enabled, progress_reason = should_enable_progress(force_enable=progress)
    progress_tracker = RunProgressTracker(
        enabled=progress_enabled,
        total=max(1, len(artworks) * max(1, len(templates))),
    )
    logger.info("Progress display enabled=%s reason=%s", progress_enabled, progress_reason)

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
        progress_tracker.total = max(1, len(launch_rows))
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
                    metadata=sanitize_metadata_for_publish(metadata),
                )
                artwork = _apply_inline_metadata_generation(
                    artwork=artwork,
                    metadata_source=match_info.get("source", "fallback"),
                    metadata_match_key=match_info.get("key", ""),
                    auto_generate_missing_metadata=auto_generate_missing_metadata,
                    metadata_inline_only_when_weak=metadata_inline_only_when_weak,
                    auto_write_generated_sidecars=auto_write_generated_sidecars,
                    metadata_inline_overwrite_weak_sidecars=metadata_inline_overwrite_weak_sidecars,
                    metadata_inline_generator=metadata_inline_generator,
                    metadata_openai_model=metadata_openai_model,
                    metadata_openai_timeout=metadata_openai_timeout,
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
                defer_publish=defer_publish,
                auto_rebuild_on_incompatible_update=auto_rebuild_on_incompatible_update,
                sync_collections=collection_sync_enabled,
                verify_collections=verify_collections,
                enforce_family_collection_membership=enforce_family_collection_membership,
                collection_removal_mode=collection_removal_mode,
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
                collection_image_src=launch_row.collection_image_src,
                collection_sort_order=launch_row.collection_sort_order,
                secondary_collection_handles=launch_row.secondary_collection_handles,
                progress=progress_tracker,
            )
            if summary.failures > before_failures:
                if fail_fast:
                    stop_requested = True
                    break
                if stop_after_failures > 0 and summary.failures >= stop_after_failures:
                    stop_requested = True
                    break
    else:
        if generated_template_routing:
            artwork_by_resolved = {str(artwork.src_path.resolve()): artwork for artwork in artworks}
            artwork_by_name = {artwork.src_path.name: artwork for artwork in artworks}
            template_by_key = {template.key: template for template in templates}
            logged_route_skips: Set[Tuple[str, int, str]] = set()
            for route in generated_template_routing:
                if batch_size > 0 and combinations_processed >= batch_size:
                    stop_requested = True
                    break
                template = template_by_key.get(route.template_key)
                if template is None:
                    skip_key = (route.template_key, int(route.concept_index), route.asset_path.name)
                    if skip_key not in logged_route_skips:
                        logged_route_skips.add(skip_key)
                        logger.warning(
                            "Prompt-art route excluded template=%s concept=%s asset=%s reason=template_not_runnable_after_preflight",
                            route.template_key,
                            route.concept_index,
                            route.asset_path.name,
                        )
                        run_rows.append(
                            RunReportRow(
                                timestamp=datetime.now(timezone.utc).isoformat(),
                                artwork_filename=route.asset_path.name,
                                artwork_slug=slugify(route.asset_path.stem),
                                template_key=route.template_key,
                                status="skipped",
                                action="prompt_art_route_skip",
                                blueprint_id=0,
                                provider_id=0,
                                upload_strategy=upload_strategy,
                                product_id="",
                                publish_attempted=False,
                                publish_verified=False,
                                rendered_title="",
                                reason_code="template_not_runnable_after_preflight",
                                routed_asset_family=route.asset_family or route.family,
                                routed_asset_mode=route.routing_strategy,
                                template_family=route.family,
                                eligibility_outcome="ineligible",
                                eligibility_reason_code="template_not_runnable_after_preflight",
                                eligibility_rule_failed="asset_routing",
                                eligibility_gate_stage="routing_gate",
                            )
                        )
                    continue
                resolved_key = str(route.asset_path.resolve())
                artwork = artwork_by_resolved.get(resolved_key) or artwork_by_name.get(route.asset_path.name)
                if artwork is None:
                    skip_key = (route.template_key, int(route.concept_index), route.asset_path.name)
                    if skip_key not in logged_route_skips:
                        logged_route_skips.add(skip_key)
                        logger.warning(
                            "Prompt-art route excluded template=%s concept=%s asset=%s reason=routed_asset_not_discovered",
                            route.template_key,
                            route.concept_index,
                            route.asset_path.name,
                        )
                        run_rows.append(
                            RunReportRow(
                                timestamp=datetime.now(timezone.utc).isoformat(),
                                artwork_filename=route.asset_path.name,
                                artwork_slug=slugify(route.asset_path.stem),
                                template_key=route.template_key,
                                status="skipped",
                                action="prompt_art_route_skip",
                                blueprint_id=template.printify_blueprint_id if template else 0,
                                provider_id=template.printify_print_provider_id if template else 0,
                                upload_strategy=upload_strategy,
                                product_id="",
                                publish_attempted=False,
                                publish_verified=False,
                                rendered_title="",
                                reason_code="routed_asset_not_discovered",
                                routed_asset_family=route.asset_family or route.family,
                                routed_asset_mode=route.routing_strategy,
                                template_family=route.family,
                                eligibility_outcome="ineligible",
                                eligibility_reason_code="routed_asset_not_discovered",
                                eligibility_rule_failed="asset_routing",
                                eligibility_gate_stage="routing_gate",
                            )
                        )
                    continue
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
                    defer_publish=defer_publish,
                    auto_rebuild_on_incompatible_update=auto_rebuild_on_incompatible_update,
                    sync_collections=collection_sync_enabled,
                    verify_collections=verify_collections,
                    enforce_family_collection_membership=enforce_family_collection_membership,
                    collection_removal_mode=collection_removal_mode,
                    summary=summary,
                    failure_rows=failure_rows,
                    run_rows=run_rows,
                    routed_asset_family=route.asset_family or route.family,
                    routed_asset_mode=route.routing_strategy or "family-aware",
                    progress=progress_tracker,
                )
                if summary.failures > before_failures:
                    if fail_fast:
                        stop_requested = True
                        break
                    if stop_after_failures > 0 and summary.failures >= stop_after_failures:
                        stop_requested = True
                        break
        for chunk_index, artwork_chunk in enumerate(artwork_chunks, start=1):
            for artwork in artwork_chunk:
                if generated_template_routing:
                    break
                templates_for_artwork: List[ProductTemplate] = []
                for template in templates:
                    if batch_size > 0 and combinations_processed >= batch_size:
                        stop_requested = True
                        break
                    if resume and is_state_key_successful(state, f"{artwork.slug}:{template.key}"):
                        summary.resumed_combinations += 1
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
                        defer_publish=defer_publish,
                        auto_rebuild_on_incompatible_update=auto_rebuild_on_incompatible_update,
                        sync_collections=collection_sync_enabled,
                        verify_collections=verify_collections,
                        enforce_family_collection_membership=enforce_family_collection_membership,
                        collection_removal_mode=collection_removal_mode,
                        summary=summary,
                        failure_rows=failure_rows,
                        run_rows=run_rows,
                        progress=progress_tracker,
                    )
                    if template_spacing_ms > 0:
                        time.sleep(template_spacing_ms / 1000.0)
                    if summary.failures > before_failures:
                        if fail_fast:
                            stop_requested = True
                            break
                        if stop_after_failures > 0 and summary.failures >= stop_after_failures:
                            stop_requested = True
                            break
                if artwork_spacing_ms > 0:
                    time.sleep(artwork_spacing_ms / 1000.0)
                if stop_requested:
                    break
            summary.chunks_completed = chunk_index
            save_json_atomic(state_path, state)
            if pause_between_chunks_seconds > 0 and chunk_index < len(artwork_chunks):
                time.sleep(max(0.0, float(pause_between_chunks_seconds)))
            if stop_requested:
                break

    publish_stats = {"total": 0, "pending": 0, "completed": 0, "failed": 0, "rate_limited": 0, "processed": 0}
    if not defer_publish:
        publish_stats = process_publish_queue(
            printify=printify,
            state=state,
            templates_by_key=templates_by_key,
            publish_batch_size=publish_batch_size,
            pause_between_publish_batches_seconds=pause_between_publish_batches_seconds,
        )
    else:
        queue_counts = summarize_publish_queue(state)
        publish_stats["total"] = queue_counts["total"]
        publish_stats["pending"] = queue_counts["pending"]
        publish_stats["completed"] = queue_counts["completed"]
        publish_stats["failed"] = queue_counts["failed"]

    summary.combinations_processed = len(run_rows)
    summary.combinations_success = sum(1 for row in run_rows if row.status == "success")
    summary.combinations_failed = sum(1 for row in run_rows if row.status == "failure")
    summary.combinations_skipped = sum(1 for row in run_rows if row.status == "skipped")
    if getattr(printify, "catalog_cache", None):
        summary.catalog_cache_hits = printify.catalog_cache.stats.hits
        summary.catalog_cache_misses = printify.catalog_cache.stats.misses
        summary.catalog_requests_avoided = printify.catalog_cache.stats.requests_avoided
    summary.publish_queue_total_count = publish_stats.get("total", 0)
    summary.publish_queue_pending_count = publish_stats["pending"]
    summary.publish_queue_completed_count = publish_stats["completed"]
    summary.publish_queue_failed_count = publish_stats["failed"]
    summary.publish_rate_limit_events += publish_stats["rate_limited"]
    summary.rate_limit_events = dict(getattr(printify, "rate_limit_events", {}))
    progress_tracker.finish()

    if export_failure_report:
        write_csv_report(pathlib.Path(export_failure_report), [row.__dict__ for row in failure_rows])
        logger.info("Failure report exported path=%s rows=%s", export_failure_report, len(failure_rows))
    if export_run_report:
        write_csv_report(pathlib.Path(export_run_report), [row.__dict__ for row in run_rows])
        logger.info("Run report exported path=%s rows=%s", export_run_report, len(run_rows))

    save_json_atomic(state_path, state)
    log_run_summary(summary)
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
    if args.defer_publish and args.publish:
        raise SystemExit("--defer-publish and --publish cannot be used together")
    if args.publish:
        publish_mode = "publish"
    elif args.skip_publish:
        publish_mode = "skip"
    effective_max_artworks = args.local_image_batch if args.local_image_batch > 0 else args.max_artworks
    try:
        run(
            pathlib.Path(args.templates),
            dry_run=args.dry_run,
            force=args.force,
            allow_upscale=args.allow_upscale,
            upscale_method=args.upscale_method,
            auto_wallart_master=args.auto_wallart_master,
            skip_undersized=args.skip_undersized,
            image_dir=pathlib.Path(args.image_dir),
            export_dir=pathlib.Path(args.export_dir),
            state_path=pathlib.Path(args.state_path),
            skip_audit=args.skip_audit,
            max_artworks=effective_max_artworks,
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
            max_retry_sleep_seconds=args.max_retry_sleep_seconds,
            interactive_retry_cap_seconds=args.interactive_retry_cap_seconds,
            auto_rebuild_on_incompatible_update=args.auto_rebuild_on_incompatible_update,
            sync_collections=args.sync_collections,
            skip_collections=args.skip_collections,
            verify_collections=args.verify_collections,
            enforce_family_collection_membership=args.enforce_family_collection_membership,
            collection_removal_mode=args.collection_removal_mode,
            inspect_state_key_value=args.inspect_state_key,
            list_state_keys_only=args.list_state_keys,
            list_failures_only=args.list_failures,
            list_pending_only=args.list_pending,
            export_failure_report=args.export_failure_report,
            export_run_report=args.export_run_report,
            export_preflight_report_path=args.export_preflight_report,
            run_free_shipping_profit_audit_only=args.run_free_shipping_profit_audit,
            free_shipping_profit_min=args.free_shipping_profit_min,
            free_shipping_profit_audit_csv_path=args.free_shipping_profit_audit_csv,
            free_shipping_profit_audit_json_path=args.free_shipping_profit_audit_json,
            allow_preflight_failures=args.allow_preflight_failures,
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
            auto_generate_missing_metadata=args.auto_generate_missing_metadata,
            auto_write_generated_sidecars=args.auto_write_generated_sidecars,
            metadata_inline_generator=args.metadata_inline_generator,
            metadata_inline_only_when_weak=args.metadata_inline_only_when_weak,
            metadata_inline_overwrite_weak_sidecars=args.metadata_inline_overwrite_weak_sidecars,
            enable_ai_product_copy=args.enable_ai_product_copy,
            ai_product_copy_model=args.ai_product_copy_model,
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
            art_openai_timeout_seconds=args.art_openai_timeout_seconds,
            art_openai_max_retries=args.art_openai_max_retries,
            art_openai_retry_backoff_seconds=args.art_openai_retry_backoff_seconds,
            art_openai_size=args.art_openai_size,
            art_openai_portrait_size=args.art_openai_portrait_size,
            art_openai_square_size=args.art_openai_square_size,
            art_openai_landscape_size=args.art_openai_landscape_size,
            art_run_metadata=args.art_run_metadata,
            art_run_storefront_qa=args.art_run_storefront_qa,
            art_publish=args.art_publish,
            art_verify_publish=args.art_verify_publish,
            art_target_mode=args.art_target_mode,
            art_family_aware=args.art_family_aware,
            art_family_mode=args.art_family_mode,
            art_generate_poster_master=args.art_generate_poster_master,
            art_generate_apparel_master=args.art_generate_apparel_master,
            art_mug_tote_master=args.art_mug_tote_master,
            art_apparel_style=args.art_apparel_style,
            art_poster_style=args.art_poster_style,
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
            catalog_cache_dir=args.catalog_cache_dir,
            catalog_cache_ttl_hours=args.catalog_cache_ttl_hours,
            no_catalog_cache=args.no_catalog_cache,
            chunk_size=args.chunk_size,
            pause_between_chunks_seconds=args.pause_between_chunks_seconds,
            catalog_request_spacing_ms=args.catalog_request_spacing_ms,
            template_spacing_ms=args.template_spacing_ms,
            artwork_spacing_ms=args.artwork_spacing_ms,
            resume_only_pending=args.resume_only_pending,
            high_volume_mode=args.high_volume_mode,
            publish_batch_size=args.publish_batch_size,
            pause_between_publish_batches_seconds=args.pause_between_publish_batches_seconds,
            defer_publish=args.defer_publish,
            resume_publish_only=args.resume_publish_only,
            progress=args.progress,
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
