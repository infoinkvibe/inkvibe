import base64
import csv
import json
import logging
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Lightweight stubs so tests run without external deps installed in CI sandbox.
sys.modules.setdefault("requests", types.SimpleNamespace(Session=lambda: None))
sys.modules.setdefault("dotenv", types.SimpleNamespace(load_dotenv=lambda: None))

import pytest
from PIL import Image

import product_copy_generator
from r2_uploader import build_r2_public_url

from printify_shopify_sync_pipeline import (
    BaseApiClient,
    Artwork,
    ArtworkProcessingOptions,
    PlacementRequirement,
    ProductTemplate,
    PreparedArtwork,
    TemplateValidationError,
    InsufficientArtworkResolutionError,
    choose_upload_strategy,
    classify_failure,
    _compute_backoff,
    choose_variants_from_catalog,
    choose_variants_from_catalog_with_diagnostics,
    ensure_state_shape,
    normalize_printify_price,
    compute_sale_price_minor,
    compute_compare_at_price_minor,
    filename_slug_to_title,
    render_product_description,
    render_product_title,
    summarize_upload_strategy,
    load_templates,
    select_templates,
    search_blueprints,
    filter_providers,
    score_provider_for_template,
    generate_template_snippet,
    build_seo_context,
    resolve_product_action,
    normalize_catalog_variants_response,
    prepare_artwork_export,
    process_artwork,
    process_publish_queue,
    save_json_atomic,
    DryRunMutationSkipped,
    NonRetryableRequestError,
    RetryLimitExceededError,
    CatalogCliUsageError,
    run_catalog_cli,
    RunSummary,
    RunReportRow,
    list_state_keys,
    inspect_state_key,
    load_artwork_metadata,
    load_artwork_metadata_map,
    resolve_artwork_metadata_for_path,
    resolve_artwork_metadata_with_source,
    metadata_is_missing_or_weak,
    title_is_sluglike_or_generic,
    description_is_weak_fallback,
    sanitize_metadata_for_publish,
    persist_inline_metadata_sidecar,
    apply_variant_margin_guardrails,
    filename_title_quality_reason,
    resolve_artwork_title,
    discover_artworks,
    _render_listing_tags,
    preview_listing_copy,
    title_semantically_includes_product_label,
    validate_printify_payload_consistency,
    assess_update_compatibility,
    enforce_variant_safety_limit,
    upsert_in_printify,
    _is_printify_update_incompatible_error,
    _is_printify_product_edit_disabled_error,
    _row_status,
    write_csv_report,
    run,
    summarize_publish_queue,
    format_run_summary,
    template_blueprint_type_warning,
    generate_mug_template_snippet,
    parse_launch_plan_csv,
    compute_placement_transform_for_artwork,
    resolve_tote_template_catalog_mapping,
    export_launch_plan_from_images,
    resolve_launch_plan_rows,
    build_resolved_template,
    build_printify_product_payload,
    normalize_printify_transform,
    resolve_artwork_for_placement,
    evaluate_artwork_eligibility_for_template,
    list_eligible_templates_for_artwork,
    _resolve_trim_bounds_settings,
    build_shopify_product_options,
    validate_storefront_title,
    validate_storefront_description,
    validate_storefront_tags,
    validate_storefront_pricing,
    validate_storefront_options,
    validate_storefront_mockups,
    build_storefront_qa_row,
    run_storefront_qa,
    run_artwork_metadata_generation,
    sync_shopify_collection,
    _extract_numeric_shopify_id,
    PromptArtworkGenerationResult,
    normalize_theme_tag,
    extract_theme_signal_candidates,
    choose_best_theme_signal,
    choose_preferred_featured_variant_color,
    choose_preferred_featured_mockup_candidate,
    resolve_family_collection_target,
    select_provider_for_template,
    preflight_active_templates,
    validate_catalog_family_schema,
    PRODUCTION_BASELINE_TEMPLATE_KEYS,
    CatalogCache,
    PrintifyClient,
    should_enable_progress,
    apply_high_volume_mode_defaults,
    configure_ai_product_copy,
)
from artwork_metadata_generator import (
    CompositeArtworkMetadataGenerator,
    GeneratedArtworkMetadata,
    GeneratedArtworkMetadataCandidate,
    HeuristicArtworkMetadataGenerator,
    LocalVisionAnalyzer,
    MetadataReviewDecision,
    MetadataGeneratorMode,
    OpenAiArtworkMetadataGenerator,
    OpenAiVisionAnalyzer,
    VisionAnalysis,
    VisionArtworkMetadataGenerator,
    build_metadata_review_row,
    evaluate_generated_metadata,
    export_metadata_review_csv,
    should_write_sidecar,
    should_auto_approve_metadata,
    write_artwork_sidecar,
    preview_generated_metadata,
    select_artwork_metadata_generator,
)
from artwork_generation import (
    APPAREL_FAMILY,
    POSTER_FAMILY,
    ArtworkGenerationRequest,
    GeneratedArtworkAsset,
    build_generation_prompt,
    choose_generation_aspect_modes,
    generate_artwork_with_openai,
    is_preview_or_low_value_asset,
    plan_family_artwork_targets,
    plan_generated_artwork_targets,
    route_templates_to_generated_assets,
    TemplateAssetRouting,
    validate_generated_asset_for_templates,
)


class DummyPrintify:
    dry_run = True

    def list_variants(self, blueprint_id, provider_id):
        return [{"id": 1, "is_available": True, "options": {"color": "Black", "size": "M"}, "price": 1200}]

    def upload_image(self, file_path):
        return {"id": "upload-1"}


class StubVisionAnalyzer:
    name = "stub_vision"

    def __init__(self, analysis: VisionAnalysis | None):
        self.analysis = analysis

    def analyze_image(self, image_path: Path) -> VisionAnalysis | None:
        return self.analysis


class StubOpenAiClient:
    def __init__(self, output_text: str = "", error: Exception | None = None):
        self.output_text = output_text
        self.error = error
        self.responses = self

    def create(self, **kwargs):
        if self.error:
            raise self.error
        return types.SimpleNamespace(output_text=self.output_text)


class StubMetadataGenerator:
    name = "stub_review_generator"

    def __init__(self, candidates):
        self._candidates = candidates

    def generate_metadata_for_artwork(self, image_path: Path):
        return self._candidates[image_path.name]


def test_template_validation_rejects_missing_fields(tmp_path: Path):
    config = tmp_path / "product_templates.json"
    config.write_text(json.dumps([{"key": "t1"}]), encoding="utf-8")
    with pytest.raises(TemplateValidationError):
        load_templates(config)


def test_save_json_atomic_roundtrip(tmp_path: Path):
    path = tmp_path / "state.json"
    data = {"processed": {"a": 1}}
    save_json_atomic(path, data)
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded == data


def test_ensure_state_shape():
    shaped = ensure_state_shape({})
    assert set(["processed", "uploads", "shopify", "printify"]).issubset(shaped.keys())


def test_backoff_increases():
    assert _compute_backoff(3) >= _compute_backoff(1)


def _template_for_variant_tests() -> ProductTemplate:
    return ProductTemplate(
        key="test",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black"],
        enabled_sizes=["M"],
    )


def test_normalize_catalog_variants_response_accepts_raw_list():
    variants = [{"id": 1}, {"id": 2}]
    normalized = normalize_catalog_variants_response(variants)
    assert normalized == variants


def test_choose_variants_from_catalog_accepts_wrapped_dict_shape():
    template = _template_for_variant_tests()
    wrapped = {
        "variants": [
            {"id": 10, "is_available": True, "options": {"color": "Black", "size": "M"}},
            {"id": 11, "is_available": True, "options": {"color": "White", "size": "M"}},
        ]
    }
    chosen = choose_variants_from_catalog(wrapped, template)
    assert [v["id"] for v in chosen] == [10]



def test_choose_variants_from_catalog_ignores_color_filter_when_color_dimension_missing(caplog):
    template = ProductTemplate(
        key="mug_11oz",
        printify_blueprint_id=68,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["White"],
        enabled_sizes=["11oz"],
    )
    variants = [
        {"id": 21, "is_available": True, "options": {"size": "11oz"}},
        {"id": 22, "is_available": False, "options": {"size": "11oz"}},
    ]

    with caplog.at_level("WARNING"):
        chosen = choose_variants_from_catalog(variants, template)

    assert [v["id"] for v in chosen] == [21]
    assert "specifies enabled_colors" in caplog.text
    assert "blueprint 68/provider 1" in caplog.text


def test_choose_variants_from_catalog_ignores_size_filter_when_size_dimension_missing(caplog):
    template = ProductTemplate(
        key="poster",
        printify_blueprint_id=700,
        printify_print_provider_id=44,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black"],
        enabled_sizes=["18x24"],
    )
    variants = [
        {"id": 31, "is_available": True, "options": {"color": "Black"}},
        {"id": 32, "is_available": True, "options": {"color": "White"}},
    ]

    with caplog.at_level("WARNING"):
        chosen = choose_variants_from_catalog(variants, template)

    assert [v["id"] for v in chosen] == [31]
    assert "specifies enabled_sizes" in caplog.text
    assert "blueprint 700/provider 44" in caplog.text


def test_choose_variants_from_catalog_ignores_color_and_size_when_both_dimensions_missing(caplog):
    template = ProductTemplate(
        key="single_variant",
        printify_blueprint_id=900,
        printify_print_provider_id=2,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["White"],
        enabled_sizes=["One Size"],
    )
    variants = [
        {"id": 41, "is_available": True, "options": {}},
        {"id": 42, "is_available": False, "options": {}},
    ]

    with caplog.at_level("WARNING"):
        chosen = choose_variants_from_catalog(variants, template)

    assert [v["id"] for v in chosen] == [41]
    assert "specifies enabled_colors" in caplog.text
    assert "specifies enabled_sizes" in caplog.text


def test_choose_variants_from_catalog_keeps_strict_filtering_when_dimensions_exist():
    template = ProductTemplate(
        key="tee",
        printify_blueprint_id=6,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black"],
        enabled_sizes=["M"],
    )
    variants = [
        {"id": 51, "is_available": True, "options": {"color": "Black", "size": "M"}},
        {"id": 52, "is_available": True, "options": {"color": "White", "size": "M"}},
        {"id": 53, "is_available": True, "options": {"color": "Black", "size": "L"}},
        {"id": 54, "is_available": True, "options": {"color": "White", "size": "L"}},
    ]

    chosen = choose_variants_from_catalog(variants, template)

    assert [v["id"] for v in chosen] == [51]


def test_normalize_catalog_variants_response_rejects_malformed_string():
    with pytest.raises(ValueError, match="got type=str"):
        normalize_catalog_variants_response("bad payload")


def test_normalize_catalog_variants_response_rejects_malformed_dict():
    with pytest.raises(ValueError) as exc:
        normalize_catalog_variants_response({"unexpected": []})
    assert "dict keys=['unexpected']" in str(exc.value)


def _create_artwork(tmp_path: Path, width: int, height: int) -> Artwork:
    path = tmp_path / "art.png"
    Image.new("RGBA", (width, height), (255, 0, 0, 128)).save(path)
    return Artwork(
        slug="art",
        src_path=path,
        title="Art",
        description_html="<p>Art</p>",
        tags=[],
        image_width=width,
        image_height=height,
    )


def test_strict_default_failure_on_undersized_image(tmp_path: Path):
    artwork = _create_artwork(tmp_path, 495, 504)
    template = _template_for_variant_tests()
    placement = PlacementRequirement("front", 4500, 5400, artwork_fit_mode="cover")
    with pytest.raises(ValueError, match="image too small"):
        prepare_artwork_export(artwork, template, placement, tmp_path / "exports", ArtworkProcessingOptions())


def test_skip_behavior_with_skip_undersized(tmp_path: Path):
    artwork = _create_artwork(tmp_path, 495, 504)
    template = _template_for_variant_tests()
    placement = PlacementRequirement("front", 4500, 5400, artwork_fit_mode="cover")
    result = prepare_artwork_export(
        artwork,
        template,
        placement,
        tmp_path / "exports",
        ArtworkProcessingOptions(skip_undersized=True),
    )
    assert result is None


def test_upscale_behavior_with_allow_upscale(tmp_path: Path):
    artwork = _create_artwork(tmp_path, 495, 504)
    template = _template_for_variant_tests()
    placement = PlacementRequirement("front", 4500, 5400, artwork_fit_mode="cover")
    result = prepare_artwork_export(
        artwork,
        template,
        placement,
        tmp_path / "exports",
        ArtworkProcessingOptions(allow_upscale=True, upscale_method="nearest"),
    )
    assert result is not None
    with Image.open(result.export_path) as exported:
        assert exported.size == (4500, 5400)


def test_force_reprocesses_completed_state(tmp_path: Path):
    artwork = _create_artwork(tmp_path, 1200, 1200)
    template = ProductTemplate(
        key="force-template",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black"],
        enabled_sizes=["M"],
        placements=[PlacementRequirement("front", 1000, 1000)],
    )
    state = ensure_state_shape({"processed": {"art": {"completed": True, "products": []}}})
    process_artwork(
        printify=DummyPrintify(),
        shopify=None,
        shop_id=None,
        artwork=artwork,
        templates=[template],
        state=state,
        force=True,
        export_dir=tmp_path / "exports",
        state_path=tmp_path / "state.json",
        artwork_options=ArtworkProcessingOptions(),
        upload_strategy="auto",
        r2_config=None,
    )
    assert len(state["processed"]["art"]["products"]) == 1


def test_normalize_printify_price_to_minor_units():
    assert normalize_printify_price(2499) == 2499
    assert normalize_printify_price(24.99) == 2499
    assert normalize_printify_price("24.99") == 2499


def test_normalize_printify_price_rejects_invalid_values():
    with pytest.raises(ValueError):
        normalize_printify_price("not-a-price")


def test_no_retry_on_400_validation_error():
    class DummyResponse:
        status_code = 400
        headers = {}
        content = b"{}"

        def json(self):
            return {"errors": ["variants.0.price must be an integer"]}

        @property
        def text(self):
            return '{"errors":["variants.0.price must be an integer"]}'

    class DummySession:
        def __init__(self):
            self.calls = 0
            self.headers = {}

        def request(self, **kwargs):
            self.calls += 1
            return DummyResponse()

    client = BaseApiClient.__new__(BaseApiClient)
    client.base_url = "https://example.test"
    client.dry_run = False
    client.session = DummySession()
    with pytest.raises(Exception, match="HTTP 400"):
        client.post("/products", payload={})
    assert client.session.calls == 1


def test_upload_strategy_selection():
    assert choose_upload_strategy(1024, "auto", None) == "direct"
    assert choose_upload_strategy(6 * 1024 * 1024, "auto", object()) == "r2_url"
    with pytest.raises(RuntimeError):
        choose_upload_strategy(6 * 1024 * 1024, "auto", None)


def test_r2_public_url_generation():
    url = build_r2_public_url("https://pub.example.r2.dev", "inkvibe/art/key.png")
    assert url == "https://pub.example.r2.dev/inkvibe/art/key.png"


def test_dry_run_r2_url_upload_records_metadata(tmp_path: Path):
    from printify_shopify_sync_pipeline import upload_assets_to_printify
    from r2_uploader import R2Config

    class DummyDryRunPrintify:
        dry_run = True

        def upload_image(self, *, file_path=None, image_url=None):
            raise DryRunMutationSkipped("dry run")

    export_dir = tmp_path / "exports"
    export_dir.mkdir()
    export_path = export_dir / "asset.png"
    Image.new("RGBA", (5000, 5000), (1, 2, 3, 255)).save(export_path)

    artwork = Artwork(
        slug="art",
        src_path=export_path,
        title="Art",
        description_html="",
        tags=[],
        image_width=5000,
        image_height=5000,
    )
    template = ProductTemplate(
        key="t",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[PlacementRequirement("front", 5000, 5000)],
    )
    prepared = [
        PreparedArtwork(
            artwork=artwork,
            template=template,
            placement=template.placements[0],
            export_path=export_path,
            width_px=5000,
            height_px=5000,
        )
    ]

    state = {"uploads": {}}
    result = upload_assets_to_printify(
        DummyDryRunPrintify(),
        state,
        artwork,
        template,
        prepared,
        tmp_path / "state.json",
        "r2_url",
        R2Config("acct", "ak", "sk", "bucket", "https://pub.example.r2.dev"),
    )
    assert result["front"]["upload_strategy"] == "r2_url"
    assert result["front"]["r2_public_url"].startswith("https://pub.example.r2.dev/")


def test_filename_slug_to_title_cleanup():
    assert filename_slug_to_title("my_cool-design_v2_202412121200") == "My Cool Design"


def test_render_title_uses_clean_fallback(tmp_path: Path):
    src = tmp_path / "groovy_cat-print_v2_20241212.png"
    Image.new("RGBA", (1000, 1000), (255, 0, 0, 255)).save(src)
    art = Artwork(
        slug="groovy-cat-print-v2-20241212",
        src_path=src,
        title="groovy_cat-print_v2_20241212",
        description_html="",
        tags=[],
        image_width=1000,
        image_height=1000,
    )
    template = _template_for_variant_tests()
    template.title_pattern = "{artwork_title} Tee"
    assert render_product_title(template, art) == "Groovy Cat Print Tee"

def test_title_dedup_for_shirt_wording(tmp_path: Path):
    src = tmp_path / "signature_t-shirt.png"
    Image.new("RGBA", (1000, 1000), (255, 0, 0, 255)).save(src)
    art = Artwork(
        slug="signature",
        src_path=src,
        title="Signature T-Shirt",
        description_html="",
        tags=[],
        image_width=1000,
        image_height=1000,
        metadata={"title": "Signature T-Shirt"},
    )
    template = _template_for_variant_tests()
    template.product_type_label = "T-Shirt"
    template.title_pattern = "{artwork_title} {product_type_label}"
    assert render_product_title(template, art) == "Signature T-Shirt"


def test_title_dedup_for_mug_wording(tmp_path: Path):
    src = tmp_path / "cozy_mug.png"
    Image.new("RGBA", (1000, 1000), (255, 0, 0, 255)).save(src)
    art = Artwork(
        slug="cozy",
        src_path=src,
        title="Cozy Mug",
        description_html="",
        tags=[],
        image_width=1000,
        image_height=1000,
    )
    template = _template_for_variant_tests()
    template.product_type_label = "Mug"
    template.title_pattern = "{artwork_title} {product_type_label}"
    assert render_product_title(template, art) == "Cozy Mug"


def test_metadata_title_pattern_semantic_dedup(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.metadata = {"title": "Signature Tee"}
    template = _template_for_variant_tests()
    template.product_type_label = "T-Shirt"
    template.title_pattern = "{artwork_title} {product_type_label}"
    assert title_semantically_includes_product_label("Signature Tee", "T-Shirt") is True
    assert render_product_title(template, art) == "Signature Tee"


def test_sidecar_metadata_loading(tmp_path: Path):
    sidecar = tmp_path / "piece.json"
    sidecar.write_text(json.dumps({"title": "Aurora Bloom", "tags": ["Floral", "Spring"], "seo_keywords": "gift, botanical"}), encoding="utf-8")
    metadata = load_artwork_metadata(sidecar)
    assert metadata["title"] == "Aurora Bloom"
    assert metadata["tags"] == ["Floral", "Spring"]
    assert metadata["seo_keywords"] == ["gift", "botanical"]


def test_generated_artwork_metadata_preview_does_not_write(tmp_path: Path, capsys):
    image = tmp_path / "sample.png"
    Image.new("RGB", (640, 480), (230, 160, 90)).save(image)

    run_artwork_metadata_generation(
        image_dir=tmp_path,
        metadata_preview=True,
        write_sidecars=False,
        overwrite_sidecars=False,
        metadata_only_missing=True,
        metadata_max_artworks=0,
        metadata_output_dir="",
    )

    out = capsys.readouterr().out
    assert "sample.png" in out
    assert not image.with_suffix(".json").exists()


def test_generated_artwork_metadata_write_only_missing(tmp_path: Path):
    first = tmp_path / "first.png"
    second = tmp_path / "second.png"
    Image.new("RGB", (600, 600), (220, 180, 140)).save(first)
    Image.new("RGB", (600, 600), (40, 60, 100)).save(second)
    preserved = {"title": "Manual Title", "description": "Keep me", "tags": ["manual"]}
    second.with_suffix(".json").write_text(json.dumps(preserved), encoding="utf-8")

    run_artwork_metadata_generation(
        image_dir=tmp_path,
        metadata_preview=False,
        write_sidecars=True,
        overwrite_sidecars=False,
        metadata_only_missing=True,
        metadata_max_artworks=0,
        metadata_output_dir="",
    )

    generated_payload = json.loads(first.with_suffix(".json").read_text(encoding="utf-8"))
    preserved_payload = json.loads(second.with_suffix(".json").read_text(encoding="utf-8"))
    assert generated_payload["title"]
    assert generated_payload["title"].lower().startswith(("amber", "gold", "crimson", "ivory", "rose", "slate", "teal", "azure", "violet", "emerald", "midnight", "charcoal"))
    assert preserved_payload == preserved


def test_generated_artwork_metadata_overwrite_sidecars(tmp_path: Path):
    image = tmp_path / "override.png"
    sidecar = image.with_suffix(".json")
    Image.new("RGB", (500, 700), (35, 48, 75)).save(image)
    sidecar.write_text(json.dumps({"title": "Manual Keep"}), encoding="utf-8")

    run_artwork_metadata_generation(
        image_dir=tmp_path,
        metadata_preview=False,
        write_sidecars=True,
        overwrite_sidecars=True,
        metadata_only_missing=True,
        metadata_max_artworks=0,
        metadata_output_dir="",
    )

    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    assert payload["title"] != "Manual Keep"
    assert set(payload.keys()) == {
        "title",
        "subtitle",
        "description",
        "tags",
        "seo_keywords",
        "audience",
        "style_keywords",
        "theme",
        "collection",
        "occasion",
        "artist_note",
    }


def test_metadata_generator_strategy_selection_modes():
    heuristic = select_artwork_metadata_generator(mode=MetadataGeneratorMode.HEURISTIC.value)
    assert isinstance(heuristic, HeuristicArtworkMetadataGenerator)

    vision = select_artwork_metadata_generator(
        mode=MetadataGeneratorMode.VISION.value,
        vision_analyzer=StubVisionAnalyzer(
            VisionAnalysis(subject="lion", mood="dramatic", style_keywords=["wildlife"], confidence=0.95),
        ),
    )
    assert isinstance(vision, CompositeArtworkMetadataGenerator)

    auto = select_artwork_metadata_generator(mode=MetadataGeneratorMode.AUTO.value)
    assert isinstance(auto, CompositeArtworkMetadataGenerator)
    openai = select_artwork_metadata_generator(mode=MetadataGeneratorMode.OPENAI.value)
    assert isinstance(openai, CompositeArtworkMetadataGenerator)


def test_vision_generator_supports_injected_analyzer(tmp_path: Path):
    image = tmp_path / "lion.png"
    Image.new("RGB", (512, 512), (120, 90, 60)).save(image)
    generator = VisionArtworkMetadataGenerator(
        analyzer=StubVisionAnalyzer(
            VisionAnalysis(
                subject="lion",
                supporting_subjects=["savanna"],
                style_keywords=["wildlife", "painterly"],
                mood="dramatic",
                palette=["gold", "umber"],
                visible_text=[],
                confidence=0.93,
                rationale="subject detected: lion",
            )
        )
    )
    candidate = generator.generate_metadata_for_artwork(image)
    payload = candidate.metadata.as_sidecar_dict()
    assert "lion" in payload["title"].lower()
    assert "lion" in " ".join(payload["tags"]).lower()
    assert "vision_subject" in (candidate.source_signals or [])


def test_auto_metadata_generator_falls_back_to_heuristic(tmp_path: Path):
    image = tmp_path / "fallback.png"
    Image.new("RGB", (500, 500), (25, 30, 70)).save(image)
    generator = select_artwork_metadata_generator(
        mode=MetadataGeneratorMode.AUTO.value,
        vision_analyzer=StubVisionAnalyzer(None),
    )
    candidate = generator.generate_metadata_for_artwork(image)
    assert candidate.generator.startswith("auto:")
    assert "fallback" in (candidate.source_signals or [])
    assert candidate.metadata.title


def test_openai_analyzer_missing_api_key_degrades_safely(tmp_path: Path):
    image = tmp_path / "openai-missing-key.png"
    Image.new("RGB", (500, 500), (45, 40, 70)).save(image)
    analyzer = OpenAiVisionAnalyzer(api_key="", client=StubOpenAiClient())
    assert analyzer.analyze_image_with_metadata(image) is None


def test_openai_generator_supports_mocked_response(tmp_path: Path):
    image = tmp_path / "uuid-like-8321310.png"
    Image.new("RGB", (640, 640), (90, 120, 160)).save(image)
    payload = {
        "main_subject": "moonlit wolf",
        "supporting_subjects": ["pine forest"],
        "visible_design_text": ["stay wild"],
        "visual_style": ["painterly", "moody"],
        "mood": "moody",
        "color_story": ["midnight blue", "silver"],
        "likely_buyer_appeal": ["wildlife", "outdoors"],
        "title": "Moonlit Wolf",
        "subtitle": "Atmospheric wildlife artwork",
        "description": "A moonlit wolf stands at the edge of a pine forest in a calm, cinematic scene.",
        "tags": ["wolf", "wildlife art", "forest", "moody"],
        "seo_keywords": ["wolf wall art", "moonlit wolf print"],
        "audience": "Wildlife and outdoors decor fans",
        "style_keywords": ["painterly", "cinematic"],
        "theme": "Moonlit Wilderness",
        "collection": "Moonlit Wilderness",
        "occasion": "",
        "artist_note": "Generated from image-first OpenAI vision analysis.",
    }
    analyzer = OpenAiVisionAnalyzer(
        api_key="test-key",
        model="gpt-test",
        client=StubOpenAiClient(output_text=json.dumps(payload)),
    )
    generator = OpenAiArtworkMetadataGenerator(analyzer=analyzer)
    candidate = generator.generate_metadata_for_artwork(image)
    sidecar = candidate.metadata.as_sidecar_dict()
    assert "wolf" in sidecar["title"].lower()
    assert "wolf" in " ".join(sidecar["tags"]).lower()
    assert "openai_vision_subject" in (candidate.source_signals or [])
    preview = preview_generated_metadata([candidate])
    assert "openai_vision_subject" in preview
    assert "openai_vision_text" in preview


def test_auto_mode_prefers_openai_then_falls_back_to_vision_then_heuristic(tmp_path: Path):
    image = tmp_path / "lion-retro.png"
    Image.new("RGB", (640, 640), (120, 95, 80)).save(image)
    failing_openai = OpenAiVisionAnalyzer(api_key="test-key", client=StubOpenAiClient(error=RuntimeError("boom")))
    generator = select_artwork_metadata_generator(
        mode=MetadataGeneratorMode.AUTO.value,
        openai_analyzer=failing_openai,
    )
    candidate = generator.generate_metadata_for_artwork(image)
    assert candidate.generator.startswith("auto:openai:vision_subject")
    assert "lion" in candidate.metadata.title.lower()


def test_openai_mode_missing_key_can_fallback_to_heuristic(tmp_path: Path):
    image = tmp_path / "ai-generated-8321310.png"
    Image.new("RGB", (640, 480), (70, 100, 150)).save(image)
    generator = select_artwork_metadata_generator(mode=MetadataGeneratorMode.OPENAI.value)
    candidate = generator.generate_metadata_for_artwork(image)
    assert candidate.generator.startswith("openai:")
    assert "fallback" in (candidate.source_signals or [])
    assert candidate.metadata.title


def test_vision_mode_uses_local_subject_detection_from_filename(tmp_path: Path):
    image = tmp_path / "majestic-lion-retro.png"
    Image.new("RGB", (600, 600), (140, 90, 55)).save(image)
    generator = select_artwork_metadata_generator(mode=MetadataGeneratorMode.VISION.value)
    candidate = generator.generate_metadata_for_artwork(image)
    title = candidate.metadata.title.lower()
    assert "lion" in title
    assert candidate.generator.startswith("vision:")
    assert "fallback" not in (candidate.source_signals or [])
    assert "local_vision_subject" in (candidate.source_signals or [])


def test_auto_mode_prefers_subject_aware_over_heuristic_when_detected(tmp_path: Path):
    image = tmp_path / "wild-wolf-adventure-poster.png"
    Image.new("RGB", (640, 480), (80, 80, 90)).save(image)
    generator = select_artwork_metadata_generator(mode=MetadataGeneratorMode.AUTO.value)
    candidate = generator.generate_metadata_for_artwork(image)
    assert candidate.generator.startswith("auto:openai:vision_subject")
    assert "wolf" in candidate.metadata.title.lower()
    assert "heuristic_palette" not in (candidate.source_signals or [])


def test_metadata_preview_surfaces_generator_sources(tmp_path: Path, capsys):
    image = tmp_path / "preview-fallback.png"
    Image.new("RGB", (640, 480), (70, 80, 95)).save(image)
    run_artwork_metadata_generation(
        image_dir=tmp_path,
        metadata_preview=True,
        write_sidecars=False,
        overwrite_sidecars=False,
        metadata_only_missing=True,
        metadata_max_artworks=0,
        metadata_output_dir="",
        metadata_generator=MetadataGeneratorMode.AUTO.value,
    )
    out = capsys.readouterr().out
    assert "sources:" in out
    assert "heuristic_palette" in out


def test_metadata_preview_surfaces_vision_debug_signals(tmp_path: Path, capsys):
    image = tmp_path / "forest-lion-minimal.png"
    Image.new("RGB", (640, 480), (130, 110, 90)).save(image)
    generator = VisionArtworkMetadataGenerator(analyzer=LocalVisionAnalyzer())
    candidate = generator.generate_metadata_for_artwork(image)
    preview = preview_generated_metadata([candidate])
    print(preview)
    out = capsys.readouterr().out
    assert "detected_subject: lion" in out
    assert "confidence:" in out


def test_metadata_review_decision_auto_approved_for_high_confidence_subject(tmp_path: Path):
    image = tmp_path / "wolf.png"
    Image.new("RGB", (512, 512), (80, 70, 60)).save(image)
    candidate = GeneratedArtworkMetadataCandidate(
        image_path=image,
        sidecar_path=image.with_suffix(".json"),
        metadata=GeneratedArtworkMetadata(
            title="Moonlit Wolf Trail",
            subtitle="Wildlife nightscape",
            description="A moonlit wolf crossing a pine ridge with calm, cinematic atmosphere.",
            tags=["wolf", "wildlife", "forest", "night"],
            seo_keywords=["wolf print"],
            audience="wildlife decor fans",
            style_keywords=["cinematic"],
            theme="Moonlit Wilderness",
            collection="Moonlit Wilderness",
        ),
        generator="openai:openai_subject",
        source_signals=["openai_vision_subject"],
        debug_signals={"confidence": 0.97, "detected_subject": "wolf", "visible_text": []},
    )
    decision = evaluate_generated_metadata(candidate, min_confidence=0.9)
    assert decision.approval_status == "auto_approved"
    assert should_auto_approve_metadata(decision) is True


def test_metadata_review_decision_needs_review_for_low_confidence_generic(tmp_path: Path):
    image = tmp_path / "generic.png"
    Image.new("RGB", (512, 512), (90, 90, 90)).save(image)
    candidate = GeneratedArtworkMetadataCandidate(
        image_path=image,
        sidecar_path=image.with_suffix(".json"),
        metadata=GeneratedArtworkMetadata(
            title="Signature Product",
            description="Great gift idea.",
            tags=["art", "design", "print"],
        ),
        generator="openai:openai_subject",
        source_signals=["openai_vision_subject", "fallback"],
        debug_signals={"confidence": 0.52, "detected_subject": "", "visible_text": ["stay wild"]},
    )
    decision = evaluate_generated_metadata(candidate, min_confidence=0.9)
    assert decision.approval_status == "needs_review"
    assert "confidence_below_threshold" in decision.review_reasons


def test_metadata_review_csv_exports_rows(tmp_path: Path):
    image = tmp_path / "row.png"
    Image.new("RGB", (200, 200), (120, 100, 90)).save(image)
    candidate = GeneratedArtworkMetadataCandidate(
        image_path=image,
        sidecar_path=image.with_suffix(".json"),
        metadata=GeneratedArtworkMetadata(title="Forest Fox", description="A fox in forest light.", tags=["fox", "forest"]),
        generator="vision:vision_subject",
        source_signals=["local_vision_subject"],
        debug_signals={"confidence": 0.94, "detected_subject": "fox"},
    )
    decision = MetadataReviewDecision(approval_status="auto_approved", confidence=0.94, review_reasons=[], quality_flags=[], would_write_sidecar=True)
    row = build_metadata_review_row(candidate, decision)
    report = tmp_path / "review.csv"
    export_metadata_review_csv([row], report)
    text = report.read_text(encoding="utf-8")
    assert "artwork_filename" in text
    assert "row.png" in text
    assert "auto_approved" in text


def test_metadata_auto_approved_only_write_mode_skips_review_needed(tmp_path: Path, monkeypatch):
    high_image = tmp_path / "high.png"
    low_image = tmp_path / "low.png"
    Image.new("RGB", (512, 512), (120, 120, 120)).save(high_image)
    Image.new("RGB", (512, 512), (80, 80, 80)).save(low_image)
    candidates = {
        "high.png": GeneratedArtworkMetadataCandidate(
            image_path=high_image,
            sidecar_path=high_image.with_suffix(".json"),
            metadata=GeneratedArtworkMetadata(
                title="Golden Lion",
                description="A striking lion portrait with warm golden tones and textured brushwork.",
                tags=["lion", "wildlife", "golden"],
            ),
            generator="openai:openai_subject",
            source_signals=["openai_vision_subject"],
            debug_signals={"confidence": 0.96, "detected_subject": "lion", "visible_text": []},
        ),
        "low.png": GeneratedArtworkMetadataCandidate(
            image_path=low_image,
            sidecar_path=low_image.with_suffix(".json"),
            metadata=GeneratedArtworkMetadata(title="Signature Product", description="Great gift idea.", tags=["art", "design", "print"]),
            generator="openai:openai_subject",
            source_signals=["openai_vision_subject"],
            debug_signals={"confidence": 0.45, "detected_subject": "", "visible_text": []},
        ),
    }
    monkeypatch.setattr("printify_shopify_sync_pipeline.select_artwork_metadata_generator", lambda **kwargs: StubMetadataGenerator(candidates))

    report = tmp_path / "metadata-review.csv"
    run_artwork_metadata_generation(
        image_dir=tmp_path,
        metadata_preview=False,
        write_sidecars=True,
        overwrite_sidecars=False,
        metadata_only_missing=True,
        metadata_max_artworks=0,
        metadata_output_dir="",
        metadata_auto_approve=True,
        metadata_min_confidence=0.9,
        metadata_review_report=str(report),
        metadata_write_auto_approved_only=True,
        metadata_allow_review_writes=False,
    )

    assert high_image.with_suffix(".json").exists()
    assert not low_image.with_suffix(".json").exists()
    report_text = report.read_text(encoding="utf-8")
    assert "needs_review" in report_text
    assert "auto_approved" in report_text


def test_vision_subject_titles_are_not_generic_palette_labels(tmp_path: Path):
    image = tmp_path / "subject.png"
    Image.new("RGB", (512, 512), (200, 120, 80)).save(image)
    generator = VisionArtworkMetadataGenerator(
        analyzer=StubVisionAnalyzer(
            VisionAnalysis(
                subject="retro motorcycle",
                style_keywords=["retro", "coastal"],
                mood="bright",
                visible_text=["good vibes"],
                confidence=0.88,
            )
        )
    )
    candidate = generator.generate_metadata_for_artwork(image)
    title = candidate.metadata.title.lower()
    assert "midnight atmosphere" not in title
    assert "slate contrast" not in title
    assert any(token in title for token in ["retro", "motorcycle", "good vibes"])


def test_artwork_metadata_generator_helper_functions(tmp_path: Path):
    image = tmp_path / "helper.png"
    Image.new("RGB", (512, 512), (120, 200, 180)).save(image)
    generator = HeuristicArtworkMetadataGenerator()
    candidate = generator.generate_metadata_for_artwork(image)

    assert should_write_sidecar(image.with_suffix(".json"), overwrite_sidecars=False, only_missing=True) is True
    target = write_artwork_sidecar(candidate=candidate)
    assert target.exists()
    assert should_write_sidecar(target, overwrite_sidecars=False, only_missing=True) is False
    assert should_write_sidecar(target, overwrite_sidecars=True, only_missing=True) is True

    preview = preview_generated_metadata([candidate])
    assert "helper.png" in preview
    assert "title:" in preview


def test_artwork_metadata_map_loading_and_resolution(tmp_path: Path):
    mapping = tmp_path / "artwork_metadata_map.json"
    mapping.write_text(
        json.dumps(
            {
                "ai-generated-8321310": {
                    "art_title": "Golden Trail Wolf",
                    "short_description": "A lone wolf moving through warm mountain light.",
                    "tags": ["wolf", "wildlife"],
                    "subject": "wolf",
                    "mood": "golden-hour",
                }
            }
        ),
        encoding="utf-8",
    )
    image = tmp_path / "ai-generated-8321310.png"
    Image.new("RGBA", (900, 900), (0, 0, 0, 255)).save(image)
    metadata_map = load_artwork_metadata_map(mapping)
    resolved = resolve_artwork_metadata_for_path(image, metadata_map)
    assert resolved["title"] == "Golden Trail Wolf"
    assert resolved["description"].startswith("A lone wolf")
    assert "wolf" in resolved["tags"]


def test_sidecar_metadata_preferred_over_repo_map(tmp_path: Path):
    image = tmp_path / "ai-generated-8383720.png"
    sidecar = tmp_path / "ai-generated-8383720.json"
    Image.new("RGBA", (900, 900), (0, 0, 0, 255)).save(image)
    sidecar.write_text(json.dumps({"title": "Sidecar Title", "description": "From sidecar.", "tags": ["sidecar"]}), encoding="utf-8")
    metadata_map = {"ai-generated-8383720": {"title": "Map Title", "description": "From map.", "tags": ["map"]}}
    resolved = resolve_artwork_metadata_for_path(image, metadata_map)
    assert resolved["title"] == "Sidecar Title"
    assert resolved["tags"] == ["sidecar"]


def test_artwork_metadata_alias_match_for_uuid_filename(tmp_path: Path):
    image = tmp_path / "7a90f8b0-5687-4cbf-81a5-3eba04940253.png"
    Image.new("RGBA", (900, 900), (0, 0, 0, 255)).save(image)
    metadata_map = {
        "ai-generated-8321310": {
            "title": "Golden Trail Wolf",
            "description": "A lone wolf moving through warm mountain light.",
            "tags": ["wolf"],
            "aliases": ["7a90f8b0-5687-4cbf-81a5-3eba04940253"],
        }
    }
    resolved, match = resolve_artwork_metadata_with_source(image, metadata_map, artwork_slug="7a90f8b0-5687-4cbf-81a5-3eba04940253")
    assert resolved["title"] == "Golden Trail Wolf"
    assert match["source"] == "alias"
    assert match["key"] == "ai-generated-8321310"


def test_sidecar_still_wins_over_alias_match(tmp_path: Path):
    image = tmp_path / "7a90f8b0-5687-4cbf-81a5-3eba04940253.png"
    sidecar = tmp_path / "7a90f8b0-5687-4cbf-81a5-3eba04940253.json"
    Image.new("RGBA", (900, 900), (0, 0, 0, 255)).save(image)
    sidecar.write_text(json.dumps({"title": "Sidecar Wolf", "tags": ["sidecar"]}), encoding="utf-8")
    metadata_map = {
        "ai-generated-8321310": {
            "title": "Golden Trail Wolf",
            "tags": ["map"],
            "aliases": ["7a90f8b0-5687-4cbf-81a5-3eba04940253"],
        }
    }
    resolved, match = resolve_artwork_metadata_with_source(image, metadata_map, artwork_slug="7a90f8b0-5687-4cbf-81a5-3eba04940253")
    assert resolved["title"] == "Sidecar Wolf"
    assert match["source"] == "sidecar"


def test_artwork_metadata_ambiguous_alias_falls_back_safely(tmp_path: Path):
    image = tmp_path / "wolf-scene.png"
    Image.new("RGBA", (900, 900), (0, 0, 0, 255)).save(image)
    metadata_map = {
        "wolf-entry-a": {
            "title": "Wolf A",
            "tags": ["wolf"],
            "aliases": ["wolf-scene"],
        },
        "wolf-entry-b": {
            "title": "Wolf B",
            "tags": ["wolf"],
            "aliases": ["wolf-scene"],
        },
    }
    resolved, match = resolve_artwork_metadata_with_source(image, metadata_map, artwork_slug="wolf-scene")
    assert resolved == {}
    assert match["source"] == "ambiguous_alias"
    assert match["key"] == "wolf-scene"


def test_inline_metadata_helpers_detect_sluglike_and_generic_fallbacks(tmp_path: Path):
    image = tmp_path / "ai-generated-9228632.png"
    Image.new("RGBA", (900, 900), (80, 80, 80, 255)).save(image)

    weak_title, title_reasons = title_is_sluglike_or_generic("Ai Generated 9228632", artwork_path=image)
    assert weak_title is True
    assert "title_long_numeric_suffix" in title_reasons

    weak_description, description_reasons = description_is_weak_fallback("Great gift idea.")
    assert weak_description is True
    assert "description_too_short" in description_reasons

    is_weak, weak_reasons = metadata_is_missing_or_weak(
        {"title": "Chicken 6600568", "description": "Great gift idea."},
        artwork_path=image,
        metadata_source="fallback",
    )
    assert is_weak is True
    assert "title_long_numeric_suffix" in weak_reasons


def test_inline_generation_skips_strong_sidecar_metadata(tmp_path: Path, monkeypatch):
    image = tmp_path / "lion.png"
    Image.new("RGBA", (1000, 1000), (0, 0, 0, 255)).save(image)
    sidecar = image.with_suffix(".json")
    sidecar.write_text(json.dumps({"title": "Majestic Lion Portrait", "description": "A detailed lion portrait with bold contrast."}), encoding="utf-8")

    class FailIfUsed:
        def generate_metadata_for_artwork(self, image_path: Path):
            raise AssertionError("inline generator should not run for strong sidecar")

    monkeypatch.setattr("printify_shopify_sync_pipeline.select_artwork_metadata_generator", lambda **kwargs: FailIfUsed())
    artworks = discover_artworks(tmp_path)
    assert artworks[0].metadata["title"] == "Majestic Lion Portrait"
    assert artworks[0].metadata_generated_inline is False
    assert artworks[0].metadata_resolution_source == "sidecar"


def test_inline_generation_uses_generated_metadata_and_writes_sidecar(tmp_path: Path, monkeypatch):
    image = tmp_path / "ai-generated-9228632.png"
    Image.new("RGBA", (1000, 1000), (0, 0, 0, 255)).save(image)

    class StubInlineGenerator:
        name = "stub_inline"

        def generate_metadata_for_artwork(self, image_path: Path):
            return GeneratedArtworkMetadataCandidate(
                image_path=image_path,
                sidecar_path=image_path.with_suffix(".json"),
                metadata=GeneratedArtworkMetadata(
                    title="Fierce Monster Truck Jump",
                    description="A monster truck launches over fire ramps in a dramatic stadium night scene.",
                    tags=["monster truck", "racing"],
                ),
                generator="openai:openai_subject",
                source_signals=["openai_vision_subject"],
            )

    monkeypatch.setattr("printify_shopify_sync_pipeline.select_artwork_metadata_generator", lambda **kwargs: StubInlineGenerator())
    artworks = discover_artworks(tmp_path, metadata_inline_generator=MetadataGeneratorMode.AUTO.value)
    assert artworks[0].title == "Fierce Monster Truck Jump"
    assert artworks[0].metadata_generated_inline is True
    assert artworks[0].metadata_resolution_source == "inline_openai"
    assert artworks[0].metadata_sidecar_written is True
    payload = json.loads(image.with_suffix(".json").read_text(encoding="utf-8"))
    assert payload["title"] == "Fierce Monster Truck Jump"


def test_inline_generation_does_not_overwrite_existing_sidecar_by_default(tmp_path: Path, monkeypatch):
    image = tmp_path / "chicken-6600568.png"
    Image.new("RGBA", (1000, 1000), (0, 0, 0, 255)).save(image)
    sidecar = image.with_suffix(".json")
    sidecar.write_text(json.dumps({"title": "Chicken 6600568", "description": "Great gift idea."}), encoding="utf-8")

    class StubInlineGenerator:
        name = "stub_inline"

        def generate_metadata_for_artwork(self, image_path: Path):
            return GeneratedArtworkMetadataCandidate(
                image_path=image_path,
                sidecar_path=image_path.with_suffix(".json"),
                metadata=GeneratedArtworkMetadata(
                    title="Vintage Country Chicken Portrait",
                    description="A rustic chicken portrait with warm farm tones and playful retro character.",
                ),
                generator="vision:vision_subject",
            )

    monkeypatch.setattr("printify_shopify_sync_pipeline.select_artwork_metadata_generator", lambda **kwargs: StubInlineGenerator())
    artworks = discover_artworks(tmp_path, metadata_inline_generator=MetadataGeneratorMode.AUTO.value)
    assert artworks[0].metadata_generated_inline is True
    assert artworks[0].metadata_sidecar_written is True
    persisted = json.loads(sidecar.read_text(encoding="utf-8"))
    assert persisted["title"] == "Vintage Country Chicken Portrait"
    assert persisted["metadata_provenance"].startswith("inline_")


def test_inline_generation_does_not_rewrite_same_upgraded_sidecar(tmp_path: Path):
    image = tmp_path / "owly.png"
    Image.new("RGBA", (1000, 1000), (0, 0, 0, 255)).save(image)
    artwork = Artwork("owly", image, "Owly", "", [], 1000, 1000)
    candidate = {"title": "Night Owl Portrait", "description": "A moody owl portrait in moonlight.", "tags": ["owl", "night forest"]}
    wrote, reason = persist_inline_metadata_sidecar(
        artwork=artwork,
        candidate_metadata=candidate,
        generator_name="openai",
        weak_reasons=["title_slug_like"],
        metadata_source="sidecar",
    )
    assert wrote is True
    assert reason == "written"
    wrote_again, reason_again = persist_inline_metadata_sidecar(
        artwork=artwork,
        candidate_metadata=candidate,
        generator_name="openai",
        weak_reasons=["title_slug_like"],
        metadata_source="sidecar",
    )
    assert wrote_again is False
    assert reason_again == "unchanged_fingerprint"


def test_curated_sluglike_sidecar_not_forced_to_regenerate(tmp_path: Path):
    image = tmp_path / "sunset-over-lake.png"
    Image.new("RGBA", (1000, 1000), (0, 0, 0, 255)).save(image)
    is_weak, reasons = metadata_is_missing_or_weak(
        {
            "title": "sunset-over-lake-special-edition-print",
            "description": "A warm sunset scene over a calm alpine lake with detailed brush texture.",
            "tags": ["sunset lake", "landscape wall art", "alpine twilight"],
        },
        artwork_path=image,
        metadata_source="sidecar",
    )
    assert is_weak is False
    assert "title_slug_like" not in reasons


def test_metadata_sanitizer_strips_prompt_artifacts():
    cleaned = sanitize_metadata_for_publish(
        {
            "title": "Poster",
            "description": "Style notes: moody tones. Keywords: wolf, moon. Made for general gifting. A dynamic scene of a wolf at dusk.",
            "tags": ["wolf", "wolf", "keywords", "gift idea", "moonlit forest"],
        }
    )
    assert "Style notes:" not in cleaned["description"]
    assert "Keywords:" not in cleaned["description"]
    assert "Made for general" not in cleaned["description"]
    assert "dynamic scene of a" not in cleaned["description"].lower()
    assert "keywords" not in cleaned["tags"]
    assert cleaned["tags"].count("wolf") == 1


def test_inline_generation_uses_auto_generator_mode_by_default(tmp_path: Path, monkeypatch):
    image = tmp_path / "wolf-forest.png"
    Image.new("RGBA", (1000, 1000), (0, 0, 0, 255)).save(image)
    captured = {}

    class StubInlineGenerator:
        def generate_metadata_for_artwork(self, image_path: Path):
            return GeneratedArtworkMetadataCandidate(
                image_path=image_path,
                sidecar_path=image_path.with_suffix(".json"),
                metadata=GeneratedArtworkMetadata(
                    title="Golden Trail Wolf",
                    description="A lone wolf crossing a glowing forest trail at dusk.",
                ),
                generator="auto:openai:vision_subject",
            )

    def _select(**kwargs):
        captured["mode"] = kwargs.get("mode")
        return StubInlineGenerator()

    monkeypatch.setattr("printify_shopify_sync_pipeline.select_artwork_metadata_generator", _select)
    artworks = discover_artworks(tmp_path)
    assert artworks[0].metadata_generated_inline is True
    assert artworks[0].title == "Golden Trail Wolf"
    assert captured["mode"] == MetadataGeneratorMode.AUTO.value


def test_variant_margin_guardrails_reprice_disable_and_longsleeve_behavior():
    hoodie = ProductTemplate(
        key="hoodie_gildan",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        base_price="20.00",
        markup_type="fixed",
        markup_value="1.00",
        min_margin_after_shipping="3.00",
        target_margin_after_shipping="5.00",
        reprice_variants_to_margin_floor=True,
        disable_variants_below_margin_floor=True,
    )
    adjusted, report = apply_variant_margin_guardrails(
        hoodie,
        [{"id": 1, "price": 2100, "cost": 2000, "shipping": 700, "is_available": True}],
    )
    assert report["repriced_variant_ids"] == [1]
    assert adjusted and adjusted[0]["price"] >= 3200
    assert compute_sale_price_minor(hoodie, adjusted[0]) == adjusted[0]["price"]
    assert report["final_enabled_count"] == 1
    assert report["disabled_count_after_reprice"] == 0

    longsleeve = ProductTemplate(
        key="longsleeve_gildan",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        base_price="10.00",
        markup_type="fixed",
        markup_value="0.00",
        min_margin_after_shipping="4.00",
        target_margin_after_shipping="5.00",
        reprice_variants_to_margin_floor=False,
        disable_variants_below_margin_floor=True,
        mark_template_nonviable_if_needed=True,
    )
    adjusted_long, report_long = apply_variant_margin_guardrails(
        longsleeve,
        [{"id": 2, "price": 2499, "cost": 2499, "shipping": 700, "is_available": True}],
    )
    assert adjusted_long == []
    assert report_long["viable"] is False
    assert report_long["final_enabled_count"] == 0


def test_tote_guardrails_report_economics_and_ceiling_failure_reason():
    tote = ProductTemplate(
        key="tote_basic",
        printify_blueprint_id=609,
        printify_print_provider_id=74,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        base_price="24.99",
        markup_type="fixed",
        markup_value="5.00",
        min_margin_after_shipping="3.00",
        target_margin_after_shipping="5.00",
        max_allowed_price="29.99",
        reprice_variants_to_margin_floor=True,
        disable_variants_below_margin_floor=True,
    )
    adjusted, report = apply_variant_margin_guardrails(
        tote,
        [{"id": 21, "price": 2200, "cost": 2600, "shipping": 600, "is_available": True}],
    )
    assert adjusted == []
    assert report["viable"] is False
    assert report["failed_variant_reasons"][21] == "required_price_exceeds_max_allowed_price"
    diag = report["variant_diagnostics"][0]
    assert diag["original_sale_price_minor"] == 2999
    assert diag["repriced_sale_price_minor"] == 2999
    assert diag["printify_cost_minor"] == 2600
    assert diag["shipping_basis_used"] == "cost"
    assert diag["target_margin_after_shipping_minor"] == 500
    assert diag["min_margin_after_shipping_minor"] == 300
    assert diag["after_shipping_margin_before_reprice_minor"] == -201
    assert diag["after_shipping_margin_after_reprice_minor"] == -201
    assert diag["max_allowed_price_minor"] == 2999
    assert diag["failure_reason"] == "required_price_exceeds_max_allowed_price"
    assert report["failure_reason_counts"]["required_price_exceeds_max_allowed_price"] == 1


def test_apparel_guardrails_preserve_original_cost_when_price_is_repriced():
    tee = ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=6,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        base_price="19.99",
        markup_type="fixed",
        markup_value="0.00",
        min_margin_after_shipping="3.00",
        target_margin_after_shipping="5.00",
        reprice_variants_to_margin_floor=True,
        disable_variants_below_margin_floor=True,
    )
    adjusted, report = apply_variant_margin_guardrails(
        tee,
        [{"id": 31, "price": 1200, "shipping": 500, "is_available": True, "options": {"color": "Black", "size": "M"}}],
    )
    assert report["repriced_count"] == 1
    assert report["final_enabled_count"] == 1
    assert adjusted and adjusted[0]["price"] == 2200
    diag = report["variant_diagnostics"][0]
    assert diag["printify_cost_minor"] == 1200
    assert diag["after_shipping_margin_after_reprice_minor"] == 500


def test_guardrails_use_min_profit_after_shipping_alias_and_us_cheapest_shipping_rows():
    tee = ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=6,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        base_price="19.99",
        markup_type="fixed",
        markup_value="0.00",
        min_profit_after_shipping="4.00",
        target_margin_after_shipping="5.00",
        reprice_variants_to_margin_floor=True,
        disable_variants_below_margin_floor=True,
    )
    adjusted, report = apply_variant_margin_guardrails(
        tee,
        [{
            "id": 301,
            "price": 2000,
            "cost": 1800,
            "is_available": True,
            "shipping": [
                {"country": "CA", "first_item": 1200},
                {"country": "US", "first_item": 600},
                {"country": "US", "first_item": 500},
            ],
        }],
    )
    assert adjusted and adjusted[0]["price"] == 2800
    assert report["repriced_count"] == 1
    diag = report["variant_diagnostics"][0]
    assert diag["shipping_minor"] == 500
    assert diag["min_margin_after_shipping_minor"] == 400
    assert diag["target_margin_after_shipping_minor"] == 500


def test_color_expansion_uses_curated_allowlist_and_reports_unavailable():
    template = ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black", "White"],
        expanded_enabled_colors=["Dark Heather", "Military Green"],
        enabled_sizes=["M"],
        max_enabled_variants=10,
    )
    variants = [
        {"id": 1, "is_available": True, "options": {"color": "Black", "size": "M"}},
        {"id": 2, "is_available": True, "options": {"color": "Dark Heather", "size": "M"}},
    ]
    chosen, diagnostics = choose_variants_from_catalog_with_diagnostics(variants, template)
    assert [row["id"] for row in chosen] == [1, 2]
    assert diagnostics.selected_additional_colors == ["Dark Heather"]
    assert diagnostics.unavailable_additional_colors == ["Military Green"]


def test_sticker_template_remains_without_expanded_colors():
    templates = load_templates(Path("product_templates.json"))
    sticker = next(template for template in templates if template.key == "sticker_kisscut")
    assert sticker.expanded_enabled_colors == []


def test_should_enable_progress_respects_tty_and_overrides(monkeypatch: pytest.MonkeyPatch):
    class _Stream:
        def __init__(self, is_tty: bool):
            self._is_tty = is_tty

        def isatty(self):
            return self._is_tty

    monkeypatch.delenv("CI", raising=False)
    assert should_enable_progress(force_enable=None, stream=_Stream(True)) == (True, "interactive_tty")
    assert should_enable_progress(force_enable=None, stream=_Stream(False)) == (False, "non_tty")
    assert should_enable_progress(force_enable=True, stream=_Stream(False)) == (True, "forced_on")
    assert should_enable_progress(force_enable=False, stream=_Stream(True)) == (False, "forced_off")

def test_uuid_noisy_filename_detection():
    assert filename_title_quality_reason("8f6f45d4-c95f-4f68-9cf9-f022f5197a18") == "uuid_like"
    assert filename_title_quality_reason("2fd4e1c67a2d28fced849ee1bb76e7391b93eb12") == "hex_like"


def test_metadata_title_precedence(tmp_path: Path):
    src = tmp_path / "e9a47a8c-7f53-4f4f-857f-2d3fe6b5fbe9.png"
    Image.new("RGBA", (1000, 1000), (0, 0, 0, 255)).save(src)
    art = Artwork(
        slug="x",
        src_path=src,
        title="ignored",
        description_html="",
        tags=[],
        image_width=1000,
        image_height=1000,
        metadata={"title": "Golden Hour Daisies"},
    )
    template = _template_for_variant_tests()
    resolved = resolve_artwork_title(template, art)
    assert resolved.title_source == "metadata"
    assert resolved.cleaned_display_title == "Golden Hour Daisies"


def test_fallback_title_generation_for_noisy_filename(tmp_path: Path):
    src = tmp_path / "8f6f45d4-c95f-4f68-9cf9-f022f5197a18.png"
    Image.new("RGBA", (1000, 1000), (0, 0, 0, 255)).save(src)
    art = Artwork(
        slug="x",
        src_path=src,
        title="",
        description_html="",
        tags=[],
        image_width=1000,
        image_height=1000,
        metadata={"theme": "retro sunset"},
    )
    template = _template_for_variant_tests()
    template.product_type_label = "Graphic Tee"
    resolved = resolve_artwork_title(template, art)
    assert resolved.title_source == "fallback"
    assert resolved.cleaned_display_title == "Retro Sunset Graphic Tee"


def test_metadata_title_weak_phrase_uses_contextual_fallback(tmp_path: Path):
    src = tmp_path / "8f6f45d4-c95f-4f68-9cf9-f022f5197a18.png"
    Image.new("RGBA", (1000, 1000), (0, 0, 0, 255)).save(src)
    art = Artwork(
        slug="8f6f45d4-c95f-4f68-9cf9-f022f5197a18",
        src_path=src,
        title="",
        description_html="",
        tags=[],
        image_width=1000,
        image_height=1000,
        metadata={"title": "Signature Product", "theme": "sunset palms"},
    )
    template = _template_for_variant_tests()
    template.product_type_label = "Poster"
    resolved = resolve_artwork_title(template, art)
    assert resolved.title_source == "fallback"
    assert resolved.cleaned_display_title == "Sunset Palms Poster"


def test_noisy_filename_fallback_uses_slug_when_available(tmp_path: Path):
    src = tmp_path / "8f6f45d4-c95f-4f68-9cf9-f022f5197a18.png"
    Image.new("RGBA", (1000, 1000), (0, 0, 0, 255)).save(src)
    art = Artwork(
        slug="golden-trail-wolf",
        src_path=src,
        title="",
        description_html="",
        tags=[],
        image_width=1000,
        image_height=1000,
        metadata={},
    )
    template = _template_for_variant_tests()
    template.product_type_label = "Hoodie"
    resolved = resolve_artwork_title(template, art)
    assert resolved.title_source == "fallback"
    assert resolved.cleaned_display_title == "Golden Trail Wolf Hoodie"


def test_tag_merging_and_deduplication(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.tags = ["Gift", "Floral"]
    art.metadata = {"tags": ["floral", "spring"], "seo_keywords": ["gift", "garden party"]}
    template = _template_for_variant_tests()
    template.tags = ["spring", "Boho"]
    tags = _render_listing_tags(template, art)
    assert tags.count("gift") == 1
    assert tags.count("floral") == 1
    assert "boho" in tags


def test_tag_generation_prioritizes_family_and_theme_over_generic(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.slug = "misty-forest-wolf"
    art.metadata = {
        "theme": "forest wolf",
        "collection": "woodland stories",
        "occasion": "birthday gifting",
        "tags": ["forest wolf", "gift"],
        "style_keywords": ["rustic"],
    }
    template = _template_for_variant_tests()
    template.key = "poster_basic"
    template.product_type_label = "Poster"
    tags = _render_listing_tags(template, art)
    assert "poster" in tags
    assert any("forest" in tag or "wolf" in tag for tag in tags)
    assert tags.count("printify") <= 1
    assert len(tags) <= 20


def test_preview_listing_copy_behavior(tmp_path: Path, capsys):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.metadata = {"title": "Ocean Drift", "tags": ["coastal"]}
    template = _template_for_variant_tests()
    preview_listing_copy(artworks=[art], templates=[template])
    out = capsys.readouterr().out
    assert "Ocean Drift" in out
    assert "tags:" in out


def test_render_description_generic_fallback(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.title = "messy_title_20240202"
    template = _template_for_variant_tests()
    template.description_pattern = "<p>{artwork_title}</p>"
    description = render_product_description(template, art)
    assert "InkVibe" in description
    assert "expressive everyday style" in description


def test_render_description_uses_rich_metadata_context(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.metadata = {
        "title": "Neon Coyote",
        "subtitle": "Night pulse wildlife scene.",
        "theme": "neon desert",
        "collection": "after dark",
        "occasion": "holiday gifting",
        "color_story": "electric cyan and magenta",
        "artist_note": "Built from hand-sketched ink lines.",
        "audience": "nightlife art fans",
        "style_keywords": ["retro", "street"],
    }
    template = _template_for_variant_tests()
    template.key = "hoodie_gildan"
    template.description_pattern = "<p>{artwork_title}</p>"
    description = render_product_description(template, art)
    assert "Night pulse wildlife scene." in description
    assert "Inspired by neon desert, after dark, electric cyan and magenta." in description
    assert "Artist note: Built from hand-sketched ink lines." in description
    assert "<ul>" in description


def test_sticker_description_adds_terminal_punctuation_when_metadata_lacks_it(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.metadata = {
        "title": "Golden Trail Wolf",
        "description": "A nature-inspired wolf design with a bold expressive look suited for stickers and everyday display",
    }
    template = _template_for_variant_tests()
    template.key = "sticker_kisscut"
    template.product_type_label = "Sticker"
    template.shopify_product_type = "Stickers"
    description = render_product_description(template, art)
    assert "display. An easy gift-ready choice" in description
    assert "display.An easy gift-ready choice" not in description


def test_sticker_description_respects_existing_punctuation_and_avoids_duplicate_sticker_line(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.metadata = {
        "title": "Golden Trail Wolf",
        "description": "A nature-inspired wolf design with a bold expressive look suited for stickers and everyday display.",
    }
    template = _template_for_variant_tests()
    template.key = "sticker_kisscut"
    template.product_type_label = "Sticker"
    template.shopify_product_type = "Stickers"
    description = render_product_description(template, art)
    assert description.count("everyday display.") == 1
    assert "<p>Golden Trail Wolf by InkVibe." in description


def test_sticker_description_wolf_preview_is_natural_sentence_block(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.metadata = {
        "title": "Golden Trail Wolf",
        "description": "A nature-inspired wolf design with a bold, expressive look suited for stickers and everyday display.",
    }
    template = _template_for_variant_tests()
    template.key = "sticker_kisscut"
    template.product_type_label = "Sticker"
    template.shopify_product_type = "Stickers"
    description = render_product_description(template, art)
    assert description.startswith("<p>Golden Trail Wolf by InkVibe.")
    assert "stickers and everyday display. An easy gift-ready choice" in description
    assert description.endswith("</p>")


def test_launch_plan_overrides_still_win_for_listing_copy(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.metadata = {
        "title": "Sunrise Bloom",
        "description": "Metadata description",
        "tags": ["flower", "botanical"],
    }
    base = _template_for_variant_tests()
    resolved = build_resolved_template(
        base,
        {
            "title_override": "Launch Title",
            "description_override": "<p>Launch Description</p>",
            "tags_override": "launch,exclusive",
        },
    )
    assert render_product_title(resolved, art) == "Launch Title"
    assert render_product_description(resolved, art) == "<p>Launch Description</p>"
    tags = _render_listing_tags(resolved, art)
    assert "launch" in tags
    assert "exclusive" in tags


def test_curated_artwork_title_and_family_suffix(tmp_path: Path):
    src = tmp_path / "ai-generated-8321310.png"
    Image.new("RGBA", (1000, 1000), (255, 0, 0, 255)).save(src)
    art = Artwork(
        slug="ai-generated-8321310",
        src_path=src,
        title="Ai Generated 8321310",
        description_html="",
        tags=[],
        image_width=1000,
        image_height=1000,
        metadata={"title": "Golden Trail Wolf", "description": "A lone wolf moving through warm mountain light."},
    )
    template = _template_for_variant_tests()
    template.key = "hoodie_gildan"
    template.product_type_label = "Hoodie"
    template.title_pattern = "{artwork_title}"
    assert render_product_title(template, art) == "Golden Trail Wolf Hoodie"


def test_unknown_artwork_slug_still_human_readable_fallback(tmp_path: Path):
    src = tmp_path / "mystery-shape-20260215.png"
    Image.new("RGBA", (1000, 1000), (255, 0, 0, 255)).save(src)
    art = Artwork(
        slug="mystery-shape-20260215",
        src_path=src,
        title="mystery-shape-20260215",
        description_html="",
        tags=[],
        image_width=1000,
        image_height=1000,
        metadata={},
    )
    template = _template_for_variant_tests()
    template.key = "poster_basic"
    template.title_pattern = "{artwork_title}"
    assert render_product_title(template, art) == "Mystery Shape Poster"


def test_listing_tags_include_family_brand_and_dedupe(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.metadata = {"tags": ["inkvibe", "wolf", "wolf"]}
    template = _template_for_variant_tests()
    template.key = "poster_basic"
    template.tags = ["wall art", "inkvibe"]
    tags = _render_listing_tags(template, art)
    assert "inkvibe" in tags
    assert "poster" in tags
    assert tags.count("inkvibe") == 1
    assert len(tags) <= 14


def test_title_rendering_includes_family_signal_for_all_active_families(tmp_path: Path):
    art = _create_artwork(tmp_path, 1200, 1800)
    art.metadata = {"title": "Aurora Fox", "theme": "northern sky"}
    families = [
        ("hoodie_gildan", "Hoodie"),
        ("sweatshirt_gildan", "Sweatshirt"),
        ("tshirt_gildan", "T-Shirt"),
        ("longsleeve_gildan", "Long Sleeve T-Shirt"),
        ("mug_11oz", "Mug"),
        ("poster_basic", "Poster"),
        ("tote_basic", "Tote Bag"),
    ]
    for key, label in families:
        template = _template_for_variant_tests()
        template.key = key
        template.product_type_label = label
        template.shopify_product_type = label
        template.title_pattern = "{artwork_title}"
        title = render_product_title(template, art)
        assert title_semantically_includes_product_label(title, label), f"{key} missing family signal: {title}"


def test_storefront_tag_builder_keeps_subject_theme_and_avoids_excess(tmp_path: Path):
    art = _create_artwork(tmp_path, 1200, 1800)
    art.slug = "golden-field-fox-portrait"
    art.metadata = {
        "title": "Golden Field Fox",
        "theme": "wildlife portrait",
        "subtitle": "warm dusk palette",
        "style_keywords": ["botanical", "textured"],
        "tags": ["fox", "wildlife portrait", "wall art", "wall art"],
    }
    template = _template_for_variant_tests()
    template.key = "poster_basic"
    template.tags = ["wall art", "wall decor", "design", "artwork"]
    tags = _render_listing_tags(template, art)
    warnings, _ = validate_storefront_tags(tags=tags, template=template, artwork=art)
    assert len(tags) <= 14
    assert any("fox" in tag for tag in tags)
    assert any(tag in {"wildlife portrait", "warm dusk palette", "botanical", "textured"} for tag in tags)
    assert "tag_count_high" not in warnings
    assert "tags_missing_artwork_theme_signal" not in warnings


def test_storefront_qa_strong_metadata_avoids_generic_copy_warnings(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.metadata = {
        "title": "Golden Field Fox",
        "description": "A fox in golden hour light.",
        "theme": "golden field fox",
        "collection": "wild trails",
        "tags": ["fox", "golden field"],
    }
    template = _template_for_variant_tests()
    template.key = "poster_basic"
    title = render_product_title(template, art)
    description = render_product_description(template, art)
    tags = _render_listing_tags(template, art)
    context = build_seo_context(template, art)
    title_warnings, _ = validate_storefront_title(
        title=title,
        template=template,
        artwork=art,
        title_source=context.get("title_source", ""),
        title_quality=context.get("title_quality", ""),
    )
    description_warnings, _ = validate_storefront_description(description_html=description, template=template, artwork=art)
    tag_warnings, _ = validate_storefront_tags(tags=tags, template=template, artwork=art)
    assert "title_bad_fallback" not in title_warnings
    assert "title_missing_product_signal" not in title_warnings
    assert "description_generic_fallback_with_metadata" not in description_warnings
    assert "tags_generic_only" not in tag_warnings
    assert "tag_count_high" not in tag_warnings
    assert "tags_missing_artwork_theme_signal" not in tag_warnings


def test_theme_signal_candidates_normalize_and_dedupe(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.metadata = {
        "theme": " Tropical   Surf!! Lifestyle ",
        "style_keywords": ["surf culture", "surf culture", "retro"],
        "seo_keywords": ["summer vibe", "art"],
        "occasion": "beach party",
    }
    template = _template_for_variant_tests()
    context = build_seo_context(template, art)
    candidates = extract_theme_signal_candidates(artwork=art, context=context, template=template)
    assert "tropical surf lifestyle" in candidates
    assert candidates.count("surf culture") == 1
    assert "art" not in candidates


def test_choose_best_theme_signal_prefers_specific_searchable_phrase():
    selected = choose_best_theme_signal(
        candidates=["design", "summer vibe", "wildlife portrait", "nature"],
        existing_tags=["fox", "poster"],
    )
    assert selected == "wildlife portrait"


def test_ai_product_copy_disabled_falls_back_to_deterministic(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.metadata = {"title": "Cozy Wolf", "description": "Warm mountain wolf art.", "tags": ["wolf", "mountain"]}
    template = _template_for_variant_tests()
    template.key = "mug_new"
    template.product_type_label = "Mug"
    template.shopify_product_type = "Mugs"
    configure_ai_product_copy(enabled=False, model="gpt-4.1-mini", api_key="test")
    title = render_product_title(template, art)
    description = render_product_description(template, art)
    tags = _render_listing_tags(template, art)
    assert "Cozy Wolf" in title
    assert "<p>" in description
    assert isinstance(tags, list) and tags


def test_ai_product_copy_applies_for_hoodie_and_mug(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    template = _template_for_variant_tests()
    template.key = "hoodie_gildan"
    template.product_type_label = "Hoodie"
    template.shopify_product_type = "Hoodies"
    generated = product_copy_generator.GeneratedProductCopy(
        title="Trail Glow Hoodie",
        title_alternatives=["Golden Trail Hoodie"],
        short_description="A cozy expressive layer for everyday wear.",
        long_description="A soft, expressive hoodie look designed for layering and gifting moments.",
        seo_title="Trail Glow Hoodie | InkVibe",
        meta_description="Cozy wearable-art hoodie with everyday personality.",
        tags=["hoodie", "wearable art", "gift idea"],
        chosen_angle="cozy_giftable",
    )
    monkeypatch.setattr(product_copy_generator, "maybe_generate_product_copy", lambda **kwargs: generated)
    configure_ai_product_copy(enabled=True, model="gpt-4.1-mini", api_key="test")
    assert render_product_title(template, art) == "Trail Glow Hoodie"
    assert "expressive hoodie" in render_product_description(template, art).lower()
    assert "wearable art" in _render_listing_tags(template, art)


@pytest.mark.parametrize(
    ("template_key", "product_type", "shopify_type", "family"),
    [
        ("tshirt_gildan", "T-Shirt", "T-Shirts", "tshirt"),
        ("sweatshirt_gildan", "Sweatshirt", "Sweatshirts", "sweatshirt"),
        ("poster_basic", "Poster", "Posters", "poster"),
        ("phone_case_basic", "Phone Case", "Phone Cases", "phone_case"),
    ],
)
def test_ai_product_copy_enabled_for_new_supported_families(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    template_key: str,
    product_type: str,
    shopify_type: str,
    family: str,
):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.metadata = {"title": "Fox Glow", "description": "A vivid fox artwork.", "tags": ["fox", "art"]}
    template = _template_for_variant_tests()
    template.key = template_key
    template.product_type_label = product_type
    template.shopify_product_type = shopify_type

    class _StubClient:
        def __init__(self, *_args, **_kwargs):
            self.responses = self

        def create(self, **_kwargs):
            return types.SimpleNamespace(
                output_text=json.dumps(
                    {
                        "title": f"{product_type} Test",
                        "title_alternatives": [f"{product_type} Alt"],
                        "short_description": "Short copy",
                        "long_description": "Long conversion-focused copy for this listing.",
                        "seo_title": f"{product_type} SEO",
                        "meta_description": "Meta copy for listing previews.",
                        "tags": [family, "gift idea", "art"],
                        "chosen_angle": "everyday_lifestyle",
                    }
                )
            )

    monkeypatch.setattr(product_copy_generator, "OpenAI", _StubClient)
    configure_ai_product_copy(enabled=True, model="gpt-4.1-mini", api_key="test")
    generated = product_copy_generator.maybe_generate_product_copy(
        template=template,
        artwork=art,
        context=build_seo_context(template, art),
        family=family,
        enabled=True,
        model="gpt-4.1-mini",
        api_key="test",
    )
    assert generated is not None
    assert generated.title.endswith("Test")
    expected_tag = family.replace("_", "")
    assert expected_tag in [tag.replace(" ", "").replace("-", "") for tag in generated.tags]


def test_ai_product_copy_unsupported_family_falls_back(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    template = _template_for_variant_tests()
    template.key = "sticker_kisscut"
    template.product_type_label = "Sticker"
    template.shopify_product_type = "Stickers"

    class _ExplodingClient:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("OpenAI client should not be constructed for unsupported family")

    monkeypatch.setattr(product_copy_generator, "OpenAI", _ExplodingClient)
    configure_ai_product_copy(enabled=True, model="gpt-4.1-mini", api_key="test")
    generated = product_copy_generator.maybe_generate_product_copy(
        template=template,
        artwork=art,
        context=build_seo_context(template, art),
        family="default",
        enabled=True,
        model="gpt-4.1-mini",
        api_key="test",
    )
    assert generated is None


def test_ai_product_copy_cache_hit_skips_second_generation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.metadata = {}
    template = _template_for_variant_tests()
    template.key = "poster_basic"
    template.product_type_label = "Poster"
    template.shopify_product_type = "Posters"
    calls = {"count": 0}

    class _StubClient:
        def __init__(self, *_args, **_kwargs):
            self.responses = self

        def create(self, **_kwargs):
            calls["count"] += 1
            return types.SimpleNamespace(
                output_text=json.dumps(
                    {
                        "title": "Poster Title",
                        "title_alternatives": ["Poster Alt"],
                        "short_description": "Short poster copy",
                        "long_description": "Long poster copy describing wall decor mood and centerpiece energy.",
                        "seo_title": "Poster SEO",
                        "meta_description": "Poster metadata description",
                        "tags": ["poster", "wall decor", "gift idea"],
                        "chosen_angle": "wall_decor_mood",
                    }
                )
            )

    monkeypatch.setattr(product_copy_generator, "OpenAI", _StubClient)
    context = build_seo_context(template, art)
    first = product_copy_generator.maybe_generate_product_copy(
        template=template,
        artwork=art,
        context=context,
        family="poster",
        enabled=True,
        model="gpt-4.1-mini",
        api_key="test",
    )
    second = product_copy_generator.maybe_generate_product_copy(
        template=template,
        artwork=art,
        context=context,
        family="poster",
        enabled=True,
        model="gpt-4.1-mini",
        api_key="test",
    )
    assert first is not None and second is not None
    assert first.title == second.title
    assert calls["count"] == 1
    sidecar = json.loads(art.src_path.with_suffix(".json").read_text(encoding="utf-8"))
    assert "ai_product_copy" in sidecar and sidecar["ai_product_copy"]


def test_ai_product_copy_cache_identity_prevents_cross_artwork_reuse(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir(parents=True, exist_ok=True)
    dir_b.mkdir(parents=True, exist_ok=True)
    art_a = _create_artwork(dir_a, 1000, 1000)
    art_b = _create_artwork(dir_b, 1000, 1000)
    art_a.slug = "art-a"
    art_b.slug = "art-b"
    art_a.metadata = {"title": "Forest Wolf", "description": "First artwork"}
    art_b.metadata = {"title": "Forest Wolf", "description": "First artwork"}
    template = _template_for_variant_tests()
    template.key = "poster_basic"
    template.product_type_label = "Poster"
    template.shopify_product_type = "Posters"
    calls = {"count": 0}

    class _StubClient:
        def __init__(self, *_args, **_kwargs):
            self.responses = self

        def create(self, **_kwargs):
            calls["count"] += 1
            return types.SimpleNamespace(
                output_text=json.dumps(
                    {
                        "title": f"Poster Title {calls['count']}",
                        "title_alternatives": ["Poster Alt"],
                        "short_description": "Short poster copy",
                        "long_description": "Long poster copy describing wall decor mood and centerpiece energy.",
                        "seo_title": "Poster SEO",
                        "meta_description": "Poster metadata description",
                        "tags": ["poster", "wall decor", "gift idea"],
                        "chosen_angle": "wall_decor_mood",
                    }
                )
            )

    monkeypatch.setattr(product_copy_generator, "OpenAI", _StubClient)
    context_a = build_seo_context(template, art_a)
    first = product_copy_generator.maybe_generate_product_copy(
        template=template,
        artwork=art_a,
        context=context_a,
        family="poster",
        enabled=True,
        model="gpt-4.1-mini",
        api_key="test",
    )
    sidecar_a = json.loads(art_a.src_path.with_suffix(".json").read_text(encoding="utf-8"))
    art_b_sidecar_path = art_b.src_path.with_suffix(".json")
    art_b_sidecar_path.write_text(json.dumps({"ai_product_copy": sidecar_a["ai_product_copy"]}), encoding="utf-8")
    art_b.metadata["ai_product_copy"] = sidecar_a["ai_product_copy"]
    context_b = build_seo_context(template, art_b)
    second = product_copy_generator.maybe_generate_product_copy(
        template=template,
        artwork=art_b,
        context=context_b,
        family="poster",
        enabled=True,
        model="gpt-4.1-mini",
        api_key="test",
    )
    assert first is not None and second is not None
    assert first.title != second.title
    assert calls["count"] == 2


def test_ai_product_copy_cache_identity_invalidates_on_metadata_change(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.metadata = {"title": "Wolf Dawn", "description": "Original description"}
    template = _template_for_variant_tests()
    template.key = "poster_basic"
    template.product_type_label = "Poster"
    template.shopify_product_type = "Posters"
    calls = {"count": 0}

    class _StubClient:
        def __init__(self, *_args, **_kwargs):
            self.responses = self

        def create(self, **_kwargs):
            calls["count"] += 1
            return types.SimpleNamespace(
                output_text=json.dumps(
                    {
                        "title": f"Poster Title {calls['count']}",
                        "title_alternatives": ["Poster Alt"],
                        "short_description": "Short poster copy",
                        "long_description": "Long poster copy describing wall decor mood and centerpiece energy.",
                        "seo_title": "Poster SEO",
                        "meta_description": "Poster metadata description",
                        "tags": ["poster", "wall decor", "gift idea"],
                        "chosen_angle": "wall_decor_mood",
                    }
                )
            )

    monkeypatch.setattr(product_copy_generator, "OpenAI", _StubClient)
    first = product_copy_generator.maybe_generate_product_copy(
        template=template,
        artwork=art,
        context=build_seo_context(template, art),
        family="poster",
        enabled=True,
        model="gpt-4.1-mini",
        api_key="test",
    )
    art.metadata["description"] = "Updated artwork description"
    second = product_copy_generator.maybe_generate_product_copy(
        template=template,
        artwork=art,
        context=build_seo_context(template, art),
        family="poster",
        enabled=True,
        model="gpt-4.1-mini",
        api_key="test",
    )
    assert first is not None and second is not None
    assert first.title != second.title
    assert calls["count"] == 2


def test_normalize_theme_tag_rejects_generic_filler():
    assert normalize_theme_tag("style") == ""
    assert normalize_theme_tag("Gift Idea") == ""
    assert normalize_theme_tag("  Mountain  Landscape!! ") == "mountain landscape"


def test_theme_signal_injected_without_bloating_tag_count(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.slug = "lion-portrait"
    art.metadata = {
        "title": "Lion Portrait",
        "theme": "wildlife portrait",
        "tags": ["lion", "animal", "wall art", "decor", "gift", "inkvibe"],
    }
    template = _template_for_variant_tests()
    template.tags = ["design", "artwork", "style", "printify"]
    tags = _render_listing_tags(template, art)
    warnings, _ = validate_storefront_tags(tags=tags, template=template, artwork=art)
    assert len(tags) <= 14
    assert any(tag in {"wildlife portrait", "animal art"} for tag in tags)
    assert "tags_missing_artwork_theme_signal" not in warnings


def test_storefront_tag_qa_accepts_contextual_theme_phrases(tmp_path: Path):
    art = _create_artwork(tmp_path, 1000, 1000)
    art.slug = "skeleton-surf-beach"
    art.metadata = {
        "title": "Skeleton Surfer",
        "theme": "tropical surf",
        "style_keywords": ["surf culture", "summer vibe"],
    }
    template = _template_for_variant_tests()
    warnings, _ = validate_storefront_tags(
        tags=["skeleton surfer", "tropical surf", "surf culture", "poster"],
        template=template,
        artwork=art,
    )
    assert "tags_missing_artwork_theme_signal" not in warnings


def test_upload_strategy_summary_helper():
    upload_map = {"front": {"upload_strategy": "direct"}, "back": {"upload_strategy": "r2_url"}}
    assert summarize_upload_strategy(upload_map) == "direct+r2_url"


def test_pricing_markup_fixed_with_x99_rounding():
    template = ProductTemplate(
        key="pricing",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        base_price="21.00",
        markup_type="fixed",
        markup_value="4.50",
        rounding_mode="x_99",
    )
    price = compute_sale_price_minor(template, {"id": 1, "price": 1800})
    assert price == 2599


def test_pricing_markup_percent_with_whole_dollar_rounding():
    template = ProductTemplate(
        key="pricing",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        markup_type="percent",
        markup_value="20",
        rounding_mode="whole_dollar",
    )
    price = compute_sale_price_minor(template, {"id": 1, "price": 2499})
    assert price == 3000


def test_compare_at_price_only_when_higher():
    template = ProductTemplate(
        key="pricing",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        compare_at_price="39.99",
    )
    assert compute_compare_at_price_minor(template, 2999) == 3999
    assert compute_compare_at_price_minor(template, 3999) is None


def test_shopify_variant_pricing_matches_template_pricing_logic():
    template = ProductTemplate(
        key="pricing",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        base_price="12.00",
        markup_type="percent",
        markup_value="35",
        rounding_mode="whole_dollar",
        compare_at_price="19.99",
    )
    _, variants = build_shopify_product_options(template, [{"id": 1, "price": 1099, "options": {"size": "11oz"}}])
    assert variants[0]["price"] == "16.00"
    assert variants[0]["compareAtPrice"] == "19.99"


def test_template_filtering_by_key_and_limit():
    templates = [
        ProductTemplate("tee", 1, 1, "{artwork_title}", "{artwork_title}"),
        ProductTemplate("mug", 2, 2, "{artwork_title}", "{artwork_title}"),
        ProductTemplate("poster", 3, 3, "{artwork_title}", "{artwork_title}"),
    ]
    selected = select_templates(templates, template_keys=["mug", "poster"], limit_templates=1)
    assert [template.key for template in selected] == ["mug"]


def test_template_filtering_defaults_to_active_only():
    templates = [
        ProductTemplate("tee", 1, 1, "{artwork_title}", "{artwork_title}", active=True),
        ProductTemplate("long", 2, 2, "{artwork_title}", "{artwork_title}", active=False),
    ]
    selected = select_templates(templates)
    assert [template.key for template in selected] == ["tee"]


def test_default_product_templates_only_include_proven_active_set():
    templates = load_templates(Path("product_templates.json"))
    active_keys = {template.key for template in templates if template.active}
    assert active_keys == set(PRODUCTION_BASELINE_TEMPLATE_KEYS)
    assert len(active_keys) == 7
    for key in {"canvas_basic", "blanket_basic", "tote_basic"}:
        assert key not in active_keys


def test_production_baseline_template_keys_are_frozen_to_current_validated_set():
    assert PRODUCTION_BASELINE_TEMPLATE_KEYS == (
        "tshirt_gildan",
        "sweatshirt_gildan",
        "hoodie_gildan",
        "mug_new",
        "poster_basic",
        "phone_case_basic",
        "sticker_kisscut",
    )


def test_preflight_classifies_invalid_zero_selected_and_guardrail_failures():
    class DummyPrintify:
        def list_blueprints(self):
            return [{"id": 1}]

        def list_print_providers(self, blueprint_id):
            return [{"id": 2}]

        def list_variants(self, blueprint_id, provider_id):
            return [
                {"id": 11, "is_available": True, "options": {"color": "Black", "size": "M"}, "cost": 2000, "price": 2000},
            ]

    invalid = ProductTemplate("invalid", 404, 2, "{artwork_title}", "{artwork_title}", active=True)
    zero_selected = ProductTemplate(
        "zero_selected", 1, 2, "{artwork_title}", "{artwork_title}", active=True, enabled_colors=["White"]
    )
    guardrail_zero = ProductTemplate(
        "guardrail_zero",
        1,
        2,
        "{artwork_title}",
        "{artwork_title}",
        active=True,
        base_price="10.00",
        markup_type="fixed",
        markup_value="0",
        min_margin_after_shipping="50.00",
        reprice_variants_to_margin_floor=False,
        disable_variants_below_margin_floor=True,
    )
    passed, issues, report_rows = preflight_active_templates(
        printify=DummyPrintify(),
        templates=[invalid, zero_selected, guardrail_zero],
        explicit_template_keys=[],
    )
    assert passed == []
    issue_map = {issue.template_key: issue.classification for issue in issues}
    assert issue_map["invalid"] == "invalid_template_config"
    assert issue_map["zero_selected"] == "zero_variants_selected"
    assert issue_map["guardrail_zero"] == "zero_enabled_after_guardrails"
    assert len(report_rows) == 3


def test_preflight_tote_nonviable_report_includes_economic_diagnostics():
    class DummyPrintify:
        def list_blueprints(self):
            return [{"id": 609}]

        def list_print_providers(self, blueprint_id):
            return [{"id": 74, "title": "Tote Provider"}]

        def list_variants(self, blueprint_id, provider_id):
            return [
                {"id": 21, "is_available": True, "options": {"color": "Natural", "size": "One size"}, "cost": 2600, "shipping": 600},
            ]

    tote = ProductTemplate(
        "tote_basic",
        609,
        74,
        "{artwork_title}",
        "{artwork_title}",
        active=True,
        enabled_colors=["Natural"],
        enabled_sizes=["One size"],
        base_price="24.99",
        markup_type="fixed",
        markup_value="5.00",
        min_margin_after_shipping="3.00",
        target_margin_after_shipping="5.00",
        max_allowed_price="29.99",
        reprice_variants_to_margin_floor=True,
        disable_variants_below_margin_floor=True,
    )
    passed, issues, report_rows = preflight_active_templates(
        printify=DummyPrintify(),
        templates=[tote],
        explicit_template_keys=[],
    )
    assert passed == []
    assert issues and issues[0].classification == "zero_enabled_after_guardrails"
    row = report_rows[0]
    assert row.classification == "zero_enabled_after_guardrails"
    assert row.tote_original_sale_price_minor == 2999
    assert row.tote_repriced_sale_price_minor == 2999
    assert row.tote_printify_cost_minor == 2600
    assert row.tote_shipping_basis_used == "cost"
    assert row.tote_target_margin_after_shipping_minor == 500
    assert row.tote_min_margin_after_shipping_minor == 300
    assert row.tote_margin_before_reprice_minor == -201
    assert row.tote_margin_after_reprice_minor == -201
    assert row.tote_max_allowed_price_minor == 2999
    assert row.tote_failure_reason == "required_price_exceeds_max_allowed_price"
    assert "reason=required_price_exceeds_max_allowed_price" in row.message


def test_preflight_apparel_nonviable_report_includes_economic_diagnostics_and_reason_counts():
    class DummyPrintify:
        def list_blueprints(self):
            return [{"id": 6}]

        def list_print_providers(self, blueprint_id):
            return [{"id": 99, "title": "Printify Choice"}]

        def list_variants(self, blueprint_id, provider_id):
            return [
                {"id": 22, "is_available": True, "options": {"color": "Black", "size": "M"}, "cost": 2600, "shipping": 600},
            ]

    tee = ProductTemplate(
        "tshirt_gildan",
        6,
        99,
        "{artwork_title}",
        "{artwork_title}",
        active=True,
        enabled_colors=["Black"],
        enabled_sizes=["M"],
        base_price="24.99",
        markup_type="fixed",
        markup_value="0.00",
        min_margin_after_shipping="3.00",
        target_margin_after_shipping="5.00",
        max_allowed_price="29.99",
        reprice_variants_to_margin_floor=True,
        disable_variants_below_margin_floor=True,
    )
    passed, issues, report_rows = preflight_active_templates(printify=DummyPrintify(), templates=[tee], explicit_template_keys=[])
    assert passed == []
    assert issues and issues[0].classification == "zero_enabled_after_guardrails"
    row = report_rows[0]
    assert row.apparel_original_sale_price_minor == 2499
    assert row.apparel_repriced_sale_price_minor == 2999
    assert row.apparel_printify_cost_minor == 2600
    assert row.apparel_shipping_basis_used == "cost"
    assert row.apparel_shipping_minor == 600
    assert row.apparel_target_margin_after_shipping_minor == 500
    assert row.apparel_min_margin_after_shipping_minor == 300
    assert row.apparel_margin_after_reprice_minor == -201
    assert '"required_price_exceeds_max_allowed_price": 1' in row.apparel_failure_reason_counts
    assert "Apparel economics:" in row.message


def test_preflight_applies_provider_strategy_before_family_mismatch_classification():
    class DummyPrintify:
        def list_blueprints(self):
            return [{"id": 9}]

        def list_print_providers(self, blueprint_id):
            return [{"id": 2, "title": "Fallback Provider"}]

        def list_variants(self, blueprint_id, provider_id):
            return [{"id": 100, "is_available": True, "options": {"color": "Black", "size": "M"}, "cost": 1200, "price": 1200}]

    template = ProductTemplate(
        "phone_case_basic",
        9,
        1,
        "{artwork_title}",
        "{artwork_title}",
        active=True,
        pinned_provider_id=2,
        fallback_provider_allowed=True,
        provider_selection_strategy="prefer_printify_choice_then_ranked",
    )
    passed, issues, report_rows = preflight_active_templates(
        printify=DummyPrintify(),
        templates=[template],
        explicit_template_keys=[],
    )
    assert passed == []
    assert issues and issues[0].classification == "wrong_catalog_family"
    assert report_rows[0].provider_id == 2
    assert report_rows[0].classification == "wrong_catalog_family"


def test_choose_variants_for_apparel_prefers_core_colors_and_common_sizes_when_capped():
    template = ProductTemplate(
        "hoodie_gildan",
        77,
        99,
        "{artwork_title}",
        "{artwork_title}",
        enabled_colors=["Black", "White", "Navy", "Sport Grey", "Sand"],
        enabled_sizes=["S", "M", "L", "XL", "2XL", "3XL"],
        max_enabled_variants=4,
    )
    variants = [
        {"id": 1, "is_available": True, "options": {"color": "Sand", "size": "3XL"}, "cost": 3200},
        {"id": 2, "is_available": True, "options": {"color": "Black", "size": "XL"}, "cost": 2800},
        {"id": 3, "is_available": True, "options": {"color": "Black", "size": "M"}, "cost": 2600},
        {"id": 4, "is_available": True, "options": {"color": "White", "size": "L"}, "cost": 2700},
        {"id": 5, "is_available": True, "options": {"color": "Navy", "size": "S"}, "cost": 2750},
        {"id": 6, "is_available": True, "options": {"color": "Sport Grey", "size": "2XL"}, "cost": 2900},
    ]
    chosen = choose_variants_from_catalog(variants, template)
    assert [v["id"] for v in chosen] == [3, 2, 4, 5]


def test_run_audit_mode_does_not_raise_on_explicit_preflight_failures_and_exports_report(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    class DummyPrintifyClient:
        def __init__(self, *args, **kwargs):
            self.dry_run = True

        def list_blueprints(self):
            return [{"id": 1}]

        def list_print_providers(self, blueprint_id):
            return [{"id": 2}]

        def list_variants(self, blueprint_id, provider_id):
            return [{"id": 11, "is_available": True, "options": {"color": "Black", "size": "M"}, "cost": 1000, "price": 1000}]

    monkeypatch.setattr(pipeline, "PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "load_json", lambda path, default: ensure_state_shape({}))
    monkeypatch.setattr(pipeline, "load_templates", lambda p: [
        ProductTemplate("bad", 404, 1, "{artwork_title}", "{artwork_title}", active=True),
    ])
    monkeypatch.setattr(pipeline, "select_templates", lambda templates, **kwargs: templates)
    monkeypatch.setattr(pipeline, "discover_artworks", lambda d: [])
    monkeypatch.setattr(pipeline, "PrintifyClient", DummyPrintifyClient)
    monkeypatch.setattr(pipeline, "resolve_shop_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "load_r2_config_from_env", lambda: None)
    monkeypatch.setattr(pipeline, "audit_printify_integration", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "run_catalog_cli", lambda **kwargs: False)

    preflight_report = tmp_path / "preflight.csv"
    run(
        tmp_path / "templates.json",
        image_dir=tmp_path,
        export_dir=tmp_path / "exp",
        state_path=tmp_path / "state.json",
        template_keys=["bad"],
        allow_preflight_failures=True,
        export_preflight_report_path=str(preflight_report),
    )
    text = preflight_report.read_text(encoding="utf-8")
    assert "template_key,requested_explicitly,preflight_status,classification" in text
    assert "bad,True,failed,invalid_template_config" in text


def test_seo_metadata_context_and_rendering(tmp_path: Path):
    art = _create_artwork(tmp_path, 1200, 1200)
    template = ProductTemplate(
        key="seo",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title} {product_type_label}",
        description_pattern="<p>{audience}</p><p>{seo_keywords}</p><p>{style_keywords}</p>",
        seo_keywords=["gift for cat lovers", "cute cat shirt"],
        audience="cat moms",
        product_type_label="Graphic Tee",
        style_keywords=["retro", "minimal"],
    )
    context = build_seo_context(template, art)
    assert context["audience"] == "cat moms"
    description = render_product_description(template, art)
    assert "gift for cat lovers" in description
    assert "retro, minimal" in description


def test_state_key_tracks_artwork_template_combinations(tmp_path: Path):
    artwork = _create_artwork(tmp_path, 1200, 1200)
    template_a = ProductTemplate(
        key="tee",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black"],
        enabled_sizes=["M"],
        placements=[PlacementRequirement("front", 1000, 1000)],
    )
    template_b = ProductTemplate(
        key="mug",
        printify_blueprint_id=2,
        printify_print_provider_id=2,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black"],
        enabled_sizes=["M"],
        placements=[PlacementRequirement("front", 1000, 1000)],
    )
    state = ensure_state_shape({})
    process_artwork(
        printify=DummyPrintify(),
        shopify=None,
        shop_id=None,
        artwork=artwork,
        templates=[template_a, template_b],
        state=state,
        force=True,
        export_dir=tmp_path / "exports",
        state_path=tmp_path / "state.json",
        artwork_options=ArtworkProcessingOptions(),
        upload_strategy="auto",
        r2_config=None,
    )
    keys = {row["state_key"] for row in state["processed"]["art"]["products"]}
    assert keys == {"art:tee", "art:mug"}


def test_blueprint_search_filters_by_keywords():
    rows = [
        {"id": 1, "title": "Unisex Heavy Cotton Tee", "brand": "Gildan", "model": "5000"},
        {"id": 2, "title": "Ceramic Mug", "brand": "Generic", "model": "11oz"},
    ]
    filtered = search_blueprints(rows, "cotton gildan")
    assert [row["id"] for row in filtered] == [1]


def test_provider_filter_by_title():
    providers = [{"id": 1, "title": "Print Provider A"}, {"id": 2, "title": "Another Co"}]
    assert [p["id"] for p in filter_providers(providers, "provider")] == [1]


def test_provider_scoring_prefers_matching_template_constraints():
    template = ProductTemplate(
        key="tee",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black", "White"],
        enabled_sizes=["M", "L"],
        placements=[PlacementRequirement("front", 4500, 5400)],
    )
    provider = {"id": 99, "title": "Best Provider"}
    variants = [
        {"id": 1, "is_available": True, "options": {"color": "Black", "size": "M"}},
        {"id": 2, "is_available": True, "options": {"color": "White", "size": "L"}},
    ]
    score = score_provider_for_template(provider, variants, template)
    assert score["matching_color_count"] == 2
    assert score["matching_size_count"] == 2
    assert score["matching_variant_count"] == 2


def test_generate_template_snippet_contains_detected_values():
    variants = [
        {"id": 1, "options": {"color": "Black", "size": "M"}},
        {"id": 2, "options": {"color": "White", "size": "L"}},
    ]
    snippet = generate_template_snippet(key="new_tee", blueprint_id=6, provider_id=7, variants=variants)
    assert snippet["key"] == "new_tee"
    assert snippet["printify_blueprint_id"] == 6
    assert snippet["printify_print_provider_id"] == 7
    assert snippet["enabled_colors"] == ["Black", "White"]
    assert snippet["enabled_sizes"] == ["L", "M"]
    assert snippet["base_price"] == "24.99"
    assert snippet["markup_type"] == "fixed"
    assert snippet["rounding_mode"] == "x_99"
    assert "audience" in snippet


def test_resolve_product_action_modes():
    assert resolve_product_action(existing_product_id="", create_only=False, update_only=False, rebuild_product=False) == "create"
    assert resolve_product_action(existing_product_id="p1", create_only=False, update_only=False, rebuild_product=False) == "update"
    assert resolve_product_action(existing_product_id="p1", create_only=True, update_only=False, rebuild_product=False) == "skip"
    assert resolve_product_action(existing_product_id="", create_only=False, update_only=True, rebuild_product=False) == "skip"
    assert resolve_product_action(existing_product_id="p1", create_only=False, update_only=False, rebuild_product=True) == "rebuild"

def test_payload_consistency_validation_detects_missing_variant_ids():
    payload = {
        "variants": [{"id": 1, "is_enabled": True}, {"id": 2, "is_enabled": True}],
        "print_areas": [{"variant_ids": [1], "placeholders": []}],
    }
    with pytest.raises(ValueError, match=r"missing=\[2\]"):
        validate_printify_payload_consistency(payload)


def test_update_compatibility_logic_detects_provider_and_variant_mismatch():
    existing = {
        "blueprint_id": 6,
        "print_provider_id": 99,
        "variants": [{"id": 1, "is_enabled": True}],
        "print_areas": [{"placeholders": [{"position": "front"}]}],
    }
    payload = {
        "blueprint_id": 6,
        "print_provider_id": 12,
        "variants": [{"id": 1, "is_enabled": True}, {"id": 2, "is_enabled": True}],
        "print_areas": [{"variant_ids": [1, 2], "placeholders": [{"position": "front"}, {"position": "back"}]}],
    }
    decision = assess_update_compatibility(existing, payload)
    assert decision["compatible"] is False
    assert any("provider mismatch" in issue for issue in decision["issues"])
    assert any("missing variant ids=[2]" in issue for issue in decision["issues"])


def test_process_artwork_updates_existing_product_by_default(tmp_path: Path):
    class UpdateCapablePrintify(DummyPrintify):
        dry_run = False

        def get_product(self, shop_id, product_id):
            return {
                "id": product_id,
                "blueprint_id": 1,
                "print_provider_id": 1,
                "variants": [{"id": 1, "is_enabled": True}],
                "print_areas": [{"placeholders": [{"position": "front"}]}],
            }

        def update_product(self, shop_id, product_id, payload):
            return {"id": product_id, "status": "updated"}

        def create_product(self, shop_id, payload):
            raise AssertionError("should not create")

        def publish_product(self, shop_id, product_id, payload):
            return {"status": "published"}

    artwork = _create_artwork(tmp_path, 1200, 1200)
    template = ProductTemplate(
        key="tee",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black"],
        enabled_sizes=["M"],
        placements=[PlacementRequirement("front", 1000, 1000)],
    )
    state = ensure_state_shape({
        "processed": {
            "art": {
                "products": [
                    {
                        "template": "tee",
                        "state_key": "art:tee",
                        "result": {"printify": {"printify_product_id": "existing-1"}},
                    }
                ]
            }
        }
    })
    process_artwork(
        printify=UpdateCapablePrintify(),
        shopify=None,
        shop_id=111,
        artwork=artwork,
        templates=[template],
        state=state,
        force=False,
        export_dir=tmp_path / "exports",
        state_path=tmp_path / "state.json",
        artwork_options=ArtworkProcessingOptions(),
        upload_strategy="auto",
        r2_config=None,
    )
    latest = state["processed"]["art"]["products"][-1]["result"]["printify"]
    assert latest["action"] == "update"
    assert latest["printify_product_id"] == "existing-1"


def test_phone_case_runtime_uses_catalog_titles_and_creates_product_after_preflight_like_selection(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    class PhoneRuntimePrintify:
        dry_run = False

        def list_blueprints(self):
            return [{"id": 421, "title": "Tough Phone Cases"}]

        def list_print_providers(self, blueprint_id):
            return [{"id": 23, "title": "Phone Provider"}]

        def list_variants(self, blueprint_id, provider_id):
            return [
                {"id": 101, "is_available": True, "options": {"surface": "Glossy"}, "price": 1899, "cost": 900},
                {"id": 102, "is_available": True, "options": {"surface": "Glossy"}, "price": 1899, "cost": 900},
                {"id": 103, "is_available": True, "options": {"surface": "Glossy"}, "price": 1899, "cost": 900},
            ]

        def create_product(self, shop_id, payload):
            return {"id": "phone-created-1"}

    artwork = _create_artwork(tmp_path, 1200, 1200)
    template = ProductTemplate(
        key="phone_case_basic",
        printify_blueprint_id=421,
        printify_print_provider_id=23,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[PlacementRequirement("front", 1000, 1000)],
        publish_after_create=False,
    )
    state = ensure_state_shape({})

    monkeypatch.setattr(pipeline, "select_provider_for_template", lambda printify, template: template)
    monkeypatch.setattr(
        pipeline,
        "prepare_artwork_export",
        lambda artwork, template, placement, export_dir, options: PreparedArtwork(
            artwork=artwork,
            template=template,
            placement=placement,
            export_path=tmp_path / "exports" / f"{artwork.slug}.png",
            width_px=placement.width_px,
            height_px=placement.height_px,
            source_size=(artwork.image_width, artwork.image_height),
            exported_canvas_size=(placement.width_px, placement.height_px),
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "upload_assets_to_printify",
        lambda *args, **kwargs: {"front": {"id": "upload-1", "upload_strategy": "direct"}},
    )

    process_artwork(
        printify=PhoneRuntimePrintify(),
        shopify=None,
        shop_id=999,
        artwork=artwork,
        templates=[template],
        state=state,
        force=True,
        export_dir=tmp_path / "exports",
        state_path=tmp_path / "state.json",
        artwork_options=ArtworkProcessingOptions(),
        upload_strategy="auto",
        r2_config=None,
    )
    printify_result = state["processed"]["art"]["products"][-1]["result"]["printify"]
    assert printify_result["action"] == "create"
    assert printify_result["printify_product_id"] == "phone-created-1"
    assert "runtime_skip_reason_code" not in printify_result


def test_phone_case_runtime_skip_surfaces_structured_reason(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    class PhoneRuntimePrintify:
        dry_run = True

        def list_blueprints(self):
            return [{"id": 421, "title": "Tough Phone Cases"}]

        def list_print_providers(self, blueprint_id):
            return [{"id": 23, "title": "Phone Provider"}]

        def list_variants(self, blueprint_id, provider_id):
            return [{"id": 201, "is_available": True, "options": {"surface": "Glossy"}, "price": 1899, "cost": 900}]

    artwork = _create_artwork(tmp_path, 1200, 1200)
    template = ProductTemplate(
        key="phone_case_basic",
        printify_blueprint_id=421,
        printify_print_provider_id=23,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_variant_option_filters={"model": ["iPhone 99"]},
        placements=[PlacementRequirement("front", 1000, 1000)],
    )
    state = ensure_state_shape({})
    monkeypatch.setattr(pipeline, "select_provider_for_template", lambda printify, template: template)

    process_artwork(
        printify=PhoneRuntimePrintify(),
        shopify=None,
        shop_id=999,
        artwork=artwork,
        templates=[template],
        state=state,
        force=True,
        export_dir=tmp_path / "exports",
        state_path=tmp_path / "state.json",
        artwork_options=ArtworkProcessingOptions(),
        upload_strategy="auto",
        r2_config=None,
    )
    result = state["processed"]["art"]["products"][-1]["result"]
    assert result["status"] == "no_matching_variants"
    assert result["runtime_skip_reason_code"] == "no_matching_variants_runtime"
    diag = result["runtime_skip_diagnostics"]
    assert diag["template_key"] == "phone_case_basic"
    assert diag["provider_id"] == 23
    assert "model" in diag["resolved_option_dimensions"]
    assert diag["final_reason_code"] == "no_matching_variants_runtime"


def test_no_retry_on_404_catalog_error():
    class DummyResponse:
        status_code = 404
        headers = {}
        content = b"{}"

        def json(self):
            return {"error": "Not found"}

        @property
        def text(self):
            return '{"error":"Not found"}'

    class DummySession:
        def __init__(self):
            self.calls = 0
            self.headers = {}

        def request(self, **kwargs):
            self.calls += 1
            return DummyResponse()

    client = BaseApiClient.__new__(BaseApiClient)
    client.base_url = "https://example.test"
    client.dry_run = False
    client.session = DummySession()

    with pytest.raises(NonRetryableRequestError, match="HTTP 404"):
        client.get("/catalog/blueprints/1/print_providers/99/variants.json")
    assert client.session.calls == 1


def test_catalog_429_retry_after_sleep_is_capped(monkeypatch):
    class DummyResponse:
        def __init__(self):
            self.status_code = 429
            self.headers = {"Retry-After": "745"}
            self.content = b"{}"

        def json(self):
            return {}

        @property
        def text(self):
            return "{}"

    class DummySession:
        def __init__(self):
            self.calls = 0
            self.headers = {}

        def request(self, **kwargs):
            self.calls += 1
            return DummyResponse()

    sleep_calls = []
    monkeypatch.setattr("printify_shopify_sync_pipeline.time.sleep", lambda seconds: sleep_calls.append(seconds))

    client = BaseApiClient.__new__(BaseApiClient)
    client.base_url = "https://example.test"
    client.dry_run = False
    client.session = DummySession()

    with pytest.raises(RetryLimitExceededError, match="catalog_rate_limited"):
        client.get("/catalog/blueprints/1658/print_providers/90/variants.json")
    assert sleep_calls
    assert max(sleep_calls) <= 15


def test_mutation_429_retry_after_sleep_is_capped_for_interactive(monkeypatch):
    class DummyResponse:
        def __init__(self):
            self.status_code = 429
            self.headers = {"Retry-After": "958"}
            self.content = b"{}"

        def json(self):
            return {}

        @property
        def text(self):
            return "{}"

    class DummySession:
        def __init__(self):
            self.calls = 0
            self.headers = {}

        def request(self, **kwargs):
            self.calls += 1
            return DummyResponse()

    sleep_calls = []
    monkeypatch.setattr("printify_shopify_sync_pipeline.time.sleep", lambda seconds: sleep_calls.append(seconds))

    client = BaseApiClient.__new__(BaseApiClient)
    client.base_url = "https://example.test"
    client.dry_run = False
    client.session = DummySession()
    client.interactive_retry_policy = True
    client.interactive_retry_cap_seconds = 7
    client.max_retry_sleep_seconds = 60

    with pytest.raises(RetryLimitExceededError, match="publish_rate_limited"):
        client.post("/shops/9/products/abc/publish.json", {"title": True})
    assert sleep_calls
    assert max(sleep_calls) <= 7


def test_retry_logging_includes_endpoint_policy_and_requested_vs_capped(monkeypatch, caplog):
    class DummyResponse:
        def __init__(self):
            self.status_code = 429
            self.headers = {"Retry-After": "120"}
            self.content = b"{}"

        def json(self):
            return {}

        @property
        def text(self):
            return "{}"

    class DummySession:
        def __init__(self):
            self.calls = 0
            self.headers = {}

        def request(self, **kwargs):
            self.calls += 1
            return DummyResponse()

    monkeypatch.setattr("printify_shopify_sync_pipeline.time.sleep", lambda seconds: None)
    client = BaseApiClient.__new__(BaseApiClient)
    client.base_url = "https://example.test"
    client.dry_run = False
    client.session = DummySession()
    client.interactive_retry_policy = True
    client.interactive_retry_cap_seconds = 9
    client.max_retry_sleep_seconds = 30

    with caplog.at_level(logging.WARNING):
        with pytest.raises(RetryLimitExceededError):
            client.post("/shops/5/products/abc/publish.json", {"images": True})
    assert "policy_bucket=mutation" in caplog.text
    assert "endpoint=/shops/5/products/abc/publish.json" in caplog.text
    assert "requested=120.00s capped=9.00s" in caplog.text


def test_catalog_cli_invalid_provider_for_blueprint_has_helpful_error(tmp_path: Path):
    class DummyCatalogPrintify:
        def list_print_providers(self, blueprint_id):
            return [{"id": 1, "title": "SPOKE Custom Products"}]

        def list_variants(self, blueprint_id, provider_id):
            return []

    with pytest.raises(CatalogCliUsageError, match="Provider 99 is not available for blueprint 68") as exc:
        run_catalog_cli(
            printify=DummyCatalogPrintify(),
            config_path=tmp_path / "templates.json",
            list_blueprints=False,
            search_query="",
            limit_blueprints=25,
            list_providers=False,
            blueprint_id=68,
            provider_id=99,
            limit_providers=25,
            inspect_variants=True,
            recommend_provider=False,
            template_file="",
            generate_template_snippet_flag=False,
            auto_provider=False,
            snippet_key="",
            template_output_file="",
        )
    assert "Valid providers: 1 SPOKE Custom Products" in str(exc.value)
    assert "--list-providers --blueprint-id 68" in str(exc.value)


def test_catalog_cli_auto_provider_selects_best(tmp_path: Path, capsys):
    class DummyCatalogPrintify:
        def list_print_providers(self, blueprint_id):
            return [
                {"id": 1, "title": "Provider A"},
                {"id": 2, "title": "Provider B"},
            ]

        def list_variants(self, blueprint_id, provider_id):
            if provider_id == 1:
                return [
                    {"id": 11, "is_available": True, "options": {"color": "Black", "size": "M"}, "placeholders": [{"position": "front"}]},
                    {"id": 12, "is_available": True, "options": {"color": "Black", "size": "L"}, "placeholders": [{"position": "front"}]},
                ]
            return [
                {"id": 21, "is_available": True, "options": {"color": "White", "size": "S"}, "placeholders": [{"position": "back"}]},
            ]

    done = run_catalog_cli(
        printify=DummyCatalogPrintify(),
        config_path=tmp_path / "templates.json",
        list_blueprints=False,
        search_query="",
        limit_blueprints=25,
        list_providers=False,
        blueprint_id=68,
        provider_id=0,
        limit_providers=25,
        inspect_variants=True,
        recommend_provider=False,
        template_file="",
        generate_template_snippet_flag=False,
        auto_provider=True,
        snippet_key="",
        template_output_file="",
    )

    captured = capsys.readouterr().out
    assert done is True
    assert "Auto-selected provider_id=1" in captured
    assert "provider_id=1" in captured


def test_state_shape_backward_compatible_with_new_fields():
    state = ensure_state_shape({"processed": {"art": {"products": [{"state_key": "art:tee"}]}}})
    assert "processed" in state
    assert "publish_queue" in state
    assert list_state_keys(state) == ["art:tee"]


def test_inspect_state_helpers():
    state = ensure_state_shape({
        "processed": {
            "a": {"products": [{"state_key": "a:t1", "result": {"ok": True}}]},
            "b": {"products": [{"state_key": "b:t2", "result": {"ok": True}}]},
        }
    })
    keys = list_state_keys(state)
    assert keys == ["a:t1", "b:t2"]
    assert inspect_state_key(state, "a:t1")["state_key"] == "a:t1"
    assert inspect_state_key(state, "missing") is None


def test_publish_verify_decision_logic(tmp_path: Path):
    class PublishVerifyPrintify(DummyPrintify):
        dry_run = False

        def create_product(self, shop_id, payload):
            return {"id": "p-new"}

        def publish_product(self, shop_id, product_id, payload):
            return {"status": "published"}

        def get_product(self, shop_id, product_id):
            return {
                "id": product_id,
                "title": "Art",
                "variants": [{"id": 1, "is_enabled": True}],
                "print_areas": [{"variant_ids": [1], "placeholders": []}],
                "images": [{"src": "https://example.test/mock.png"}],
                "visible": True,
            }

    artwork = _create_artwork(tmp_path, 1200, 1200)
    template = ProductTemplate(
        key="tee",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black"],
        enabled_sizes=["M"],
        placements=[PlacementRequirement("front", 1000, 1000)],
    )
    state = ensure_state_shape({})
    summary = RunSummary()
    process_artwork(
        printify=PublishVerifyPrintify(),
        shopify=None,
        shop_id=111,
        artwork=artwork,
        templates=[template],
        state=state,
        force=True,
        export_dir=tmp_path / "exports",
        state_path=tmp_path / "state.json",
        artwork_options=ArtworkProcessingOptions(),
        upload_strategy="auto",
        r2_config=None,
        publish_mode="publish",
        verify_publish=True,
        summary=summary,
    )
    row = state["processed"]["art"]["products"][-1]
    assert row["publish_attempted"] is True
    assert row["publish_verified"] is True
    assert summary.publish_attempts == 1
    assert summary.publish_verified == 1


def test_publish_skipped_but_verify_counts_warning(tmp_path: Path):
    class VerifyWarnPrintify(DummyPrintify):
        dry_run = False

        def create_product(self, shop_id, payload):
            return {"id": "p-new"}

        def get_product(self, shop_id, product_id):
            return {"id": product_id, "title": "Different", "variants": [], "print_areas": []}

    artwork = _create_artwork(tmp_path, 1200, 1200)
    template = ProductTemplate(
        key="tee",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black"],
        enabled_sizes=["M"],
        placements=[PlacementRequirement("front", 1000, 1000)],
    )
    state = ensure_state_shape({})
    summary = RunSummary()
    process_artwork(
        printify=VerifyWarnPrintify(),
        shopify=None,
        shop_id=111,
        artwork=artwork,
        templates=[template],
        state=state,
        force=True,
        export_dir=tmp_path / "exports",
        state_path=tmp_path / "state.json",
        artwork_options=ArtworkProcessingOptions(),
        upload_strategy="auto",
        r2_config=None,
        publish_mode="skip",
        verify_publish=True,
        summary=summary,
    )
    row = state["processed"]["art"]["products"][-1]
    assert row["publish_attempted"] is False
    assert row["publish_verified"] is False
    assert summary.verification_warnings == 1


def test_publish_rate_limited_records_structured_failure_row(tmp_path: Path):
    class PublishRateLimitedPrintify(DummyPrintify):
        dry_run = False

        def create_product(self, shop_id, payload):
            return {"id": "p-new"}

        def publish_product(self, shop_id, product_id, payload):
            raise RetryLimitExceededError(
                method="POST",
                path=f"/shops/{shop_id}/products/{product_id}/publish.json",
                policy_bucket="mutation",
                status_code=429,
                attempts=5,
                reason_code="publish_rate_limited",
            )

    artwork = _create_artwork(tmp_path, 1200, 1200)
    template = ProductTemplate(
        key="tee",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black"],
        enabled_sizes=["M"],
        placements=[PlacementRequirement("front", 1000, 1000)],
    )
    state = ensure_state_shape({})
    failures = []
    run_rows = []
    process_artwork(
        printify=PublishRateLimitedPrintify(),
        shopify=None,
        shop_id=111,
        artwork=artwork,
        templates=[template],
        state=state,
        force=True,
        export_dir=tmp_path / "exports",
        state_path=tmp_path / "state.json",
        artwork_options=ArtworkProcessingOptions(),
        upload_strategy="auto",
        r2_config=None,
        publish_mode="publish",
        failure_rows=failures,
        run_rows=run_rows,
    )
    assert not failures
    assert run_rows and run_rows[0].status == "success"
    assert run_rows[0].publish_outcome == "create_success_publish_rate_limited"
    assert state["publish_queue"]
    queue_row = state["publish_queue"][0]
    assert queue_row["publish_status"] == "pending_retry"
    assert queue_row["product_id"] == "p-new"


def test_process_artwork_defer_publish_enqueues_pending(tmp_path: Path):
    class DeferPrintify(DummyPrintify):
        dry_run = False

        def create_product(self, shop_id, payload):
            return {"id": "p-new"}

        def publish_product(self, shop_id, product_id, payload):
            raise AssertionError("publish should be deferred")

    artwork = _create_artwork(tmp_path, 1200, 1200)
    template = ProductTemplate(
        key="tee",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black"],
        enabled_sizes=["M"],
        placements=[PlacementRequirement("front", 1000, 1000)],
    )
    state = ensure_state_shape({})
    run_rows = []
    process_artwork(
        printify=DeferPrintify(),
        shopify=None,
        shop_id=111,
        artwork=artwork,
        templates=[template],
        state=state,
        force=True,
        export_dir=tmp_path / "exports",
        state_path=tmp_path / "state.json",
        artwork_options=ArtworkProcessingOptions(),
        upload_strategy="auto",
        r2_config=None,
        defer_publish=True,
        run_rows=run_rows,
    )
    assert run_rows[0].publish_outcome == "create_success_publish_deferred"
    assert state["publish_queue"][0]["publish_status"] == "pending"


def test_process_publish_queue_resume_only_without_recreate():
    class ResumePrintify(DummyPrintify):
        def __init__(self):
            self.calls = []

        def publish_product(self, shop_id, product_id, payload):
            self.calls.append((shop_id, product_id))
            return {"status": "published"}

    template = ProductTemplate(
        key="tee",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black"],
        enabled_sizes=["M"],
        placements=[PlacementRequirement("front", 1000, 1000)],
    )
    state = ensure_state_shape(
        {
            "publish_queue": [
                {
                    "artwork_key": "art",
                    "template_key": "tee",
                    "shop_id": 111,
                    "product_id": "p-existing",
                    "created_at": "2025-01-01T00:00:00+00:00",
                    "publish_status": "pending",
                    "publish_attempts": 0,
                    "last_error": "",
                    "reason_code": "",
                    "next_eligible_publish_at": "",
                }
            ]
        }
    )
    client = ResumePrintify()
    stats = process_publish_queue(
        printify=client,
        state=state,
        templates_by_key={"tee": template},
        publish_batch_size=1,
        pause_between_publish_batches_seconds=0,
    )
    assert client.calls == [(111, "p-existing")]
    assert stats["completed"] == 1
    assert stats["processed"] == 1
    assert state["publish_queue"][0]["publish_status"] == "completed"
    assert state["publish_queue"][0]["publish_attempts"] == 1


def test_summarize_publish_queue_counts_completed_history_as_not_pending():
    state = ensure_state_shape(
        {
            "publish_queue": [
                {"publish_status": "completed"},
                {"publish_status": "pending"},
                {"publish_status": "pending_retry"},
                {"publish_status": "failed"},
                {"publish_status": "unknown"},
            ]
        }
    )
    counts = summarize_publish_queue(state)
    assert counts == {"total": 5, "pending": 2, "completed": 1, "failed": 1}


def test_run_resume_publish_only_drains_queue_without_artwork_discovery(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    state = ensure_state_shape(
        {
            "publish_queue": [
                {
                    "artwork_key": "a1",
                    "template_key": "t1",
                    "shop_id": 111,
                    "product_id": "p-existing",
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "publish_status": "pending",
                    "publish_attempts": 0,
                    "last_error": "",
                    "reason_code": "",
                    "next_eligible_publish_at": "",
                }
            ]
        }
    )

    class DummyPrintifyClient:
        def __init__(self, *args, **kwargs):
            self.dry_run = False
            self.rate_limit_events = {}

        def publish_product(self, shop_id, product_id, payload):
            assert shop_id == 111
            assert product_id == "p-existing"
            return {"status": "published"}

    monkeypatch.setattr(pipeline, "PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "load_json", lambda path, default: state)
    monkeypatch.setattr(
        pipeline,
        "load_templates",
        lambda p: [ProductTemplate(key="t1", printify_blueprint_id=1, printify_print_provider_id=1, title_pattern="{artwork_title}", description_pattern="{artwork_title}")],
    )
    monkeypatch.setattr(pipeline, "select_templates", lambda templates, **kwargs: templates)
    monkeypatch.setattr(
        pipeline,
        "discover_artworks",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("resume publish-only should not discover artwork")),
    )
    monkeypatch.setattr(pipeline, "PrintifyClient", DummyPrintifyClient)
    monkeypatch.setattr(pipeline, "resolve_shop_id", lambda *args, **kwargs: 111)
    monkeypatch.setattr(pipeline, "load_r2_config_from_env", lambda: None)
    monkeypatch.setattr(pipeline, "audit_printify_integration", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "run_catalog_cli", lambda **kwargs: False)

    run_report = tmp_path / "resume_run_report.csv"
    failure_report = tmp_path / "resume_failure_report.csv"
    captured_summaries = []
    monkeypatch.setattr(pipeline, "log_run_summary", lambda summary: captured_summaries.append(summary))

    run(
        tmp_path / "templates.json",
        image_dir=tmp_path / "missing-images-ok-in-resume-mode",
        export_dir=tmp_path / "exp",
        state_path=tmp_path / "state.json",
        skip_audit=True,
        resume_publish_only=True,
        export_run_report=str(run_report),
        export_failure_report=str(failure_report),
    )
    assert state["publish_queue"][0]["publish_status"] == "completed"
    assert run_report.exists()
    assert failure_report.exists()
    assert captured_summaries
    summary = captured_summaries[-1]
    assert summary.publish_queue_total_count == 1
    assert summary.publish_queue_pending_count == 0
    assert summary.publish_queue_completed_count == 1
    assert summary.publish_queue_failed_count == 0
    assert summary.publish_attempts == 1
    assert summary.resumed_combinations == 1

    with run_report.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["artwork_slug"] == "a1"
    assert rows[0]["template_key"] == "t1"
    assert rows[0]["product_id"] == "p-existing"
    assert rows[0]["publish_queue_status_before"] == "pending"
    assert rows[0]["publish_queue_status_after"] == "completed"
    assert rows[0]["resume_only_queue_processing"] == "True"

    with failure_report.open(newline="", encoding="utf-8") as handle:
        assert list(csv.DictReader(handle)) == []


def test_run_resume_publish_only_empty_queue_reports_zero_processed(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    state = ensure_state_shape(
        {
            "publish_queue": [
                {
                    "artwork_key": "old",
                    "template_key": "old-template",
                    "shop_id": 111,
                    "product_id": "p-old",
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "publish_status": "completed",
                    "publish_attempts": 2,
                    "last_error": "",
                    "reason_code": "",
                    "next_eligible_publish_at": "",
                }
            ]
        }
    )

    class NoopPrintifyClient:
        def __init__(self, *args, **kwargs):
            self.dry_run = False
            self.rate_limit_events = {}

        def publish_product(self, *_a, **_k):
            raise AssertionError("should not publish when no pending rows")

    monkeypatch.setattr(pipeline, "PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "load_json", lambda path, default: state)
    monkeypatch.setattr(
        pipeline,
        "load_templates",
        lambda p: [ProductTemplate(key="old-template", printify_blueprint_id=1, printify_print_provider_id=1, title_pattern="{artwork_title}", description_pattern="{artwork_title}")],
    )
    monkeypatch.setattr(pipeline, "select_templates", lambda templates, **kwargs: templates)
    monkeypatch.setattr(pipeline, "discover_artworks", lambda *_a, **_k: [])
    monkeypatch.setattr(pipeline, "PrintifyClient", NoopPrintifyClient)
    monkeypatch.setattr(pipeline, "resolve_shop_id", lambda *args, **kwargs: 111)
    monkeypatch.setattr(pipeline, "load_r2_config_from_env", lambda: None)
    monkeypatch.setattr(pipeline, "audit_printify_integration", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "run_catalog_cli", lambda **kwargs: False)
    captured_summaries = []
    monkeypatch.setattr(pipeline, "log_run_summary", lambda summary: captured_summaries.append(summary))

    run_report = tmp_path / "empty_resume_run_report.csv"
    run(
        tmp_path / "templates.json",
        image_dir=tmp_path / "missing-images-ok-in-resume-mode",
        export_dir=tmp_path / "exp",
        state_path=tmp_path / "state.json",
        skip_audit=True,
        resume_publish_only=True,
        export_run_report=str(run_report),
    )
    summary = captured_summaries[-1]
    assert summary.publish_queue_total_count == 1
    assert summary.publish_queue_pending_count == 0
    assert summary.publish_queue_completed_count == 1
    assert summary.publish_attempts == 0
    assert summary.resumed_combinations == 0
    assert run_report.read_text(encoding="utf-8") == ""


def test_process_artwork_incompatible_update_suggests_rebuild(tmp_path: Path):
    class IncompatibleUpdatePrintify(DummyPrintify):
        dry_run = False

        def get_product(self, shop_id, product_id):
            return {
                "id": product_id,
                "blueprint_id": 1,
                "print_provider_id": 1,
                "variants": [{"id": 999, "is_enabled": True}],
                "print_areas": [{"placeholders": [{"position": "front"}]}],
            }

        def update_product(self, shop_id, product_id, payload):
            raise AssertionError("should not update when incompatible")

    artwork = _create_artwork(tmp_path, 1200, 1200)
    template = ProductTemplate(
        key="tee",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black"],
        enabled_sizes=["M"],
        publish_after_create=False,
        placements=[PlacementRequirement("front", 1000, 1000)],
    )
    state = ensure_state_shape({"processed": {"art": {"products": [{"template": "tee", "state_key": "art:tee", "result": {"printify": {"printify_product_id": "existing-1"}}}]}}})
    process_artwork(
        printify=IncompatibleUpdatePrintify(),
        shopify=None,
        shop_id=111,
        artwork=artwork,
        templates=[template],
        state=state,
        force=False,
        export_dir=tmp_path / "exports",
        state_path=tmp_path / "state.json",
        artwork_options=ArtworkProcessingOptions(),
        upload_strategy="auto",
        r2_config=None,
    )
    latest = state["processed"]["art"]["products"][-1]["result"]
    assert "--rebuild-product" in latest["error"]


def test_process_artwork_auto_rebuild_on_incompatible_update(tmp_path: Path):
    class AutoRebuildPrintify(DummyPrintify):
        dry_run = False

        def get_product(self, shop_id, product_id):
            return {
                "id": product_id,
                "blueprint_id": 1,
                "print_provider_id": 1,
                "variants": [{"id": 999, "is_enabled": True}],
                "print_areas": [{"placeholders": [{"position": "front"}]}],
            }

        def update_product(self, shop_id, product_id, payload):
            raise AssertionError("should rebuild, not update")

        def delete_product(self, shop_id, product_id):
            return {"deleted": True}

        def create_product(self, shop_id, payload):
            return {"id": "recreated-1"}

    artwork = _create_artwork(tmp_path, 1200, 1200)
    template = ProductTemplate(
        key="tee",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Black"],
        enabled_sizes=["M"],
        publish_after_create=False,
        placements=[PlacementRequirement("front", 1000, 1000)],
    )
    state = ensure_state_shape({"processed": {"art": {"products": [{"template": "tee", "state_key": "art:tee", "result": {"printify": {"printify_product_id": "existing-1"}}}]}}})
    process_artwork(
        printify=AutoRebuildPrintify(),
        shopify=None,
        shop_id=111,
        artwork=artwork,
        templates=[template],
        state=state,
        force=False,
        export_dir=tmp_path / "exports",
        state_path=tmp_path / "state.json",
        artwork_options=ArtworkProcessingOptions(),
        upload_strategy="auto",
        r2_config=None,
        auto_rebuild_on_incompatible_update=True,
    )
    latest = state["processed"]["art"]["products"][-1]["result"]["printify"]
    assert latest["action"] == "rebuild"
    assert latest["printify_product_id"] == "recreated-1"

def test_report_row_status_classification():
    assert _row_status({"result": {"error": "boom"}}) == "failure"
    assert _row_status({"result": {"printify": {"status": "skipped"}}}) == "skipped"
    assert _row_status({"result": {"status": "no_matching_variants"}}) == "skipped"
    assert _row_status({"result": {"printify": {"status": "dry-run"}}, "dry_run": True, "completion_status": "dry-run-only"}) == "dry-run"
    assert _row_status({"result": {"printify": {"action": "create"}}}) == "success"


def test_write_csv_report_outputs_rows(tmp_path: Path):
    out = tmp_path / "run.csv"
    write_csv_report(out, [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    text = out.read_text(encoding="utf-8")
    assert "a,b" in text
    assert "1,x" in text


def test_run_report_export_includes_eligibility_gate_columns(tmp_path: Path):
    out = tmp_path / "run.csv"
    row = RunReportRow(
        timestamp="2026-01-01T00:00:00+00:00",
        artwork_filename="art.png",
        artwork_slug="art",
        template_key="canvas_basic",
        status="skipped",
        action="skip",
        blueprint_id=944,
        provider_id=105,
        upload_strategy="direct",
        product_id="",
        publish_attempted=False,
        publish_verified=False,
        rendered_title="Art",
        source_size="1280x1280",
        required_placement_size="4500x5400",
        required_fit_mode="cover",
        eligibility_high_resolution_family=True,
        eligibility_outcome="ineligible",
        eligibility_reason_code="insufficient_artwork_resolution",
        eligibility_rule_failed="min_source_width",
        eligibility_gate_stage="eligibility_gate",
    )
    write_csv_report(out, [row.__dict__])
    text = out.read_text(encoding="utf-8")
    assert "eligibility_reason_code" in text
    assert "required_placement_size" in text
    assert "eligibility_gate_stage" in text
    assert "insufficient_artwork_resolution" in text


def test_run_batch_size_and_resume_and_reporting(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    state = ensure_state_shape({"processed": {"a1": {"products": [{"state_key": "a1:t1", "result": {"printify": {"action": "create"}}}]}}})

    class DummyPrintifyClient:
        def __init__(self, *args, **kwargs):
            self.dry_run = True

    monkeypatch.setattr(pipeline, "PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "load_json", lambda path, default: state)
    monkeypatch.setattr(pipeline, "load_templates", lambda p: [
        ProductTemplate(key="t1", printify_blueprint_id=1, printify_print_provider_id=1, title_pattern="{artwork_title}", description_pattern="{artwork_title}"),
        ProductTemplate(key="t2", printify_blueprint_id=1, printify_print_provider_id=1, title_pattern="{artwork_title}", description_pattern="{artwork_title}"),
    ])
    monkeypatch.setattr(pipeline, "select_templates", lambda templates, **kwargs: templates)
    monkeypatch.setattr(pipeline, "discover_artworks", lambda d: [
        Artwork("a1", tmp_path / "a1.png", "a1", "", [], 1000, 1000),
        Artwork("a2", tmp_path / "a2.png", "a2", "", [], 1000, 1000),
    ])
    monkeypatch.setattr(pipeline, "PrintifyClient", DummyPrintifyClient)
    monkeypatch.setattr(pipeline, "resolve_shop_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "load_r2_config_from_env", lambda: None)
    monkeypatch.setattr(pipeline, "audit_printify_integration", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "run_catalog_cli", lambda **kwargs: False)

    def fake_process_artwork(**kwargs):
        template = kwargs["templates"][0]
        artwork = kwargs["artwork"]
        kwargs["run_rows"].append(pipeline.RunReportRow(
            timestamp="now",
            artwork_filename=artwork.src_path.name,
            artwork_slug=artwork.slug,
            template_key=template.key,
            status="success",
            action="create",
            blueprint_id=1,
            provider_id=1,
            upload_strategy="auto",
            product_id="p1",
            publish_attempted=False,
            publish_verified=False,
            rendered_title="x",
        ))

    monkeypatch.setattr(pipeline, "process_artwork", fake_process_artwork)

    run_report = tmp_path / "run_report.csv"
    run(
        tmp_path / "templates.json",
        image_dir=tmp_path,
        export_dir=tmp_path / "exp",
        state_path=tmp_path / "state.json",
        skip_audit=True,
        resume=True,
        batch_size=2,
        export_run_report=str(run_report),
    )
    rows = [r for r in run_report.read_text(encoding="utf-8").splitlines() if r.strip()]
    assert len(rows) == 3  # header + 2 rows


def test_resume_does_not_skip_dry_run_only_rows(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    state = ensure_state_shape({
        "processed": {
            "a1": {"products": [{"state_key": "a1:t1", "dry_run": True, "completion_status": "dry-run-only", "result": {"printify": {"status": "dry-run"}}}]}
        }
    })

    class DummyPrintifyClient:
        def __init__(self, *args, **kwargs):
            self.dry_run = False

    monkeypatch.setattr(pipeline, "PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "load_json", lambda path, default: state)
    monkeypatch.setattr(pipeline, "load_templates", lambda p: [
        ProductTemplate(key="t1", printify_blueprint_id=1, printify_print_provider_id=1, title_pattern="{artwork_title}", description_pattern="{artwork_title}"),
    ])
    monkeypatch.setattr(pipeline, "select_templates", lambda templates, **kwargs: templates)
    monkeypatch.setattr(pipeline, "discover_artworks", lambda d: [Artwork("a1", tmp_path / "a1.png", "a1", "", [], 1000, 1000)])
    monkeypatch.setattr(pipeline, "PrintifyClient", DummyPrintifyClient)
    monkeypatch.setattr(pipeline, "resolve_shop_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "load_r2_config_from_env", lambda: None)
    monkeypatch.setattr(pipeline, "audit_printify_integration", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "run_catalog_cli", lambda **kwargs: False)

    calls = {"n": 0}

    def fake_process_artwork(**kwargs):
        calls["n"] += 1

    monkeypatch.setattr(pipeline, "process_artwork", fake_process_artwork)

    run(
        tmp_path / "templates.json",
        image_dir=tmp_path,
        export_dir=tmp_path / "exp",
        state_path=tmp_path / "state.json",
        skip_audit=True,
        resume=True,
    )
    assert calls["n"] == 1


def test_resume_skips_real_completed_rows(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    state = ensure_state_shape({
        "processed": {
            "a1": {"products": [{"state_key": "a1:t1", "dry_run": False, "completion_status": "real-completed", "result": {"printify": {"action": "create"}}}]}
        }
    })

    class DummyPrintifyClient:
        def __init__(self, *args, **kwargs):
            self.dry_run = False

    monkeypatch.setattr(pipeline, "PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "load_json", lambda path, default: state)
    monkeypatch.setattr(pipeline, "load_templates", lambda p: [
        ProductTemplate(key="t1", printify_blueprint_id=1, printify_print_provider_id=1, title_pattern="{artwork_title}", description_pattern="{artwork_title}"),
    ])
    monkeypatch.setattr(pipeline, "select_templates", lambda templates, **kwargs: templates)
    monkeypatch.setattr(pipeline, "discover_artworks", lambda d: [Artwork("a1", tmp_path / "a1.png", "a1", "", [], 1000, 1000)])
    monkeypatch.setattr(pipeline, "PrintifyClient", DummyPrintifyClient)
    monkeypatch.setattr(pipeline, "resolve_shop_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "load_r2_config_from_env", lambda: None)
    monkeypatch.setattr(pipeline, "audit_printify_integration", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "run_catalog_cli", lambda **kwargs: False)

    calls = {"n": 0}

    def fake_process_artwork(**kwargs):
        calls["n"] += 1

    monkeypatch.setattr(pipeline, "process_artwork", fake_process_artwork)

    run(
        tmp_path / "templates.json",
        image_dir=tmp_path,
        export_dir=tmp_path / "exp",
        state_path=tmp_path / "state.json",
        skip_audit=True,
        resume=True,
    )
    assert calls["n"] == 0


def test_run_stop_after_failures_and_fail_fast(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    monkeypatch.setattr(pipeline, "PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "load_json", lambda path, default: ensure_state_shape({}))
    monkeypatch.setattr(pipeline, "load_templates", lambda p: [
        ProductTemplate(key="t1", printify_blueprint_id=1, printify_print_provider_id=1, title_pattern="{artwork_title}", description_pattern="{artwork_title}"),
        ProductTemplate(key="t2", printify_blueprint_id=1, printify_print_provider_id=1, title_pattern="{artwork_title}", description_pattern="{artwork_title}"),
    ])
    monkeypatch.setattr(pipeline, "select_templates", lambda templates, **kwargs: templates)
    monkeypatch.setattr(pipeline, "discover_artworks", lambda d: [Artwork("a1", tmp_path / "a1.png", "a1", "", [], 1000, 1000)])
    monkeypatch.setattr(pipeline, "PrintifyClient", lambda *args, **kwargs: type("X", (), {"dry_run": True})())
    monkeypatch.setattr(pipeline, "resolve_shop_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "load_r2_config_from_env", lambda: None)
    monkeypatch.setattr(pipeline, "audit_printify_integration", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "run_catalog_cli", lambda **kwargs: False)

    calls = {"n": 0}

    def fake_process_artwork(**kwargs):
        calls["n"] += 1
        kwargs["summary"].failures += 1
        kwargs["failure_rows"].append(pipeline.FailureReportRow("now", "a1.png", "a1", kwargs["templates"][0].key, "create", 1, 1, "auto", "RuntimeError", "runtime_error", "boom", "fix"))
        kwargs["run_rows"].append(pipeline.RunReportRow("now", "a1.png", "a1", kwargs["templates"][0].key, "failure", "create", 1, 1, "auto", "", False, False, "x"))

    monkeypatch.setattr(pipeline, "process_artwork", fake_process_artwork)
    run(tmp_path / "templates.json", image_dir=tmp_path, export_dir=tmp_path / "exp", state_path=tmp_path / "state.json", skip_audit=True, stop_after_failures=1)
    assert calls["n"] == 1

    calls["n"] = 0
    run(tmp_path / "templates.json", image_dir=tmp_path, export_dir=tmp_path / "exp", state_path=tmp_path / "state.json", skip_audit=True, fail_fast=True)
    assert calls["n"] == 1

def test_list_failures_and_pending_helpers(tmp_path: Path, monkeypatch, capsys):
    import printify_shopify_sync_pipeline as pipeline

    state = ensure_state_shape({
        "processed": {
            "a1": {"products": [{"state_key": "a1:t1", "result": {"error": "bad asset"}}]},
            "a2": {"products": [{"state_key": "a2:t1", "result": {"printify": {"action": "create"}}}]},
        }
    })
    monkeypatch.setattr(pipeline, "load_json", lambda path, default: state)
    monkeypatch.setattr(pipeline, "load_templates", lambda p: [ProductTemplate(key="t1", printify_blueprint_id=1, printify_print_provider_id=1, title_pattern="{artwork_title}", description_pattern="{artwork_title}")])
    monkeypatch.setattr(pipeline, "select_templates", lambda templates, **kwargs: templates)
    monkeypatch.setattr(pipeline, "discover_artworks", lambda d: [Artwork("a1", tmp_path / "a1.png", "a1", "", [], 1, 1), Artwork("a2", tmp_path / "a2.png", "a2", "", [], 1, 1), Artwork("a3", tmp_path / "a3.png", "a3", "", [], 1, 1)])

    run(tmp_path / "templates.json", image_dir=tmp_path, state_path=tmp_path / "state.json", list_failures_only=True)
    out = capsys.readouterr().out
    assert "a1:t1" in out

    run(tmp_path / "templates.json", image_dir=tmp_path, state_path=tmp_path / "state.json", list_pending_only=True)
    out2 = capsys.readouterr().out
    assert "a1:t1" in out2 and "a3:t1" in out2




def test_list_and_inspect_state_show_completion_status(tmp_path: Path, monkeypatch, capsys):
    import printify_shopify_sync_pipeline as pipeline

    state = ensure_state_shape({
        "processed": {
            "a1": {"products": [{"state_key": "a1:t1", "dry_run": True, "completion_status": "dry-run-only", "result": {"printify": {"status": "dry-run"}}}]},
            "a2": {"products": [{"state_key": "a2:t1", "dry_run": False, "completion_status": "real-completed", "result": {"printify": {"action": "create"}}}]},
        }
    })
    monkeypatch.setattr(pipeline, "load_json", lambda path, default: state)

    run(tmp_path / "templates.json", image_dir=tmp_path, state_path=tmp_path / "state.json", list_state_keys_only=True)
    out = capsys.readouterr().out
    assert "a1:t1	dry-run-only" in out
    assert "a2:t1	real-completed" in out

    run(tmp_path / "templates.json", image_dir=tmp_path, state_path=tmp_path / "state.json", inspect_state_key_value="a1:t1")
    inspect_out = capsys.readouterr().out
    assert '"completion_status": "dry-run-only"' in inspect_out


def test_enforce_variant_safety_limit_raises_when_exceeded():
    template = ProductTemplate(
        key="mug-test",
        printify_blueprint_id=9,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        max_enabled_variants=2,
    )
    with pytest.raises(RuntimeError, match="exceeds safety cap"):
        enforce_variant_safety_limit(template=template, enabled_variant_count=3)


def test_choose_variants_from_catalog_applies_option_filters_and_cap():
    template = ProductTemplate(
        key="mug-test",
        printify_blueprint_id=9,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_variant_option_filters={"material": ["Ceramic"]},
        max_enabled_variants=1,
    )
    variants = [
        {"id": 1, "is_available": True, "options": {"color": "White", "size": "11oz", "material": "Ceramic"}},
        {"id": 2, "is_available": True, "options": {"color": "Black", "size": "11oz", "material": "Ceramic"}},
        {"id": 3, "is_available": True, "options": {"color": "White", "size": "11oz", "material": "Glass"}},
    ]
    chosen = choose_variants_from_catalog(variants, template)
    assert [row["id"] for row in chosen] == [1]


def test_choose_variants_from_catalog_resolves_provider_option_aliases():
    template = ProductTemplate(
        key="phone_case_basic",
        printify_blueprint_id=9,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_colors=["Glossy"],
        enabled_variant_option_filters={"model": ["iPhone 15 Pro"]},
    )
    variants = [
        {"id": 1, "is_available": True, "options": {"Device Model": "iPhone 15 Pro", "Finish": "Glossy"}},
        {"id": 2, "is_available": True, "options": {"Device Model": "iPhone 14", "Finish": "Glossy"}},
        {"id": 3, "is_available": True, "options": {"Device Model": "iPhone 15 Pro", "Finish": "Matte"}},
    ]
    chosen = choose_variants_from_catalog(variants, template)
    assert [row["id"] for row in chosen] == [1]


def test_preflight_zero_selection_includes_option_filter_diagnostics():
    class DummyPrintify:
        def list_blueprints(self):
            return [{"id": 783}]

        def list_print_providers(self, blueprint_id):
            return [{"id": 90, "title": "Sticker Provider"}]

        def list_variants(self, blueprint_id, provider_id):
            return [
                {"id": 101, "is_available": True, "options": {"Shape": "Die-Cut", "Size": '3" × 3"'}},
                {"id": 102, "is_available": True, "options": {"Shape": "Die-Cut", "Size": '4" × 4"'}},
            ]

    sticker = ProductTemplate(
        "sticker_kisscut",
        783,
        90,
        "{artwork_title}",
        "{artwork_title}",
        active=True,
        enabled_variant_option_filters={"finish": ["Glossy"]},
    )
    passed, issues, report_rows = preflight_active_templates(printify=DummyPrintify(), templates=[sticker], explicit_template_keys=[])
    assert passed == []
    assert issues and issues[0].classification == "zero_variants_selected"
    assert "available_option_names" in issues[0].message
    assert "requested_filters" in issues[0].message
    row = report_rows[0]
    assert row.classification == "zero_variants_selected"
    assert "Shape" in row.option_names
    assert '"finish": ["Glossy"]' in row.requested_option_filters
    assert "not present in provider schema" in row.zero_selection_reason


def test_catalog_family_validation_flags_phone_case_mapped_to_apparel_schema():
    template = ProductTemplate(
        "phone_case_basic",
        9,
        99,
        "{artwork_title}",
        "{artwork_title}",
    )
    variants = [
        {"id": 1, "is_available": True, "options": {"color": "Black", "size": "M"}},
        {"id": 2, "is_available": True, "options": {"color": "Navy", "size": "L"}},
    ]
    result = validate_catalog_family_schema(template=template, variants=variants, blueprint_title="Unisex Tee")
    assert result.plausible is False
    assert result.intended_family == "phone_case"


def test_catalog_family_validation_flags_sticker_mapped_to_textile_schema():
    template = ProductTemplate(
        "sticker_kisscut",
        783,
        90,
        "{artwork_title}",
        "{artwork_title}",
    )
    variants = [
        {
            "id": 1,
            "is_available": True,
            "options": {"size": "M", "color": "White", "material": "Seam thread color automatically matched to design"},
        }
    ]
    result = validate_catalog_family_schema(template=template, variants=variants, blueprint_title="Pillow Cover")
    assert result.plausible is False
    assert result.intended_family == "sticker"


def test_preflight_classifies_wrong_catalog_family_before_zero_variant_filtering():
    class DummyPrintify:
        def list_blueprints(self):
            return [{"id": 9, "title": "Unisex Tee"}]

        def list_print_providers(self, blueprint_id):
            return [{"id": 99, "title": "Provider"}]

        def list_variants(self, blueprint_id, provider_id):
            return [{"id": 1, "is_available": True, "options": {"color": "Black", "size": "M"}}]

    template = ProductTemplate(
        "phone_case_basic",
        9,
        99,
        "{artwork_title}",
        "{artwork_title}",
        enabled_variant_option_filters={"model": ["iPhone 15"]},
        provider_selection_strategy="pinned_then_printify_choice_then_lowest_cost",
    )
    passed, issues, report_rows = preflight_active_templates(printify=DummyPrintify(), templates=[template], explicit_template_keys=[])
    assert passed == []
    assert issues and issues[0].classification == "wrong_catalog_family"
    row = report_rows[0]
    assert row.classification == "wrong_catalog_family"
    assert row.intended_family == "phone_case"
    assert "schema mismatch" in row.family_mismatch_reason.lower()
    assert row.pinned_mapping_attempted_first is True
    assert row.fallback_discovery_triggered is True
    assert row.fallback_discovery_reason.startswith("template_mapping_family_mismatch:")


def test_select_provider_for_template_discovers_family_matched_catalog_pair():
    class DummyPrintify:
        def list_blueprints(self):
            return [
                {"id": 9, "title": "Unisex Tee"},
                {"id": 210, "title": "Tough Phone Cases"},
            ]

        def list_print_providers(self, blueprint_id):
            if blueprint_id == 9:
                return [{"id": 99, "title": "Generic"}]
            if blueprint_id == 210:
                return [{"id": 55, "title": "Printify Choice"}]
            return []

        def list_variants(self, blueprint_id, provider_id):
            if blueprint_id == 9:
                return [{"id": 1, "is_available": True, "options": {"color": "Black", "size": "M"}}]
            if blueprint_id == 210:
                return [{"id": 2, "is_available": True, "options": {"Device Model": "iPhone 15 Pro", "Finish": "Glossy"}}]
            return []

    template = ProductTemplate(
        "phone_case_basic",
        9,
        99,
        "{artwork_title}",
        "{artwork_title}",
        provider_selection_strategy="pinned_then_printify_choice_then_lowest_cost",
    )
    resolved = select_provider_for_template(printify=DummyPrintify(), template=template)
    assert resolved.printify_blueprint_id == 210
    assert resolved.printify_print_provider_id == 55


def test_select_provider_for_template_keeps_proven_sticker_mapping_without_discovery():
    class DummyPrintify:
        def list_blueprints(self):
            raise AssertionError("broad discovery should not run for proven sticker mapping")

        def list_print_providers(self, blueprint_id):
            assert blueprint_id == 906
            return [{"id": 36, "title": "SPOKE Custom Products"}]

        def list_variants(self, blueprint_id, provider_id):
            assert blueprint_id == 906
            assert provider_id == 36
            return [{"id": 1, "is_available": True, "options": {"Shape": "Kiss-Cut", "Size": "4x4"}}]

    template = ProductTemplate("sticker_kisscut", 906, 36, "{artwork_title}", "{artwork_title}")
    resolved = select_provider_for_template(printify=DummyPrintify(), template=template)
    assert resolved.printify_blueprint_id == 906
    assert resolved.printify_print_provider_id == 36


def test_preflight_reports_template_hint_vs_runtime_mapping_when_they_differ():
    class DummyPrintify:
        def list_blueprints(self):
            return [
                {"id": 9, "title": "Unisex Tee"},
                {"id": 210, "title": "Tough Phone Cases"},
            ]

        def list_print_providers(self, blueprint_id):
            if blueprint_id == 9:
                return [{"id": 99, "title": "Generic"}]
            return [{"id": 55, "title": "Printify Choice"}]

        def list_variants(self, blueprint_id, provider_id):
            if blueprint_id == 210:
                return [{"id": 2, "is_available": True, "options": {"Device Model": "iPhone 15 Pro", "Finish": "Glossy"}, "cost": 1200, "price": 1900}]
            return [{"id": 1, "is_available": True, "options": {"color": "Black", "size": "M"}}]

    template = ProductTemplate("phone_case_basic", 9, 99, "{artwork_title}", "{artwork_title}")
    passed, issues, report_rows = preflight_active_templates(printify=DummyPrintify(), templates=[template], explicit_template_keys=[])
    assert len(passed) == 1
    assert issues == []
    row = report_rows[0]
    assert row.template_hint_blueprint_id == 9
    assert row.template_hint_provider_id == 99
    assert row.blueprint_id == 210
    assert row.provider_id == 55
    assert row.runtime_mapping_overrode_hint is True


def test_preflight_resolution_diagnostics_report_discovery_usage_for_sticker():
    class DummyPrintify:
        def list_blueprints(self):
            return [{"id": 906, "title": "Kiss-Cut Stickers"}]

        def list_print_providers(self, blueprint_id):
            return [{"id": 36, "title": "SPOKE Custom Products"}]

        def list_variants(self, blueprint_id, provider_id):
            return [{"id": 1, "is_available": True, "options": {"Shape": "Kiss-Cut", "Size": "4x4"}, "cost": 300, "price": 700}]

    template = ProductTemplate("sticker_kisscut", 906, 36, "{artwork_title}", "{artwork_title}")
    passed, issues, report_rows = preflight_active_templates(printify=DummyPrintify(), templates=[template], explicit_template_keys=[])
    assert issues == []
    assert len(passed) == 1
    row = report_rows[0]
    assert row.template_hint_blueprint_id == 906
    assert row.template_hint_provider_id == 36
    assert row.catalog_discovery_used is False
    assert row.pinned_mapping_attempted_first is True
    assert row.fallback_discovery_triggered is False
    assert row.fallback_discovery_reason == ""


def test_preflight_phone_case_can_recover_with_real_model_variants():
    class DummyPrintify:
        def list_blueprints(self):
            return [{"id": 210, "title": "Tough Phone Cases"}]

        def list_print_providers(self, blueprint_id):
            return [{"id": 55, "title": "Printify Choice"}]

        def list_variants(self, blueprint_id, provider_id):
            return [
                {"id": 1, "is_available": True, "options": {"Device Model": "iPhone 15", "Finish": "Glossy"}, "cost": 1200, "price": 1600},
                {"id": 2, "is_available": True, "options": {"Device Model": "iPhone 14", "Finish": "Glossy"}, "cost": 1200, "price": 1600},
            ]

    template = ProductTemplate(
        "phone_case_basic",
        210,
        55,
        "{artwork_title}",
        "{artwork_title}",
        enabled_variant_option_filters={"model": ["iPhone 15"], "finish": ["Glossy"]},
        provider_selection_strategy="pinned_then_printify_choice_then_lowest_cost",
    )
    passed, issues, _ = preflight_active_templates(printify=DummyPrintify(), templates=[template], explicit_template_keys=[])
    assert issues == []
    assert len(passed) == 1


def test_preflight_phone_case_recovers_with_provider_backed_fallback_models():
    class DummyPrintify:
        def list_blueprints(self):
            return [{"id": 421, "title": "Tough Phone Cases"}]

        def list_print_providers(self, blueprint_id):
            return [{"id": 23, "title": "Phone Provider"}]

        def list_variants(self, blueprint_id, provider_id):
            return [
                {"id": 1, "is_available": True, "options": {"Size": "iPhone 11", "Surface": "Glossy"}, "cost": 1200, "price": 1900},
                {"id": 2, "is_available": True, "options": {"Size": "iPhone 8", "Surface": "Glossy"}, "cost": 1200, "price": 1900},
                {"id": 3, "is_available": True, "options": {"Size": "Samsung Galaxy S20 Plus", "Surface": "Glossy"}, "cost": 1200, "price": 1900},
            ]

    template = ProductTemplate(
        "phone_case_basic",
        421,
        23,
        "{artwork_title}",
        "{artwork_title}",
        enabled_variant_option_filters={"model": ["iPhone 15 Pro", "Samsung Galaxy S24"], "surface": ["Glossy"]},
        provider_selection_strategy="pinned_then_printify_choice_then_lowest_cost",
    )
    passed, issues, report_rows = preflight_active_templates(printify=DummyPrintify(), templates=[template], explicit_template_keys=[])
    assert issues == []
    assert len(passed) == 1
    row = report_rows[0]
    assert row.template_hint_blueprint_id == 421
    assert row.template_hint_provider_id == 23
    assert row.catalog_discovery_used is False
    assert row.pinned_mapping_attempted_first is True
    assert row.fallback_discovery_triggered is False
    assert row.fallback_discovery_reason == ""
    assert row.resolved_model_dimension == "Size"
    assert row.requested_model_overlap_count == 0
    assert row.fallback_model_set_applied is True
    assert "iPhone 11" in row.final_selected_models
    assert row.final_selected_models.count(",") <= 3


def test_preflight_sticker_remains_inactive_with_correct_family_when_requested_size_missing():
    class DummyPrintify:
        def list_blueprints(self):
            return [{"id": 501, "title": "Kiss-Cut Stickers"}]

        def list_print_providers(self, blueprint_id):
            return [{"id": 42, "title": "Sticker Provider"}]

        def list_variants(self, blueprint_id, provider_id):
            return [{"id": 1, "is_available": True, "options": {"Shape": "Kiss-Cut", "Size": '2" × 2"'}}]

    template = ProductTemplate(
        "sticker_kisscut",
        501,
        42,
        "{artwork_title}",
        "{artwork_title}",
        enabled_variant_option_filters={"size": ['4" × 4"']},
        provider_selection_strategy="pinned_then_printify_choice_then_lowest_cost",
    )
    passed, issues, report_rows = preflight_active_templates(printify=DummyPrintify(), templates=[template], explicit_template_keys=[])
    assert passed == []
    assert issues and issues[0].classification == "zero_variants_selected"
    assert report_rows[0].classification == "zero_variants_selected"
    assert report_rows[0].intended_family == "sticker"


def test_validate_catalog_family_schema_rejects_wrong_family_for_canvas():
    template = ProductTemplate("canvas_basic", 13, 1, "{artwork_title}", "{artwork_title}", product_type_label="Canvas Print")
    variants = [{"id": 1, "is_available": True, "options": {"Device Model": "iPhone 15", "Finish": "Glossy"}}]
    result = validate_catalog_family_schema(template=template, variants=variants, blueprint_title="Tough Phone Cases")
    assert result.intended_family == "canvas"
    assert result.plausible is False
    assert "canvas schema mismatch" in (result.reason or "").lower()


def test_validate_catalog_family_schema_rejects_wrong_family_for_blanket():
    template = ProductTemplate("blanket_basic", 50, 1, "{artwork_title}", "{artwork_title}", product_type_label="Blanket")
    variants = [{"id": 1, "is_available": True, "options": {"Color": "Black", "Size": "M"}}]
    result = validate_catalog_family_schema(template=template, variants=variants, blueprint_title="Unisex Heavy Blend Hoodie")
    assert result.intended_family == "blanket"
    assert result.plausible is False
    assert "blanket schema mismatch" in (result.reason or "").lower()


def test_select_provider_for_template_discovers_canvas_mapping_when_template_hint_is_wrong_family():
    class DummyPrintify:
        def list_blueprints(self):
            return [
                {"id": 9, "title": "Unisex Tee"},
                {"id": 13, "title": "Matte Canvas, Framed"},
            ]

        def list_print_providers(self, blueprint_id):
            if blueprint_id == 9:
                return [{"id": 99, "title": "Generic"}]
            if blueprint_id == 13:
                return [{"id": 7, "title": "Printify Choice"}]
            return []

        def list_variants(self, blueprint_id, provider_id):
            if blueprint_id == 9:
                return [{"id": 1, "is_available": True, "options": {"Color": "Black", "Size": "M"}}]
            if blueprint_id == 13 and provider_id == 7:
                return [
                    {"id": 2, "is_available": True, "cost": 2200, "price": 3000, "options": {"Size": '12" x 16"'}},
                    {"id": 3, "is_available": True, "cost": 2600, "price": 3400, "options": {"Size": '16" x 20"'}},
                ]
            return []

    template = ProductTemplate(
        "canvas_basic",
        9,
        99,
        "{artwork_title}",
        "{artwork_title}",
        active=False,
        enabled_variant_option_filters={"size": ['9" x 12"', '11" x 14"']},
        provider_selection_strategy="prefer_printify_choice_then_ranked",
    )
    resolved = select_provider_for_template(printify=DummyPrintify(), template=template)
    assert resolved.printify_blueprint_id == 13
    assert resolved.printify_print_provider_id == 7


def test_canvas_size_normalization_matches_provider_quotes_and_orientation_labels():
    template = ProductTemplate(
        "canvas_basic",
        944,
        105,
        "{artwork_title}",
        "{artwork_title}",
        enabled_sizes=['9" x 12"', "11″ x 14″"],
    )
    variants = [
        {"id": 1, "is_available": True, "options": {"size": '9" x 12" (Vertical)'}},
        {"id": 2, "is_available": True, "options": {"size": "11″ x 14″ (Vertical)"}},
        {"id": 3, "is_available": True, "options": {"size": '16" x 12" (Horizontal)'}},
    ]
    selected, diagnostics = choose_variants_from_catalog_with_diagnostics(variants, template)
    assert sorted([row["id"] for row in selected]) == [1, 2]
    assert diagnostics.zero_selection_reason == ""


def test_preflight_canvas_reports_size_filter_mismatch_classification_with_valid_family():
    class DummyPrintify:
        def list_blueprints(self):
            return [{"id": 944, "title": "Canvas Print"}]

        def list_print_providers(self, blueprint_id):
            return [{"id": 105, "title": "Canvas Provider"}]

        def list_variants(self, blueprint_id, provider_id):
            return [
                {"id": 1, "is_available": True, "cost": 2200, "price": 3200, "options": {"size": '9" x 12" (Vertical)'}},
                {"id": 2, "is_available": True, "cost": 2400, "price": 3400, "options": {"size": "11″ x 14″ (Vertical)"}},
            ]

    template = ProductTemplate(
        "canvas_basic",
        944,
        105,
        "{artwork_title}",
        "{artwork_title}",
        active=False,
        enabled_sizes=["12″ x 16″", "16″ x 20″"],
        provider_selection_strategy="prefer_printify_choice_then_ranked",
    )
    passed, issues, rows = preflight_active_templates(printify=DummyPrintify(), templates=[template], explicit_template_keys=[])
    assert passed == []
    assert issues and issues[0].classification == "canvas_size_filter_mismatch"
    assert rows[0].classification == "canvas_size_filter_mismatch"


def test_preflight_canvas_recovers_with_provider_backed_vertical_sizes_and_stays_inactive():
    class DummyPrintify:
        def list_blueprints(self):
            return [
                {"id": 13, "title": "Unisex Tee"},
                {"id": 944, "title": "Canvas Print"},
            ]

        def list_print_providers(self, blueprint_id):
            if blueprint_id == 13:
                return [{"id": 1, "title": "Legacy Tee Provider"}]
            return [{"id": 105, "title": "Canvas Provider"}]

        def list_variants(self, blueprint_id, provider_id):
            if blueprint_id == 13:
                return [{"id": 10, "is_available": True, "options": {"color": "Black", "size": "M"}}]
            return [
                {"id": 21, "is_available": True, "cost": 2200, "price": 3200, "options": {"size": '9" x 12" (Vertical)', "depth": '1.25"'}},
                {"id": 22, "is_available": True, "cost": 2400, "price": 3400, "options": {"size": "11″ x 14″ (Vertical)", "depth": '1.25"'}},
                {"id": 23, "is_available": True, "cost": 2600, "price": 3600, "options": {"size": '16" x 12" (Horizontal)', "depth": '1.25"'}},
            ]

    template = ProductTemplate(
        "canvas_basic",
        13,
        1,
        "{artwork_title}",
        "{artwork_title}",
        active=False,
        enabled_sizes=['9" x 12"', '11" x 14"'],
        provider_selection_strategy="prefer_printify_choice_then_ranked",
    )
    passed, issues, rows = preflight_active_templates(printify=DummyPrintify(), templates=[template], explicit_template_keys=[])
    assert issues == []
    assert len(passed) == 1
    assert passed[0].active is False
    assert rows[0].blueprint_id == 944
    assert rows[0].provider_id == 105
    assert rows[0].catalog_discovery_used is True


def test_preflight_blanket_recovers_to_plausible_mapping_and_stays_inactive():
    class DummyPrintify:
        def list_blueprints(self):
            return [
                {"id": 9, "title": "Unisex Tee"},
                {"id": 50, "title": "Mink-Cotton Blanket"},
            ]

        def list_print_providers(self, blueprint_id):
            if blueprint_id == 9:
                return [{"id": 99, "title": "Generic"}]
            return [{"id": 4, "title": "Printify Choice"}]

        def list_variants(self, blueprint_id, provider_id):
            if blueprint_id == 9:
                return [{"id": 1, "is_available": True, "options": {"Color": "Black", "Size": "M"}}]
            return [
                {"id": 11, "is_available": True, "cost": 2800, "price": 3600, "options": {"Size": '50" × 60"', "Material": "Mink"}},
                {"id": 12, "is_available": True, "cost": 3300, "price": 4300, "options": {"Size": '60" × 80"', "Material": "Mink"}},
            ]

    template = ProductTemplate(
        "blanket_basic",
        9,
        99,
        "{artwork_title}",
        "{artwork_title}",
        active=False,
        enabled_sizes=['50" × 60"', '60" × 80"'],
        provider_selection_strategy="prefer_printify_choice_then_ranked",
    )
    passed, issues, rows = preflight_active_templates(printify=DummyPrintify(), templates=[template], explicit_template_keys=[])
    assert issues == []
    assert len(passed) == 1
    assert passed[0].active is False
    row = rows[0]
    assert row.template_key == "blanket_basic"
    assert row.template_hint_blueprint_id == 9
    assert row.blueprint_id == 50
    assert row.provider_id == 4
    assert row.catalog_discovery_used is True
    assert row.fallback_discovery_triggered is True
    assert row.intended_family == "blanket"
    assert row.final_enabled_count > 0


def test_upsert_in_printify_recovers_from_stale_product_id(tmp_path: Path):
    artwork = _create_artwork(tmp_path, 3000, 3000)
    template = ProductTemplate(
        key="mug-test",
        printify_blueprint_id=9,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[PlacementRequirement("front", 1000, 1000)],
    )

    class DummyPrintifyStaleRecovery:
        dry_run = False

        def get_product(self, shop_id, product_id):
            raise NonRetryableRequestError("HTTP 404 for GET /shops/1/products/stale.json")

        def create_product(self, shop_id, payload):
            return {"id": "new-123"}

        def publish_product(self, shop_id, product_id, payload):
            return {"status": "ok"}

    result = upsert_in_printify(
        printify=DummyPrintifyStaleRecovery(),
        shop_id=1,
        artwork=artwork,
        template=template,
        variant_rows=[{"id": 101, "is_available": True, "price": 1200, "options": {"color": "White", "size": "11oz"}}],
        upload_map={"front": {"id": "upload-1"}},
        existing_product_id="stale",
        action="update",
        publish_mode="skip",
        verify_publish=False,
    )
    assert result["action"] == "create"
    assert result["printify_product_id"] == "new-123"


def test_classifies_printify_update_incompatible_8251_error():
    exc = NonRetryableRequestError(
        "HTTP 400 for PUT /shops/1/products/p1.json: {'code': 8251, "
        "'message': 'Variants do not match selected blueprint and print provider.'}"
    )
    assert _is_printify_update_incompatible_error(exc) is True


def test_classifies_printify_update_edit_disabled_8252_error():
    exc = NonRetryableRequestError(
        "HTTP 400 for PUT /shops/1/products/p1.json: {'code': 8252, "
        "'message': 'Product is disabled for editing'}"
    )
    assert _is_printify_product_edit_disabled_error(exc) is True


def test_upsert_in_printify_rebuilds_after_8251_update_rejection(tmp_path: Path):
    artwork = _create_artwork(tmp_path, 3000, 3000)
    template = ProductTemplate(
        key="tee-test",
        printify_blueprint_id=9,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[PlacementRequirement("front", 1000, 1000)],
    )

    class DummyPrintify8251Fallback:
        dry_run = False

        def get_product(self, shop_id, product_id):
            return {
                "id": product_id,
                "blueprint_id": 9,
                "print_provider_id": 99,
                "variants": [{"id": 101, "is_enabled": True}],
                "print_areas": [{"placeholders": [{"position": "front"}]}],
            }

        def update_product(self, shop_id, product_id, payload):
            raise NonRetryableRequestError(
                "HTTP 400 for PUT /shops/1/products/existing-1.json: {'code': 8251, "
                "'message': 'Variants do not match selected blueprint and print provider.'}"
            )

        def delete_product(self, shop_id, product_id):
            return {"deleted": True}

        def create_product(self, shop_id, payload):
            return {"id": "recreated-8251"}

    result = upsert_in_printify(
        printify=DummyPrintify8251Fallback(),
        shop_id=1,
        artwork=artwork,
        template=template,
        variant_rows=[{"id": 101, "is_available": True, "price": 1200, "options": {"color": "White", "size": "M"}}],
        upload_map={"front": {"id": "upload-1"}},
        existing_product_id="existing-1",
        action="update",
        publish_mode="skip",
        verify_publish=False,
    )
    assert result["action"] == "rebuild"
    assert result["previous_product_id"] == "existing-1"
    assert result["printify_product_id"] == "recreated-8251"


def test_upsert_in_printify_skips_noop_rerun_when_fingerprint_unchanged(tmp_path: Path):
    artwork = _create_artwork(tmp_path, 3000, 3000)
    template = ProductTemplate(
        key="tee-test",
        printify_blueprint_id=9,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[PlacementRequirement("front", 1000, 1000)],
    )

    class DummyPrintifyNoopRerun:
        dry_run = False

        def __init__(self):
            self.get_calls = 0
            self.update_calls = 0

        def get_product(self, shop_id, product_id):
            self.get_calls += 1
            return {
                "id": product_id,
                "blueprint_id": 9,
                "print_provider_id": 99,
                "variants": [{"id": 101, "is_enabled": True}],
                "print_areas": [{"placeholders": [{"position": "front"}]}],
            }

        def update_product(self, shop_id, product_id, payload):
            self.update_calls += 1
            return {"id": product_id}

    dummy = DummyPrintifyNoopRerun()
    first_result = upsert_in_printify(
        printify=dummy,
        shop_id=1,
        artwork=artwork,
        template=template,
        variant_rows=[{"id": 101, "is_available": True, "price": 1200, "options": {"color": "White", "size": "M"}}],
        upload_map={"front": {"id": "upload-1"}},
        existing_product_id="existing-1",
        action="update",
        publish_mode="skip",
        verify_publish=False,
    )
    assert first_result["action"] == "update"
    second_result = upsert_in_printify(
        printify=dummy,
        shop_id=1,
        artwork=artwork,
        template=template,
        variant_rows=[{"id": 101, "is_available": True, "price": 1200, "options": {"color": "White", "size": "M"}}],
        upload_map={"front": {"id": "upload-1"}},
        existing_product_id="existing-1",
        action="update",
        publish_mode="skip",
        verify_publish=False,
        prior_state_row={"result": {"printify": first_result}},
    )
    assert second_result["action"] == "skip"
    assert second_result["reason"] == "rerun_noop_unchanged_fingerprint"
    assert dummy.update_calls == 1


def test_upsert_in_printify_updates_when_only_mutable_listing_changes(tmp_path: Path):
    artwork = _create_artwork(tmp_path, 3000, 3000)
    template = ProductTemplate(
        key="mug-test",
        printify_blueprint_id=9,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[PlacementRequirement("front", 1000, 1000)],
    )

    class DummyPrintifyMutableUpdate:
        dry_run = False

        def __init__(self):
            self.update_calls = 0

        def get_product(self, shop_id, product_id):
            return {
                "id": product_id,
                "blueprint_id": 9,
                "print_provider_id": 99,
                "variants": [{"id": 101, "is_enabled": True}],
                "print_areas": [{"placeholders": [{"position": "front"}]}],
            }

        def update_product(self, shop_id, product_id, payload):
            self.update_calls += 1
            return {"id": product_id, "status": "updated"}

    dummy = DummyPrintifyMutableUpdate()
    first_result = upsert_in_printify(
        printify=dummy,
        shop_id=1,
        artwork=artwork,
        template=template,
        variant_rows=[{"id": 101, "is_available": True, "price": 1200, "options": {"color": "White", "size": "11oz"}}],
        upload_map={"front": {"id": "upload-1"}},
        existing_product_id="existing-mug",
        action="update",
        publish_mode="skip",
        verify_publish=False,
    )
    second_result = upsert_in_printify(
        printify=dummy,
        shop_id=1,
        artwork=artwork,
        template=template,
        variant_rows=[{"id": 101, "is_available": True, "price": 1300, "options": {"color": "White", "size": "11oz"}}],
        upload_map={"front": {"id": "upload-1"}},
        existing_product_id="existing-mug",
        action="update",
        publish_mode="skip",
        verify_publish=False,
        prior_state_row={"result": {"printify": first_result}},
    )
    assert second_result["action"] == "update"
    assert dummy.update_calls == 2


def test_upsert_in_printify_retries_8252_then_updates(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    monkeypatch.setattr(pipeline, "PRINTIFY_UPDATE_DISABLED_RETRY_ATTEMPTS", 1)
    monkeypatch.setattr(pipeline, "PRINTIFY_UPDATE_DISABLED_RETRY_SLEEP_SECONDS", 0.0)

    artwork = _create_artwork(tmp_path, 3000, 3000)
    template = ProductTemplate(
        key="tee-test",
        printify_blueprint_id=9,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[PlacementRequirement("front", 1000, 1000)],
    )

    class DummyPrintify8252RetryThenUpdate:
        dry_run = False

        def __init__(self):
            self.update_calls = 0
            self.delete_calls = 0

        def get_product(self, shop_id, product_id):
            return {
                "id": product_id,
                "blueprint_id": 9,
                "print_provider_id": 99,
                "variants": [{"id": 101, "is_enabled": True}],
                "print_areas": [{"placeholders": [{"position": "front"}]}],
            }

        def update_product(self, shop_id, product_id, payload):
            self.update_calls += 1
            if self.update_calls == 1:
                raise NonRetryableRequestError(
                    "HTTP 400 for PUT /shops/1/products/existing-8252.json: {'code': 8252, "
                    "'message': 'Product is disabled for editing'}"
                )
            return {"id": product_id, "status": "updated"}

        def delete_product(self, shop_id, product_id):
            self.delete_calls += 1
            return {"deleted": True}

    dummy = DummyPrintify8252RetryThenUpdate()
    result = upsert_in_printify(
        printify=dummy,
        shop_id=1,
        artwork=artwork,
        template=template,
        variant_rows=[{"id": 101, "is_available": True, "price": 1200, "options": {"color": "White", "size": "M"}}],
        upload_map={"front": {"id": "upload-1"}},
        existing_product_id="existing-8252",
        action="update",
        publish_mode="skip",
        verify_publish=False,
    )
    assert result["action"] == "update"
    assert result["printify_product_id"] == "existing-8252"
    assert dummy.update_calls == 2
    assert dummy.delete_calls == 0


def test_upsert_in_printify_rebuilds_after_8252_retry_exhaustion(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    monkeypatch.setattr(pipeline, "PRINTIFY_UPDATE_DISABLED_RETRY_ATTEMPTS", 1)
    monkeypatch.setattr(pipeline, "PRINTIFY_UPDATE_DISABLED_RETRY_SLEEP_SECONDS", 0.0)

    artwork = _create_artwork(tmp_path, 3000, 3000)
    template = ProductTemplate(
        key="tee-test",
        printify_blueprint_id=9,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[PlacementRequirement("front", 1000, 1000)],
    )

    class DummyPrintify8252Fallback:
        dry_run = False

        def __init__(self):
            self.update_calls = 0

        def get_product(self, shop_id, product_id):
            return {
                "id": product_id,
                "blueprint_id": 9,
                "print_provider_id": 99,
                "variants": [{"id": 101, "is_enabled": True}],
                "print_areas": [{"placeholders": [{"position": "front"}]}],
            }

        def update_product(self, shop_id, product_id, payload):
            self.update_calls += 1
            raise NonRetryableRequestError(
                "HTTP 400 for PUT /shops/1/products/existing-8252.json: {'code': 8252, "
                "'message': 'Product is disabled for editing'}"
            )

        def delete_product(self, shop_id, product_id):
            return {"deleted": True}

        def create_product(self, shop_id, payload):
            return {"id": "recreated-8252"}

    dummy = DummyPrintify8252Fallback()
    result = upsert_in_printify(
        printify=dummy,
        shop_id=1,
        artwork=artwork,
        template=template,
        variant_rows=[{"id": 101, "is_available": True, "price": 1200, "options": {"color": "White", "size": "M"}}],
        upload_map={"front": {"id": "upload-1"}},
        existing_product_id="existing-8252",
        action="update",
        publish_mode="skip",
        verify_publish=False,
    )
    assert result["action"] == "rebuild"
    assert result["previous_product_id"] == "existing-8252"
    assert result["printify_product_id"] == "recreated-8252"
    assert dummy.update_calls == 2


def test_run_summary_logging_does_not_raise_format_error(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    monkeypatch.setattr(pipeline, "PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "load_json", lambda path, default: ensure_state_shape({}))
    monkeypatch.setattr(pipeline, "load_templates", lambda p: [ProductTemplate(key="t1", printify_blueprint_id=1, printify_print_provider_id=1, title_pattern="{artwork_title}", description_pattern="{artwork_title}")])
    monkeypatch.setattr(pipeline, "select_templates", lambda templates, **kwargs: templates)
    monkeypatch.setattr(pipeline, "discover_artworks", lambda d: [Artwork("a1", tmp_path / "a1.png", "a1", "", [], 1000, 1000)])
    monkeypatch.setattr(pipeline, "PrintifyClient", lambda *args, **kwargs: type("X", (), {"dry_run": True})())
    monkeypatch.setattr(pipeline, "resolve_shop_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "load_r2_config_from_env", lambda: None)
    monkeypatch.setattr(pipeline, "audit_printify_integration", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "run_catalog_cli", lambda **kwargs: False)

    def fake_process_artwork(**kwargs):
        kwargs["summary"].failures = 1
        kwargs["run_rows"].append(pipeline.RunReportRow("now", "a1.png", "a1", "t1", "failure", "create", 1, 1, "auto", "", False, False, "x"))

    monkeypatch.setattr(pipeline, "process_artwork", fake_process_artwork)
    run(tmp_path / "templates.json", image_dir=tmp_path, export_dir=tmp_path / "exp", state_path=tmp_path / "state.json", skip_audit=True)


def test_format_run_summary_includes_all_fields():
    summary = RunSummary(
        artworks_scanned=1,
        templates_processed=2,
        combinations_processed=3,
        combinations_success=2,
        combinations_failed=1,
        combinations_skipped=0,
        products_created=1,
        products_updated=1,
        products_rebuilt=0,
        products_skipped=1,
        failures=1,
        publish_attempts=1,
        publish_verified=1,
        verification_warnings=0,
    )
    rendered = format_run_summary(summary)
    assert "artworks_scanned=1" in rendered
    assert "templates_processed=2" in rendered
    assert "verification_warnings=0" in rendered
    assert "publish_queue_total_count=0" in rendered


def test_template_blueprint_type_warning_detects_mismatch():
    template = ProductTemplate(
        key="mug_11oz",
        printify_blueprint_id=68,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        product_type_label="11oz Mug",
    )
    warning = template_blueprint_type_warning(template=template, blueprint_title="Unisex Heavy Cotton Tee")
    assert warning is not None
    assert "mug" in warning.lower()


def test_template_blueprint_type_warning_allows_matching_family():
    template = ProductTemplate(
        key="mug_11oz",
        printify_blueprint_id=68,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        product_type_label="11oz Mug",
    )
    assert template_blueprint_type_warning(template=template, blueprint_title="Accent Coffee Mug") is None


def test_generate_mug_template_snippet_prefers_mug_defaults():
    variants = [
        {"id": 1, "is_available": True, "options": {"color": "White", "size": "11oz"}, "placeholders": [{"position": "front"}]},
        {"id": 2, "is_available": True, "options": {"color": "Black", "size": "11oz"}, "placeholders": [{"position": "front"}]},
    ]
    snippet = generate_mug_template_snippet(key="mug_11oz", blueprint_id=68, provider_id=1, variants=variants)
    assert snippet["printify_blueprint_id"] == 68
    assert snippet["printify_print_provider_id"] == 1
    assert snippet["enabled_colors"] == ["White"]
    assert snippet["enabled_sizes"] == ["11oz"]


def test_mug_sample_template_points_to_real_pair_and_safe_cap():
    templates = load_templates(Path("product_templates.json"))
    mug = next(t for t in templates if t.key == "mug_new")
    assert mug.printify_blueprint_id == 68
    assert mug.printify_print_provider_id == 1
    assert mug.max_enabled_variants is not None and mug.max_enabled_variants <= 24


def test_tote_sample_template_uses_valid_live_resolved_mapping():
    templates = load_templates(Path("product_templates.json"))
    tote = next(t for t in templates if t.key == "tote_basic")
    assert tote.printify_blueprint_id == 609
    assert tote.printify_print_provider_id == 74


def test_tote_template_file_stays_in_sync_with_product_templates():
    product_templates = load_templates(Path("product_templates.json"))
    tote_templates = load_templates(Path("tote_template.json"))
    tote_primary = next(t for t in product_templates if t.key == "tote_basic")
    tote_standalone = next(t for t in tote_templates if t.key == "tote_basic")
    assert tote_primary.printify_blueprint_id == tote_standalone.printify_blueprint_id == 609
    assert tote_primary.printify_print_provider_id == tote_standalone.printify_print_provider_id == 74


def test_phone_and_sticker_template_files_stay_in_sync_with_product_templates():
    product_templates = load_templates(Path("product_templates.json"))
    phone_templates = load_templates(Path("phone_case_basic_template.json"))
    sticker_templates = load_templates(Path("sticker_kisscut_template.json"))
    phone_primary = next(t for t in product_templates if t.key == "phone_case_basic")
    phone_standalone = next(t for t in phone_templates if t.key == "phone_case_basic")
    sticker_primary = next(t for t in product_templates if t.key == "sticker_kisscut")
    sticker_standalone = next(t for t in sticker_templates if t.key == "sticker_kisscut")
    assert phone_primary.printify_blueprint_id == phone_standalone.printify_blueprint_id == 421
    assert phone_primary.printify_print_provider_id == phone_standalone.printify_print_provider_id == 23
    assert phone_primary.active is True
    assert phone_standalone.active is True
    assert sticker_primary.printify_blueprint_id == sticker_standalone.printify_blueprint_id == 906
    assert sticker_primary.printify_print_provider_id == sticker_standalone.printify_print_provider_id == 36
    assert sticker_primary.max_enabled_variants == sticker_standalone.max_enabled_variants == 4


def test_unresolved_families_remain_inactive():
    templates = load_templates(Path("product_templates.json"))
    by_key = {template.key: template for template in templates}
    assert by_key["canvas_basic"].active is False
    assert by_key["blanket_basic"].active is False
    assert by_key["tote_basic"].active is False


def test_tote_front_primary_and_publish_only_primary_behavior_preserved():
    templates = load_templates(Path("product_templates.json"))
    tote = next(t for t in templates if t.key == "tote_basic")
    assert tote.preferred_primary_placement == "front"
    assert tote.active_placements == ["front"]
    assert tote.publish_only_primary_placement is True
    assert tote.placements and tote.placements[0].placement_name == "front"


def test_non_poster_family_mappings_remain_unchanged():
    templates = load_templates(Path("product_templates.json"))
    by_key = {template.key: template for template in templates}
    assert by_key["hoodie_gildan"].printify_blueprint_id == 77
    assert by_key["hoodie_gildan"].printify_print_provider_id == 99
    assert by_key["sweatshirt_gildan"].printify_blueprint_id == 49
    assert by_key["sweatshirt_gildan"].printify_print_provider_id == 99
    assert by_key["longsleeve_gildan"].printify_blueprint_id == 80
    assert by_key["longsleeve_gildan"].printify_print_provider_id == 30
    assert by_key["mug_new"].printify_blueprint_id == 68
    assert by_key["mug_new"].printify_print_provider_id == 1



def test_shirt_template_enables_allow_upscale_while_mug_stays_conservative():
    templates = load_templates(Path("product_templates.json"))
    shirt = next(t for t in templates if t.key == "hoodie_gildan")
    mug = next(t for t in templates if t.key == "mug_new")

    assert shirt.placements[0].artwork_fit_mode == "contain"
    assert shirt.placements[0].allow_upscale is True
    assert shirt.placements[0].max_upscale_factor == 6.0
    assert mug.placements[0].artwork_fit_mode == "contain"
    assert mug.placements[0].allow_upscale is False
    assert mug.placements[0].max_upscale_factor is None



def test_shirt_contain_export_upscales_but_mug_contain_export_does_not(tmp_path: Path):
    path = tmp_path / "small-art.png"
    Image.new("RGBA", (1000, 500), (255, 0, 0, 255)).save(path)
    artwork = Artwork("small-art", path, "Small Art", "", [], 1000, 500)
    options = ArtworkProcessingOptions()

    templates = load_templates(Path("product_templates.json"))
    shirt = next(t for t in templates if t.key == "hoodie_gildan")
    mug = next(t for t in templates if t.key == "mug_new")

    prepared_shirt = prepare_artwork_export(artwork, shirt, shirt.placements[0], tmp_path / "exports", options)
    prepared_mug = prepare_artwork_export(artwork, mug, mug.placements[0], tmp_path / "exports", options)

    assert prepared_shirt is not None and prepared_shirt.upscaled is True
    assert prepared_shirt.requested_upscale_factor == prepared_shirt.applied_upscale_factor
    assert prepared_shirt.applied_upscale_factor == 4.5
    assert prepared_shirt.upscale_capped is False
    assert prepared_shirt.effective_upscale_factor == prepared_shirt.applied_upscale_factor
    assert prepared_shirt.exported_canvas_size == (4500, 5400)

    assert prepared_mug is not None and prepared_mug.upscaled is False
    assert prepared_mug.effective_upscale_factor == 1.0
    assert prepared_mug.requested_upscale_factor == 1.0
    assert prepared_mug.applied_upscale_factor == 1.0
    assert prepared_mug.upscale_capped is False
    assert prepared_mug.exported_canvas_size == (2700, 1120)



def test_shirt_upscale_cap_logs_warning_and_mug_path_unchanged(tmp_path: Path, caplog):
    path = tmp_path / "tiny-art.png"
    Image.new("RGBA", (600, 300), (0, 128, 255, 255)).save(path)
    artwork = Artwork("tiny-art", path, "Tiny Art", "", [], 600, 300)

    shirt_placement = PlacementRequirement(
        "front",
        4500,
        5400,
        artwork_fit_mode="contain",
        allow_upscale=True,
        max_upscale_factor=3.0,
    )
    mug_placement = PlacementRequirement(
        "front",
        2700,
        1120,
        artwork_fit_mode="contain",
        allow_upscale=False,
    )

    caplog.set_level(logging.WARNING)
    shirt_result = resolve_artwork_for_placement(
        artwork,
        shirt_placement,
        allow_upscale=True,
        upscale_method="lanczos",
        skip_undersized=False,
        max_upscale_factor=shirt_placement.max_upscale_factor,
    )
    mug_result = resolve_artwork_for_placement(
        artwork,
        mug_placement,
        allow_upscale=False,
        upscale_method="lanczos",
        skip_undersized=False,
        max_upscale_factor=None,
    )

    assert shirt_result.requested_upscale_factor > 3.0
    assert shirt_result.applied_upscale_factor == 3.0
    assert shirt_result.upscale_capped is True
    assert "Upscale cap applied" in caplog.text

    assert mug_result.requested_upscale_factor == 1.0
    assert mug_result.applied_upscale_factor == 1.0
    assert mug_result.upscale_capped is False

def _launch_template() -> ProductTemplate:
    return ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=6,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        tags=["shirt"],
        placements=[PlacementRequirement("front", 4500, 5400)],
        base_price="24.00",
    )


def test_parse_launch_plan_csv(tmp_path: Path):
    csv_path = tmp_path / "launch.csv"
    csv_path.write_text("artwork_file,template_key,enabled\na.png,tshirt_gildan,true\n", encoding="utf-8")
    rows = parse_launch_plan_csv(csv_path)
    assert rows[0]["artwork_file"] == "a.png"
    assert rows[0]["enabled"] == "true"


def test_resolve_launch_plan_rows_validation_and_enabled(tmp_path: Path):
    image_dir = tmp_path / "images"
    image_dir.mkdir()
    Image.new("RGBA", (100, 100), (1, 2, 3, 255)).save(image_dir / "ok.png")
    csv_path = tmp_path / "launch.csv"
    csv_path.write_text(
        "artwork_file,template_key,enabled,row_id\n"
        "ok.png,tshirt_gildan,true,row-ok\n"
        "missing.png,tshirt_gildan,true,row-missing\n"
        "ok.png,tshirt_gildan,false,row-disabled\n",
        encoding="utf-8",
    )
    rows, failures = resolve_launch_plan_rows(
        launch_plan_path=csv_path,
        templates=[_launch_template()],
        image_dir=image_dir,
    )
    assert len(rows) == 1
    assert rows[0].row_id == "row-ok"
    assert len(failures) == 1
    assert failures[0].launch_plan_row_id == "row-missing"


def test_build_resolved_template_applies_overrides():
    base = _launch_template()
    resolved = build_resolved_template(base, {
        "title_override": "{artwork_title} Tee",
        "tags_override": "a,b",
        "base_price_override": "19.99",
        "publish_after_create_override": "false",
    })
    assert base.title_pattern == "{artwork_title}"
    assert resolved.title_pattern == "{artwork_title} Tee"
    assert resolved.tags == ["a", "b"]
    assert resolved.base_price == "19.99"
    assert resolved.publish_after_create is False


def test_normalize_printify_transform_default_angle_is_int():
    normalized = normalize_printify_transform(
        compute_placement_transform_for_artwork(
            PlacementRequirement("front", 2700, 1120, placement_scale=0.78, placement_x=0.5, placement_y=0.5, placement_angle=0.0),
            Artwork(slug="art", src_path=Path("art.png"), title="Art", description_html="", tags=[], image_width=100, image_height=100),
            "mug_11oz",
        )
    )
    assert normalized["angle"] == 0
    assert isinstance(normalized["angle"], int)


def test_normalize_printify_transform_coerces_float_angle_to_int():
    normalized = normalize_printify_transform(
        compute_placement_transform_for_artwork(
            PlacementRequirement("front", 2700, 1120, placement_scale=0.78, placement_x=0.5, placement_y=0.5, placement_angle=12.9),
            Artwork(slug="art", src_path=Path("art.png"), title="Art", description_html="", tags=[], image_width=100, image_height=100),
            "mug_11oz",
        )
    )
    assert normalized["angle"] == 13
    assert isinstance(normalized["angle"], int)


def test_build_printify_product_payload_uses_placement_transform():
    artwork = Artwork(
        slug="art",
        src_path=Path("art.png"),
        title="Art",
        description_html="",
        tags=[],
        image_width=100,
        image_height=100,
    )
    template = ProductTemplate(
        key="mug_11oz",
        printify_blueprint_id=68,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[PlacementRequirement("front", 2700, 1120, placement_scale=0.78, placement_x=0.5, placement_y=0.5, placement_angle=0.0)],
    )
    payload = build_printify_product_payload(
        artwork,
        template,
        variant_rows=[{"id": 1, "price": 1200}],
        upload_map={"front": {"id": "upload-1"}},
    )
    image = payload["print_areas"][0]["placeholders"][0]["images"][0]
    assert image["scale"] == 0.58
    assert image["x"] == 0.5
    assert image["y"] == 0.5
    assert image["angle"] == 0
    assert isinstance(image["angle"], int)


def test_run_uses_launch_plan_selection(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    image_dir = tmp_path / "images"
    image_dir.mkdir()
    Image.new("RGBA", (2000, 2000), (255, 0, 0, 255)).save(image_dir / "one.png")
    Image.new("RGBA", (2000, 2000), (255, 0, 0, 255)).save(image_dir / "two.png")

    templates_path = tmp_path / "templates.json"
    templates_path.write_text(json.dumps([
        {
            "key": "tshirt_gildan",
            "printify_blueprint_id": 6,
            "printify_print_provider_id": 99,
            "title_pattern": "{artwork_title}",
            "description_pattern": "{artwork_title}",
            "placements": [{"placement_name": "front", "width_px": 1000, "height_px": 1000}],
        }
    ]), encoding="utf-8")

    launch_csv = tmp_path / "launch.csv"
    launch_csv.write_text(
        "artwork_file,template_key,enabled,row_id\n"
        "one.png,tshirt_gildan,true,row-1\n"
        "two.png,tshirt_gildan,false,row-2\n",
        encoding="utf-8",
    )

    class DummyPrintifyClient:
        def __init__(self, *args, **kwargs):
            self.dry_run = True

    calls = []

    def fake_process_artwork(**kwargs):
        calls.append((kwargs["artwork"].src_path.name, kwargs.get("launch_plan_row_id")))

    monkeypatch.setattr(pipeline, "process_artwork", fake_process_artwork)
    monkeypatch.setattr(pipeline, "audit_printify_integration", lambda *a, **k: None)
    monkeypatch.setattr(pipeline, "resolve_shop_id", lambda *a, **k: 1)
    monkeypatch.setattr(pipeline, "load_r2_config_from_env", lambda: None)
    monkeypatch.setattr(pipeline, "PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "PrintifyClient", DummyPrintifyClient)
    monkeypatch.setattr(pipeline, "run_catalog_cli", lambda **kwargs: False)

    run(
        templates_path,
        dry_run=True,
        image_dir=image_dir,
        export_dir=tmp_path / "exports",
        state_path=tmp_path / "state.json",
        launch_plan_path=str(launch_csv),
    )

    assert calls == [("one.png", "row-1")]




def test_compute_placement_transform_for_shirt_uses_updated_orientation_caps():
    placement = PlacementRequirement("front", 4500, 5400, placement_scale=0.9)

    portrait = Artwork("p", Path("p.png"), "P", "", [], 1000, 1800)
    square = Artwork("s", Path("s.png"), "S", "", [], 1200, 1200)
    landscape = Artwork("l", Path("l.png"), "L", "", [], 1800, 1000)

    assert compute_placement_transform_for_artwork(placement, portrait, "tshirt_gildan").scale == 0.72
    assert compute_placement_transform_for_artwork(placement, square, "tshirt_gildan").scale == 0.80
    assert compute_placement_transform_for_artwork(placement, landscape, "tshirt_gildan").scale == 0.66


def test_compute_placement_transform_for_poster_uses_tuned_scale():
    placement = PlacementRequirement("front", 4500, 5400, placement_scale=1.0)
    landscape = Artwork("l", Path("l.png"), "L", "", [], 2200, 1200)
    portrait = Artwork("p", Path("p.png"), "P", "", [], 1200, 2200)
    assert compute_placement_transform_for_artwork(placement, landscape, "poster_basic").scale == 0.97
    assert compute_placement_transform_for_artwork(placement, portrait, "poster_basic").scale == 1.0


def test_resolve_artwork_for_placement_uses_cover_for_eligible_poster_resolution(tmp_path: Path):
    path = tmp_path / "poster-art.png"
    image = Image.new("RGBA", (1800, 1200), (0, 0, 0, 0))
    image.paste((255, 0, 0, 255), (600, 400, 1200, 800))
    image.save(path)
    artwork = Artwork("poster-art", path, "Poster Art", "", [], 1800, 1200)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")

    result = resolve_artwork_for_placement(
        artwork,
        placement,
        template_key="poster_basic",
        allow_upscale=False,
        upscale_method="lanczos",
        skip_undersized=False,
    )
    assert result.action == "covered_cropped"


def test_resolve_artwork_for_placement_falls_back_to_contain_for_undersized_poster(tmp_path: Path):
    path = tmp_path / "poster-small.png"
    Image.new("RGBA", (500, 700), (255, 0, 0, 255)).save(path)
    artwork = Artwork("poster-small", path, "Poster Small", "", [], 500, 700)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")

    result = resolve_artwork_for_placement(
        artwork,
        placement,
        template_key="poster_basic",
        allow_upscale=False,
        upscale_method="lanczos",
        skip_undersized=False,
    )

    assert result.action == "contained_padded"
    assert result.upscaled is False


def test_resolve_artwork_for_placement_applies_safe_enhancement_for_undersized_poster(tmp_path: Path):
    path = tmp_path / "poster-safe-enhance.png"
    Image.new("RGBA", (780, 900), (255, 0, 0, 255)).save(path)
    artwork = Artwork("poster-safe-enhance", path, "Poster Safe Enhance", "", [], 780, 900)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")

    result = resolve_artwork_for_placement(
        artwork,
        placement,
        template_key="poster_basic",
        allow_upscale=False,
        upscale_method="lanczos",
        skip_undersized=False,
    )

    assert result.action == "contained_padded_upscale"
    assert result.upscaled is True
    assert result.requested_upscale_factor == pytest.approx(1.111, rel=1e-3)
    assert result.applied_upscale_factor == pytest.approx(1.111, rel=1e-3)
    assert result.upscale_capped is False


def test_resolve_artwork_for_placement_skips_safe_enhancement_when_over_cap(tmp_path: Path):
    path = tmp_path / "poster-over-cap.png"
    Image.new("RGBA", (500, 700), (255, 0, 0, 255)).save(path)
    artwork = Artwork("poster-over-cap", path, "Poster Over Cap", "", [], 500, 700)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")

    result = resolve_artwork_for_placement(
        artwork,
        placement,
        template_key="poster_basic",
        allow_upscale=True,
        upscale_method="lanczos",
        skip_undersized=False,
    )

    assert result.action == "contained_padded"
    assert result.upscaled is False
    assert result.requested_upscale_factor == 1.0
    assert result.applied_upscale_factor == 1.0


def test_resolve_artwork_for_placement_non_poster_behavior_unchanged_for_contain(tmp_path: Path):
    path = tmp_path / "mug-small.png"
    Image.new("RGBA", (500, 700), (255, 0, 0, 255)).save(path)
    artwork = Artwork("mug-small", path, "Mug Small", "", [], 500, 700)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")

    result = resolve_artwork_for_placement(
        artwork,
        placement,
        template_key="mug_basic",
        allow_upscale=False,
        upscale_method="lanczos",
        skip_undersized=False,
    )

    assert result.action == "contained_padded"
    assert result.upscaled is False
    assert result.requested_upscale_factor == 1.0
    assert result.applied_upscale_factor == 1.0


def test_resolve_artwork_for_blanket_undersized_cover_raises_structured_resolution_error(tmp_path: Path):
    path = tmp_path / "blanket-small.png"
    Image.new("RGBA", (1280, 1280), (255, 0, 0, 255)).save(path)
    artwork = Artwork("blanket-small", path, "Blanket Small", "", [], 1280, 1280)
    placement = PlacementRequirement("front", 6000, 4800, artwork_fit_mode="cover")

    with pytest.raises(InsufficientArtworkResolutionError) as exc_info:
        resolve_artwork_for_placement(
            artwork,
            placement,
            template_key="blanket_basic",
            allow_upscale=False,
            upscale_method="lanczos",
            skip_undersized=False,
        )
    exc = exc_info.value
    assert exc.template_key == "blanket_basic"
    assert exc.placement_name == "front"
    assert exc.source_size == (1280, 1280)
    assert exc.required_size == (6000, 4800)
    assert classify_failure(exc) == "insufficient_artwork_resolution"


def test_resolve_artwork_for_blanket_cover_succeeds_when_source_is_large_enough(tmp_path: Path):
    path = tmp_path / "blanket-large.png"
    Image.new("RGBA", (8000, 6400), (255, 0, 0, 255)).save(path)
    artwork = Artwork("blanket-large", path, "Blanket Large", "", [], 8000, 6400)
    placement = PlacementRequirement("front", 6000, 4800, artwork_fit_mode="cover")

    result = resolve_artwork_for_placement(
        artwork,
        placement,
        template_key="blanket_basic",
        allow_upscale=False,
        upscale_method="lanczos",
        skip_undersized=False,
    )
    assert result.action == "covered_cropped"
    assert result.final_size == (6000, 4800)


def test_canvas_and_blanket_template_policies_marked_as_high_resolution():
    templates = load_templates(Path("product_templates.json"))
    by_key = {template.key: template for template in templates}
    canvas = by_key["canvas_basic"]
    blanket = by_key["blanket_basic"]
    assert canvas.high_resolution_family is True
    assert blanket.high_resolution_family is True
    assert canvas.skip_if_artwork_below_threshold is True
    assert blanket.skip_if_artwork_below_threshold is True


def test_canvas_artwork_below_threshold_is_ineligible_early(tmp_path: Path):
    art = tmp_path / "canvas-small.png"
    Image.new("RGBA", (1280, 1280), (1, 2, 3, 255)).save(art)
    artwork = Artwork("a", art, "A", "", [], 1280, 1280)
    template = ProductTemplate(
        key="canvas_basic",
        printify_blueprint_id=944,
        printify_print_provider_id=105,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        high_resolution_family=True,
        skip_if_artwork_below_threshold=True,
        min_source_width=4500,
        min_source_height=5400,
        min_effective_cover_ratio=1.0,
        placements=[PlacementRequirement("front", 4500, 5400, artwork_fit_mode="cover")],
    )
    result = evaluate_artwork_eligibility_for_template(artwork=artwork, template=template, placement=template.placements[0])
    assert result.eligible is False
    assert result.reason_code == "insufficient_artwork_resolution"
    assert result.source_size == (1280, 1280)
    assert result.required_size == (4500, 5400)
    assert result.fit_mode == "cover"


def test_blanket_artwork_below_threshold_is_ineligible_early(tmp_path: Path):
    art = tmp_path / "blanket-small.png"
    Image.new("RGBA", (1280, 1280), (1, 2, 3, 255)).save(art)
    artwork = Artwork("a", art, "A", "", [], 1280, 1280)
    template = ProductTemplate(
        key="blanket_basic",
        printify_blueprint_id=238,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        high_resolution_family=True,
        skip_if_artwork_below_threshold=True,
        min_source_width=6000,
        min_source_height=4800,
        min_effective_cover_ratio=1.0,
        placements=[PlacementRequirement("front", 6000, 4800, artwork_fit_mode="cover")],
    )
    result = evaluate_artwork_eligibility_for_template(artwork=artwork, template=template, placement=template.placements[0])
    assert result.eligible is False
    assert result.reason_code == "insufficient_artwork_resolution"
    assert result.source_size == (1280, 1280)
    assert result.required_size == (6000, 4800)
    assert result.fit_mode == "cover"


def test_large_artwork_remains_eligible_for_high_resolution_templates(tmp_path: Path):
    art = tmp_path / "large.png"
    Image.new("RGBA", (8000, 6400), (1, 2, 3, 255)).save(art)
    artwork = Artwork("a", art, "A", "", [], 8000, 6400)
    canvas = ProductTemplate(
        key="canvas_basic",
        printify_blueprint_id=944,
        printify_print_provider_id=105,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        high_resolution_family=True,
        skip_if_artwork_below_threshold=True,
        min_source_width=4500,
        min_source_height=5400,
        min_effective_cover_ratio=1.0,
        placements=[PlacementRequirement("front", 4500, 5400, artwork_fit_mode="cover")],
    )
    blanket = ProductTemplate(
        key="blanket_basic",
        printify_blueprint_id=238,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        high_resolution_family=True,
        skip_if_artwork_below_threshold=True,
        min_source_width=6000,
        min_source_height=4800,
        min_effective_cover_ratio=1.0,
        placements=[PlacementRequirement("front", 6000, 4800, artwork_fit_mode="cover")],
    )
    matrix = list_eligible_templates_for_artwork(artwork, [canvas, blanket])
    assert matrix["canvas_basic"].eligible is True
    assert matrix["blanket_basic"].eligible is True


def test_resolve_tote_template_catalog_mapping_fails_for_missing_blueprint():
    class DummyPrintify:
        def list_blueprints(self):
            return [{"id": 100, "title": "Other Product"}]

        def list_print_providers(self, blueprint_id):
            return []

        def list_variants(self, blueprint_id, provider_id):
            return []

    template = ProductTemplate(
        key="tote_basic",
        printify_blueprint_id=467,
        printify_print_provider_id=30,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[
            PlacementRequirement("front", 4500, 5400),
            PlacementRequirement("back", 4500, 5400),
        ],
    )
    with pytest.raises(TemplateValidationError, match="missing blueprint"):
        resolve_tote_template_catalog_mapping(printify=DummyPrintify(), template=template)


def test_resolve_tote_template_catalog_mapping_fallback_selects_valid_pair():
    class DummyPrintify:
        def list_blueprints(self):
            return [
                {"id": 100, "title": "Old Tote"},
                {"id": 200, "title": "Canvas Tote Bag"},
            ]

        def list_print_providers(self, blueprint_id):
            if blueprint_id == 100:
                return [{"id": 9, "title": "Deprecated Provider"}]
            if blueprint_id == 200:
                return [{"id": 5, "title": "Stable Provider"}]
            return []

        def list_variants(self, blueprint_id, provider_id):
            if blueprint_id == 200 and provider_id == 5:
                return [
                    {
                        "id": 1,
                        "is_available": True,
                        "options": {"size": "One size", "color": "Natural"},
                        "placeholders": [{"position": "front"}, {"position": "back"}],
                    }
                ]
            return []

    template = ProductTemplate(
        key="tote_basic",
        printify_blueprint_id=467,
        printify_print_provider_id=30,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        enabled_sizes=["One size"],
        placements=[
            PlacementRequirement("front", 4500, 5400),
            PlacementRequirement("back", 4500, 5400),
        ],
    )
    resolved_template, variants = resolve_tote_template_catalog_mapping(printify=DummyPrintify(), template=template)
    assert resolved_template.printify_blueprint_id == 200
    assert resolved_template.printify_print_provider_id == 5
    assert len(variants) == 1


def test_optional_trim_artwork_bounds_reduces_transparent_margin_when_enabled(tmp_path: Path):
    path = tmp_path / "margin-art.png"
    image = Image.new("RGBA", (1000, 1000), (0, 0, 0, 0))
    image.paste((255, 0, 0, 255), (300, 400, 700, 600))
    image.save(path)

    artwork = Artwork("margin-art", path, "Margin Art", "", [], 1000, 1000)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")

    result = resolve_artwork_for_placement(
        artwork,
        placement,
        allow_upscale=True,
        upscale_method="lanczos",
        skip_undersized=False,
        trim_artwork_bounds=True,
    )

    assert result.trimmed_size == (430, 230)
    assert result.resized_size == (1001, 535)
    assert result.final_size == (1000, 1000)
    assert result.image.getpixel((500, 500))[:3] == (255, 0, 0)


def test_trim_padding_guard_keeps_safe_margin_and_reports_bounds_pct(tmp_path: Path):
    path = tmp_path / "trim-guard.png"
    image = Image.new("RGBA", (1000, 1000), (0, 0, 0, 0))
    image.paste((255, 0, 0, 255), (300, 400, 700, 600))
    image.save(path)

    artwork = Artwork("trim-guard", path, "Trim Guard", "", [], 1000, 1000)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")

    result = resolve_artwork_for_placement(
        artwork,
        placement,
        allow_upscale=True,
        upscale_method="lanczos",
        skip_undersized=False,
        trim_artwork_bounds=True,
        trim_padding_pct=0.05,
    )

    assert result.trimmed_size == (500, 300)
    assert result.trim_bounds_pct == (50.0, 30.0)


def test_trim_guard_skips_trimming_when_reduction_too_small(tmp_path: Path):
    path = tmp_path / "trim-too-small.png"
    image = Image.new("RGBA", (1000, 1000), (0, 0, 0, 255))
    image.save(path)

    artwork = Artwork("trim-too-small", path, "Trim Too Small", "", [], 1000, 1000)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")

    result = resolve_artwork_for_placement(
        artwork,
        placement,
        allow_upscale=False,
        upscale_method="lanczos",
        skip_undersized=False,
        trim_artwork_bounds=True,
        trim_min_reduction_pct=0.5,
    )

    assert result.trimmed_size == (1000, 1000)
    assert result.trim_bounds_pct == (100.0, 100.0)


def test_contain_mode_unchanged_when_trimming_disabled(tmp_path: Path):
    path = tmp_path / "margin-art-disabled.png"
    image = Image.new("RGBA", (1000, 1000), (0, 0, 0, 0))
    image.paste((255, 0, 0, 255), (300, 400, 700, 600))
    image.save(path)

    artwork = Artwork("margin-art-disabled", path, "Margin Art", "", [], 1000, 1000)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")

    result = resolve_artwork_for_placement(
        artwork,
        placement,
        allow_upscale=False,
        upscale_method="lanczos",
        skip_undersized=False,
        trim_artwork_bounds=False,
    )

    assert result.trimmed_size is None
    assert result.resized_size == (1000, 1000)
    assert result.final_size == (1000, 1000)
    assert result.image.getpixel((500, 500))[:3] == (255, 0, 0)
    assert result.image.getpixel((100, 100))[3] == 0


def test_template_level_shirt_only_trim_toggle_does_not_trim_mugs(tmp_path: Path):
    path = tmp_path / "margin-art-template.png"
    image = Image.new("RGBA", (1000, 1000), (0, 0, 0, 0))
    image.paste((255, 0, 0, 255), (300, 400, 700, 600))
    image.save(path)

    artwork = Artwork("margin-art-template", path, "Margin Art", "", [], 1000, 1000)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")
    options = ArtworkProcessingOptions(allow_upscale=True)

    shirt_template = ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=6,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[placement],
        trim_artwork_bounds_for_shirts=True,
    )
    mug_template = ProductTemplate(
        key="mug_11oz",
        printify_blueprint_id=68,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[placement],
        trim_artwork_bounds_for_shirts=True,
    )

    prepared_shirt = prepare_artwork_export(artwork, shirt_template, placement, tmp_path / "exports", options)
    prepared_mug = prepare_artwork_export(artwork, mug_template, placement, tmp_path / "exports", options)

    assert prepared_shirt is not None and prepared_shirt.trimmed_size == (430, 230)
    assert prepared_mug is not None and prepared_mug.trimmed_size is None


def test_trim_logs_skip_reason_for_no_meaningful_alpha_bounds(tmp_path: Path, caplog):
    path = tmp_path / "all-transparent.png"
    Image.new("RGBA", (1000, 1000), (0, 0, 0, 0)).save(path)

    artwork = Artwork("transparent", path, "Transparent", "", [], 1000, 1000)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")

    with caplog.at_level(logging.INFO):
        result = resolve_artwork_for_placement(
            artwork,
            placement,
            allow_upscale=False,
            upscale_method="lanczos",
            skip_undersized=False,
            trim_artwork_bounds=True,
        )

    assert result.trim_applied is False
    assert result.trim_skip_reason == "no_meaningful_alpha_bounds"
    assert "reason=no_meaningful_alpha_bounds" in caplog.text


def test_trim_logs_skip_reason_for_reduction_threshold(tmp_path: Path, caplog):
    path = tmp_path / "full-alpha.png"
    Image.new("RGBA", (1000, 1000), (255, 0, 0, 255)).save(path)

    artwork = Artwork("full", path, "Full", "", [], 1000, 1000)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")

    with caplog.at_level(logging.INFO):
        result = resolve_artwork_for_placement(
            artwork,
            placement,
            allow_upscale=False,
            upscale_method="lanczos",
            skip_undersized=False,
            trim_artwork_bounds=True,
            trim_min_reduction_pct=0.5,
        )

    assert result.trim_applied is False
    assert result.trim_skip_reason == "below_reduction_threshold"
    assert "reason=below_reduction_threshold" in caplog.text


def test_template_level_shirt_only_trim_reports_non_shirt_template_reason(tmp_path: Path, caplog):
    path = tmp_path / "margin-art-template-reason.png"
    image = Image.new("RGBA", (1000, 1000), (0, 0, 0, 0))
    image.paste((255, 0, 0, 255), (300, 400, 700, 600))
    image.save(path)

    artwork = Artwork("margin-art-template-reason", path, "Margin Art", "", [], 1000, 1000)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")
    options = ArtworkProcessingOptions(allow_upscale=True)

    mug_template = ProductTemplate(
        key="mug_11oz",
        printify_blueprint_id=68,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[placement],
        trim_artwork_bounds_for_shirts=True,
    )

    with caplog.at_level(logging.INFO):
        prepared_mug = prepare_artwork_export(artwork, mug_template, placement, tmp_path / "exports", options)

    assert prepared_mug is not None
    assert prepared_mug.trimmed_size is None
    assert prepared_mug.trim_skip_reason == "non_shirt_template"
    assert "reason=non_shirt_template" in caplog.text




def test_aggressive_subject_trim_makes_small_central_subject_visibly_larger(tmp_path: Path):
    path = tmp_path / "cat-style-small-subject.png"
    image = Image.new("RGBA", (1000, 1000), (0, 0, 0, 0))
    image.paste((255, 80, 80, 255), (420, 420, 580, 580))
    image.putpixel((980, 980), (255, 255, 255, 255))
    image.save(path)

    artwork = Artwork("cat-style", path, "Cat Style", "", [], 1000, 1000)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")

    baseline = resolve_artwork_for_placement(
        artwork,
        placement,
        allow_upscale=True,
        upscale_method="lanczos",
        skip_undersized=False,
        trim_artwork_bounds=True,
        trim_min_alpha=1,
    )
    aggressive = resolve_artwork_for_placement(
        artwork,
        placement,
        allow_upscale=True,
        upscale_method="lanczos",
        skip_undersized=False,
        trim_artwork_bounds=True,
        trim_min_alpha=1,
        aggressive_subject_trim_mode="clipart_central",
        subject_fill_target=0.9,
    )

    assert aggressive.resized_size[0] > baseline.resized_size[0]
    assert aggressive.resized_size[1] > baseline.resized_size[1]
    assert aggressive.aggressive_trim_used is True
    assert aggressive.subject_fill_target == 0.9
    assert aggressive.subject_bounds_before_aggressive_trim is not None
    assert aggressive.subject_bounds_after_aggressive_trim is not None


def test_aggressive_shirt_trim_flags_do_not_change_mug_behavior(tmp_path: Path):
    path = tmp_path / "mug-unchanged.png"
    image = Image.new("RGBA", (1000, 1000), (0, 0, 0, 0))
    image.paste((255, 0, 0, 255), (350, 350, 650, 650))
    image.save(path)

    artwork = Artwork("mug-unchanged", path, "Mug Unchanged", "", [], 1000, 1000)
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")
    options = ArtworkProcessingOptions(allow_upscale=True)

    mug_template = ProductTemplate(
        key="mug_11oz",
        printify_blueprint_id=68,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[placement],
        trim_artwork_bounds_for_shirts=True,
        aggressive_subject_trim_for_shirts=True,
        aggressive_subject_trim_mode="clipart_central",
        shirt_subject_fill_target=0.9,
    )

    prepared_mug = prepare_artwork_export(artwork, mug_template, placement, tmp_path / "exports", options)
    assert prepared_mug is not None
    assert prepared_mug.trimmed_size is None
    assert prepared_mug.aggressive_trim_used is False
    assert prepared_mug.subject_bounds_before_aggressive_trim is None


def test_aggressive_trim_still_respects_max_upscale_factor_for_large_art(tmp_path: Path):
    path = tmp_path / "large-art-safe.png"
    image = Image.new("RGBA", (1600, 1600), (0, 0, 0, 0))
    image.paste((255, 0, 0, 255), (500, 500, 1100, 1100))
    image.putpixel((30, 30), (255, 255, 255, 255))
    image.save(path)

    artwork = Artwork("large-safe", path, "Large Safe", "", [], 1600, 1600)
    placement = PlacementRequirement("front", 2200, 2200, artwork_fit_mode="contain")

    result = resolve_artwork_for_placement(
        artwork,
        placement,
        allow_upscale=True,
        upscale_method="lanczos",
        skip_undersized=False,
        trim_artwork_bounds=True,
        trim_min_alpha=1,
        max_upscale_factor=1.1,
        aggressive_subject_trim_mode="clipart_central",
        subject_fill_target=0.85,
    )

    assert result.upscale_capped is True
    assert result.applied_upscale_factor == pytest.approx(1.1)
def test_trim_preset_resolution_uses_preset_defaults_and_numeric_overrides():
    template = ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=6,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[PlacementRequirement("front", 1000, 1000)],
        trim_bounds_preset="aggressive",
    )
    assert _resolve_trim_bounds_settings(template) == (1, 0.005, 0.002)

    template_override = ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=6,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[PlacementRequirement("front", 1000, 1000)],
        trim_bounds_preset="aggressive",
        trim_bounds_min_alpha=12,
    )
    assert _resolve_trim_bounds_settings(template_override) == (12, 0.005, 0.002)

def test_compute_placement_transform_for_mug_landscape_caps_scale():
    artwork = Artwork(
        slug="wide",
        src_path=Path("wide.png"),
        title="Wide",
        description_html="",
        tags=[],
        image_width=2200,
        image_height=1000,
    )
    placement = PlacementRequirement("front", 2700, 1120, placement_scale=0.78, placement_x=0.4, placement_y=0.6, placement_angle=0.0)
    transform = compute_placement_transform_for_artwork(placement, artwork, "mug_11oz")
    assert transform.scale == 0.54
    assert transform.x == 0.4
    assert transform.y == 0.6


def test_export_launch_plan_from_images_uses_real_files(tmp_path: Path):
    image_dir = tmp_path / "images"
    image_dir.mkdir()
    Image.new("RGBA", (100, 100), (1, 2, 3, 255)).save(image_dir / "one.png")
    Image.new("RGBA", (100, 100), (1, 2, 3, 255)).save(image_dir / "two.png")

    out_csv = tmp_path / "launch.csv"
    rows = export_launch_plan_from_images(
        path=out_csv,
        image_dir=image_dir,
        templates=[_launch_template()],
        include_disabled_template_rows=False,
        default_enabled=True,
    )
    parsed = parse_launch_plan_csv(out_csv)
    assert rows == 2
    assert {row["artwork_file"] for row in parsed} == {"one.png", "two.png"}
    assert {row["template_key"] for row in parsed} == {"tshirt_gildan"}
    assert all(row["enabled"] == "true" for row in parsed)


def test_export_launch_plan_from_images_can_include_disabled_rows(tmp_path: Path):
    image_dir = tmp_path / "images"
    image_dir.mkdir()
    Image.new("RGBA", (100, 100), (1, 2, 3, 255)).save(image_dir / "one.png")

    out_csv = tmp_path / "launch.csv"
    rows = export_launch_plan_from_images(
        path=out_csv,
        image_dir=image_dir,
        templates=[_launch_template()],
        include_disabled_template_rows=True,
        default_enabled=False,
    )
    parsed = parse_launch_plan_csv(out_csv)
    assert rows == 2
    assert [row["enabled"] for row in parsed] == ["false", "false"]


def test_contain_mode_preserves_full_image_bounds_with_padding(tmp_path: Path):
    path = tmp_path / "wide.png"
    image = Image.new("RGBA", (1000, 500), (0, 0, 0, 0))
    image.paste((255, 0, 0, 255), (0, 0, 1000, 500))
    image.save(path)
    artwork = Artwork(
        slug="wide",
        src_path=path,
        title="Wide",
        description_html="<p>Wide</p>",
        tags=[],
        image_width=1000,
        image_height=500,
    )
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="contain")

    result = resolve_artwork_for_placement(
        artwork,
        placement,
        allow_upscale=False,
        upscale_method="lanczos",
        skip_undersized=False,
    )

    assert result.action == "contained_padded"
    assert result.final_size == (1000, 1000)
    assert result.resized_size == (1000, 500)
    top_pixel = result.image.getpixel((500, 100))
    center_pixel = result.image.getpixel((500, 500))
    assert top_pixel[3] == 0
    assert center_pixel[:3] == (255, 0, 0)


def test_cover_mode_crops_to_fill(tmp_path: Path):
    path = tmp_path / "wide-cover.png"
    image = Image.new("RGBA", (1000, 500), (0, 0, 0, 0))
    image.paste((0, 255, 0, 255), (0, 0, 1000, 250))
    image.paste((0, 0, 255, 255), (0, 250, 1000, 500))
    image.save(path)
    artwork = Artwork(
        slug="wide-cover",
        src_path=path,
        title="Wide Cover",
        description_html="<p>Wide Cover</p>",
        tags=[],
        image_width=1000,
        image_height=500,
    )
    placement = PlacementRequirement("front", 1000, 1000, artwork_fit_mode="cover")

    result = resolve_artwork_for_placement(
        artwork,
        placement,
        allow_upscale=True,
        upscale_method="lanczos",
        skip_undersized=False,
    )

    assert result.action == "covered_cropped_upscale"
    assert result.final_size == (1000, 1000)
    assert result.resized_size == (2000, 1000)
    assert result.image.getpixel((500, 10))[:3] == (0, 255, 0)
    assert result.image.getpixel((500, 990))[:3] == (0, 0, 255)


def test_contain_mode_preserves_aspect_ratio_when_upscaled(tmp_path: Path):
    path = tmp_path / "small.png"
    Image.new("RGBA", (300, 150), (255, 128, 0, 255)).save(path)
    artwork = Artwork(
        slug="small",
        src_path=path,
        title="Small",
        description_html="<p>Small</p>",
        tags=[],
        image_width=300,
        image_height=150,
    )
    placement = PlacementRequirement("front", 900, 900, artwork_fit_mode="contain")

    result = resolve_artwork_for_placement(
        artwork,
        placement,
        allow_upscale=True,
        upscale_method="nearest",
        skip_undersized=False,
    )

    assert result.action == "contained_padded_upscale"
    assert result.upscaled is True
    assert result.final_size == (900, 900)
    assert result.resized_size == (900, 450)


def test_template_validation_rejects_invalid_artwork_fit_mode(tmp_path: Path):
    config = tmp_path / "product_templates.json"
    config.write_text(
        json.dumps(
            [
                {
                    "key": "t1",
                    "printify_blueprint_id": 6,
                    "printify_print_provider_id": 99,
                    "placements": [
                        {
                            "placement_name": "front",
                            "width_px": 1000,
                            "height_px": 1000,
                            "artwork_fit_mode": "stretch",
                        }
                    ],
                }
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(TemplateValidationError, match=r"artwork_fit_mode must be contain\|cover"):
        load_templates(config)


def test_template_validation_rejects_invalid_trim_thresholds(tmp_path: Path):
    config = tmp_path / "product_templates.json"
    config.write_text(
        json.dumps(
            [
                {
                    "key": "t1",
                    "printify_blueprint_id": 6,
                    "printify_print_provider_id": 99,
                    "trim_bounds_min_alpha": 300,
                    "placements": [{"placement_name": "front", "width_px": 1000, "height_px": 1000}],
                }
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(TemplateValidationError, match=r"trim_bounds_min_alpha must be between 0 and 255"):
        load_templates(config)

def test_template_validation_rejects_invalid_trim_preset(tmp_path: Path):
    config = tmp_path / "product_templates.json"
    config.write_text(
        json.dumps(
            [
                {
                    "key": "t1",
                    "printify_blueprint_id": 6,
                    "printify_print_provider_id": 99,
                    "trim_bounds_preset": "wild",
                    "placements": [{"placement_name": "front", "width_px": 1000, "height_px": 1000}],
                }
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(TemplateValidationError, match=r"trim_bounds_preset must be one of"):
        load_templates(config)


def test_template_validation_rejects_invalid_max_upscale_factor(tmp_path: Path):
    config = tmp_path / "product_templates.json"
    config.write_text(
        json.dumps(
            [
                {
                    "key": "t1",
                    "printify_blueprint_id": 6,
                    "printify_print_provider_id": 99,
                    "max_upscale_factor": 0,
                    "placements": [{"placement_name": "front", "width_px": 1000, "height_px": 1000}],
                }
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(TemplateValidationError, match=r"max_upscale_factor must be > 0"):
        load_templates(config)


def test_filename_slug_to_title_handles_ugly_flat_tokens():
    assert filename_slug_to_title("flat,750x,075,f-pad,750x1000,f8f8f8") == "Untitled Design"


def test_resolve_launch_plan_rows_parses_collection_metadata(tmp_path: Path):
    image_dir = tmp_path / "images"
    image_dir.mkdir()
    Image.new("RGBA", (100, 100), (255, 0, 0, 255)).save(image_dir / "ok.png")
    csv_path = tmp_path / "launch.csv"
    csv_path.write_text(
        "artwork_file,template_key,enabled,row_id,collection_handle,collection_title,collection_description,launch_name,campaign,merch_theme\n"
        "ok.png,tshirt_gildan,true,row-ok,animals,Animals,Animal art,spring-launch,spring,playful\n",
        encoding="utf-8",
    )
    rows, failures = resolve_launch_plan_rows(
        launch_plan_path=csv_path,
        templates=[_launch_template()],
        image_dir=image_dir,
    )
    assert not failures
    assert rows[0].collection_handle == "animals"
    assert rows[0].campaign == "spring"


def test_prepare_artwork_export_preview_smoke(tmp_path: Path):
    art = _create_artwork(tmp_path, 1200, 1200)
    template = ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[PlacementRequirement("front", 1000, 1000)],
    )
    placement = template.placements[0]
    prepared = prepare_artwork_export(
        art,
        template,
        placement,
        tmp_path / "exports",
        ArtworkProcessingOptions(placement_preview=True, preview_dir=tmp_path / "exports" / "previews"),
    )
    assert prepared is not None
    previews = list((tmp_path / "exports" / "previews").glob("*.png"))
    assert previews


def test_process_artwork_success_run_row_includes_launch_plan_metadata(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    class DummyPrintify:
        dry_run = False

        def list_variants(self, blueprint_id, provider_id):
            return [{"id": 1, "is_available": True, "options": {}, "price": 1200}]

    monkeypatch.setattr(pipeline, "choose_variants_from_catalog", lambda variants, template: variants)
    monkeypatch.setattr(
        pipeline,
        "prepare_artwork_export",
        lambda artwork, template, placement, export_dir, options: PreparedArtwork(
            artwork=artwork,
            template=template,
            placement=placement,
            export_path=tmp_path / "x.png",
            width_px=placement.width_px,
            height_px=placement.height_px,
        ),
    )
    monkeypatch.setattr(pipeline, "upload_assets_to_printify", lambda *a, **k: {"front": {"id": "up-1"}})
    monkeypatch.setattr(
        pipeline,
        "upsert_in_printify",
        lambda **kwargs: {
            "status": "ok",
            "action": "create",
            "printify_product_id": "p1",
            "publish_attempted": True,
            "publish_verified": True,
            "verification": {"ok": True, "verified_title": "X", "verified_variant_count": 1},
        },
    )

    art = _create_artwork(tmp_path, 1200, 1200)
    tpl = ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[PlacementRequirement("front", 1000, 1000)],
    )
    state = ensure_state_shape({})
    run_rows = []

    pipeline.process_artwork(
        printify=DummyPrintify(),
        shopify=None,
        shop_id=1,
        artwork=art,
        templates=[tpl],
        state=state,
        force=False,
        export_dir=tmp_path / "exports",
        state_path=tmp_path / "state.json",
        artwork_options=ArtworkProcessingOptions(),
        upload_strategy="auto",
        r2_config=None,
        run_rows=run_rows,
        launch_plan_row="2",
        launch_plan_row_id="row-2",
        collection_handle="animals",
        campaign="spring",
    )

    assert run_rows[0].launch_plan_row == "2"
    assert run_rows[0].launch_plan_row_id == "row-2"
    assert run_rows[0].collection_handle == "animals"
    assert run_rows[0].campaign == "spring"


def test_extract_numeric_shopify_id_supports_gid_and_plain_id():
    assert _extract_numeric_shopify_id("gid://shopify/Product/12345") == 12345
    assert _extract_numeric_shopify_id("12345") == 12345
    assert _extract_numeric_shopify_id("bad") is None


def test_sync_shopify_collection_creates_then_reuses_collection_and_membership():
    class DummyShopify:
        def __init__(self):
            self.collections = {}
            self.collects = set()
            self.next_id = 100

        def find_custom_collection(self, *, handle="", title=""):
            for row in self.collections.values():
                if handle and row["handle"] == handle:
                    return dict(row)
                if title and row["title"] == title:
                    return dict(row)
            return None

        def create_custom_collection(self, *, handle, title, description=""):
            row = {"id": self.next_id, "handle": handle, "title": title, "body_html": description}
            self.collections[row["id"]] = row
            self.next_id += 1
            return dict(row)

        def update_custom_collection(self, *, collection_id, title, description=""):
            row = self.collections[collection_id]
            row["title"] = title
            row["body_html"] = description
            return dict(row)

        def is_product_in_collection(self, *, collection_id, product_id):
            return (collection_id, product_id) in self.collects

        def add_product_to_collection(self, *, collection_id, product_id):
            self.collects.add((collection_id, product_id))
            return {"id": 1}

    shopify = DummyShopify()
    first = sync_shopify_collection(
        shopify=shopify,
        shopify_product_id="gid://shopify/Product/200",
        collection_handle="animals",
        collection_title="Animals",
        collection_description="Animal art",
        verify_membership=True,
    )
    second = sync_shopify_collection(
        shopify=shopify,
        shopify_product_id="gid://shopify/Product/200",
        collection_handle="animals",
        collection_title="Animals",
        collection_description="Animal art",
        verify_membership=True,
    )

    assert first["collection_sync_status"] == "created"
    assert second["collection_sync_status"] == "synced"
    assert len(shopify.collections) == 1
    assert len(shopify.collects) == 1
    assert second["collection_membership_verified"] is True


def test_sync_shopify_collection_skips_without_shopify_product_id():
    result = sync_shopify_collection(
        shopify=None,
        shopify_product_id="",
        collection_handle="animals",
        collection_title="Animals",
        collection_description="",
        verify_membership=False,
    )
    assert result["collection_sync_status"] == "skipped_no_shopify_client"


def test_sync_shopify_collection_dry_run_returns_non_mutating_status():
    class DryRunShopify:
        def find_custom_collection(self, *, handle="", title=""):
            return None

        def create_custom_collection(self, *, handle, title, description=""):
            raise DryRunMutationSkipped("dry-run skipped")

    result = sync_shopify_collection(
        shopify=DryRunShopify(),
        shopify_product_id="gid://shopify/Product/200",
        collection_handle="animals",
        collection_title="Animals",
        collection_description="",
        verify_membership=False,
    )
    assert result["collection_sync_status"] == "dry-run"


def test_family_collection_mapping_routes_active_families_correctly():
    templates = load_templates(Path("product_templates.json"))
    actual = {template.key: resolve_family_collection_target(template).get("handle", "") for template in templates}
    assert actual["tshirt_gildan"] == "t-shirts"
    assert actual["longsleeve_gildan"] == "long-sleeve-shirts"
    assert actual["hoodie_gildan"] == "hoodies"
    assert actual["sweatshirt_gildan"] == "sweatshirts"
    assert actual["mug_new"] == "mugs"
    assert actual["poster_basic"] == "posters"
    assert actual["tote_basic"] == "tote-bags"


def test_preferred_mockup_color_selection_prefers_non_white_when_available():
    template = ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=6,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="<p>{artwork_title}</p>",
        placements=[PlacementRequirement("front", 100, 100)],
        preferred_mockup_colors=["Black", "Dark Heather", "White"],
    )
    variants = [
        {"options": {"color": "White"}},
        {"options": {"color": "Black"}},
    ]
    assert choose_preferred_featured_variant_color(template=template, variant_rows=variants) == "Black"


def test_preferred_mockup_color_selection_falls_back_when_preferred_missing():
    template = ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=6,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="<p>{artwork_title}</p>",
        placements=[PlacementRequirement("front", 100, 100)],
        preferred_mockup_colors=["Black"],
    )
    variants = [{"options": {"color": "White"}}]
    assert choose_preferred_featured_variant_color(template=template, variant_rows=variants) == "White"


def test_preferred_featured_mockup_candidate_uses_preferred_color_and_type_when_available():
    template = ProductTemplate(
        key="hoodie_gildan",
        printify_blueprint_id=77,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="<p>{artwork_title}</p>",
        placements=[PlacementRequirement("front", 100, 100)],
        preferred_mockup_colors=["Dark Heather", "White"],
        preferred_default_variant_color="Dark Heather",
        preferred_mockup_types=["lifestyle", "flat"],
    )
    variants = [
        {"id": 101, "options": {"color": "White"}},
        {"id": 202, "options": {"color": "Dark Heather"}},
    ]
    candidate = choose_preferred_featured_mockup_candidate(
        template=template,
        variant_rows=variants,
        product_images=[
            {"src": "https://example.com/white-flat.png", "type": "flat", "variant_ids": [101]},
            {"src": "https://example.com/heather-life.png", "type": "lifestyle", "variant_ids": [202]},
        ],
    )
    assert candidate["selected_featured_mockup_color"] == "Dark Heather"
    assert candidate["selected_featured_mockup_type"] == "lifestyle"


def test_preferred_featured_mockup_candidate_falls_back_when_preferred_color_missing():
    template = ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=6,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="<p>{artwork_title}</p>",
        placements=[PlacementRequirement("front", 100, 100)],
        preferred_mockup_colors=["Black"],
        preferred_mockup_types=["lifestyle", "flat"],
    )
    variants = [{"id": 101, "options": {"color": "White"}}]
    candidate = choose_preferred_featured_mockup_candidate(
        template=template,
        variant_rows=variants,
        product_images=[
            {"src": "https://example.com/white-flat.png", "type": "flat", "variant_ids": [101]},
        ],
    )
    assert candidate["selected_featured_mockup_color"] == "White"
    assert candidate["selected_featured_mockup_type"] == "flat"


def test_tote_orientation_tuning_is_conservative_and_front_safe(tmp_path: Path):
    artwork_portrait = _create_artwork(tmp_path, 1200, 1800)
    artwork_square = _create_artwork(tmp_path, 1400, 1400)
    placement = PlacementRequirement("front", 1000, 1000, placement_scale=0.78)
    portrait = compute_placement_transform_for_artwork(placement, artwork_portrait, "tote_basic")
    square = compute_placement_transform_for_artwork(placement, artwork_square, "tote_basic")
    assert portrait.scale == pytest.approx(0.84)
    assert square.scale == pytest.approx(0.82)
    assert portrait.scale <= 1.0 and square.scale <= 1.0


def test_template_blueprint_type_warning_does_not_false_positive_hoodie_with_shirt_word():
    template = ProductTemplate(
        key="hoodie_gildan",
        printify_blueprint_id=77,
        printify_print_provider_id=99,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        product_type_label="Hoodie",
    )
    warning = template_blueprint_type_warning(template=template, blueprint_title="Unisex Hoodie Shirt")
    assert warning is None


def test_readme_default_recommended_command_omits_collection_sync_flags():
    readme = Path("README.md").read_text()
    marker = "Run a 3-image all-family batch (recommended default path):"
    assert marker in readme
    section = readme.split(marker, 1)[1].split("Current supported template families include:", 1)[0]
    assert "--local-image-batch 3" in section
    assert "--publish" in section
    assert "--verify-publish" in section
    assert "--sync-collections" not in section
    assert "--verify-collections" not in section
    assert "--enforce-family-collection-membership" not in section


def test_process_artwork_collection_sync_fields_in_run_report(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    class DummyPrintify:
        dry_run = False

        def list_variants(self, blueprint_id, provider_id):
            return [{"id": 1, "is_available": True, "options": {}, "price": 1200}]

    monkeypatch.setattr(pipeline, "choose_variants_from_catalog", lambda variants, template: variants)
    monkeypatch.setattr(
        pipeline,
        "prepare_artwork_export",
        lambda artwork, template, placement, export_dir, options: PreparedArtwork(
            artwork=artwork,
            template=template,
            placement=placement,
            export_path=tmp_path / "x.png",
            width_px=placement.width_px,
            height_px=placement.height_px,
        ),
    )
    monkeypatch.setattr(pipeline, "upload_assets_to_printify", lambda *a, **k: {"front": {"id": "up-1"}})
    monkeypatch.setattr(
        pipeline,
        "upsert_in_printify",
        lambda **kwargs: {"status": "ok", "action": "create", "printify_product_id": "p1"},
    )
    monkeypatch.setattr(
        pipeline,
        "create_in_shopify_only",
        lambda shopify, artwork, template, variant_rows: {"shopify_product_id": "gid://shopify/Product/200"},
    )
    monkeypatch.setattr(
        pipeline,
        "sync_shopify_collection",
        lambda **kwargs: {
            "collection_sync_attempted": True,
            "collection_sync_status": "synced",
            "collection_id": "300",
            "collection_handle": "animals",
            "collection_title": "Animals",
            "collection_membership_verified": True,
            "collection_warning": "",
            "collection_error": "",
        },
    )

    art = _create_artwork(tmp_path, 1200, 1200)
    tpl = ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[PlacementRequirement("front", 1000, 1000)],
        publish_to_shopify=True,
    )
    state = ensure_state_shape({})
    run_rows = []
    pipeline.process_artwork(
        printify=DummyPrintify(),
        shopify=object(),
        shop_id=1,
        artwork=art,
        templates=[tpl],
        state=state,
        force=False,
        export_dir=tmp_path / "exports",
        state_path=tmp_path / "state.json",
        artwork_options=ArtworkProcessingOptions(),
        upload_strategy="auto",
        r2_config=None,
        run_rows=run_rows,
        sync_collections=True,
        collection_handle="animals",
        collection_title="Animals",
    )
    assert run_rows[0].collection_sync_attempted is True
    assert run_rows[0].collection_sync_status == "synced"
    assert run_rows[0].shopify_collection_id == "300"


def _qa_template() -> ProductTemplate:
    return ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=6,
        printify_print_provider_id=99,
        title_pattern="{artwork_title} T-Shirt",
        description_pattern="<p>{generated_description}</p>",
        enabled_colors=["Black"],
        enabled_sizes=["M"],
        placements=[PlacementRequirement("front", 1000, 1000)],
    )


def _qa_artwork(tmp_path: Path, *, title: str = "Aurora Bloom", metadata: dict = None) -> Artwork:
    image_path = tmp_path / "aurora-bloom.png"
    Image.new("RGBA", (1400, 1400), (10, 10, 10, 255)).save(image_path)
    return Artwork(
        slug="aurora-bloom",
        src_path=image_path,
        title=title,
        description_html="<p>Aurora Bloom</p>",
        tags=["aurora", "bloom"],
        image_width=1400,
        image_height=1400,
        metadata=metadata or {"title": title, "description": "A rich floral scene", "tags": "aurora,bloom,floral"},
    )


def test_storefront_title_qa_flags_placeholders_and_bad_fallback(tmp_path: Path):
    template = _qa_template()
    artwork = _qa_artwork(tmp_path)
    warnings, errors = validate_storefront_title(
        title="{artwork_title}",
        title_source="filename_slug",
        title_quality="hash_like",
        artwork=artwork,
        template=template,
    )
    assert "title_unresolved_placeholder" in errors
    assert "title_low_quality_source" in warnings

    ok_warnings, ok_errors = validate_storefront_title(
        title="Aurora Bloom T-Shirt",
        title_source="metadata",
        title_quality="metadata_title",
        artwork=artwork,
        template=template,
    )
    assert not ok_errors
    assert "title_unresolved_placeholder" not in ok_warnings


def test_storefront_description_qa_flags_empty_and_short(tmp_path: Path):
    template = _qa_template()
    artwork = _qa_artwork(tmp_path)

    warnings, errors = validate_storefront_description(description_html="", template=template, artwork=artwork)
    assert "description_empty" in errors

    warnings2, errors2 = validate_storefront_description(description_html="<p>Hi</p>", template=template, artwork=artwork)
    assert "description_suspiciously_short" in warnings2
    assert not errors2


def test_storefront_tag_qa_detects_duplicates_and_generic_only(tmp_path: Path):
    template = _qa_template()
    artwork = _qa_artwork(tmp_path)
    warnings, errors = validate_storefront_tags(
        tags=["printify", "printify", "inkvibe", "print-on-demand"],
        template=template,
        artwork=artwork,
    )
    assert not errors
    assert "tags_contain_duplicates" in warnings
    assert "tags_generic_only" in warnings


def test_storefront_pricing_qa_compare_at_and_summary():
    template = _qa_template()
    template.compare_at_price = "bad-price"
    variants = [{"id": 1, "is_available": True, "options": {"color": "Black", "size": "M"}, "price": 1200}]
    warnings, errors, summary = validate_storefront_pricing(template=template, variant_rows=variants)
    assert "pricing_invalid_compare_at_value" in errors
    assert summary["sale_min"] == summary["sale_max"]
    assert not warnings


def test_storefront_options_qa_color_size_and_default_title():
    template = _qa_template()
    variant_rows = [{"id": 1, "is_available": True, "options": {"color": "Black", "size": "M"}, "price": 1200}]
    product_options, variant_payloads = build_shopify_product_options(template, variant_rows)
    warnings, errors, names = validate_storefront_options(
        template=template,
        variant_rows=variant_rows,
        product_options=product_options,
        variant_payloads=variant_payloads,
    )
    assert not errors
    assert "Color" in names and "Size" in names
    assert "options_default_title_with_real_dimensions" not in warnings


def test_storefront_mockup_qa_captures_publish_flags():
    template = _qa_template()
    template.publish_images = False
    template.publish_mockups = True
    warnings, errors = validate_storefront_mockups(
        template=template,
        publish_payload={"images": template.publish_mockups},
        placement_context="front:mode=contain",
    )
    assert not errors
    assert "mockups_publish_mockups_override_set" in warnings
    assert "mockups_channel_provider_dependent_selection" in warnings


def test_run_storefront_qa_non_mutating_and_exports(tmp_path: Path):
    class DummyPrintify:
        dry_run = False

        def list_variants(self, blueprint_id, provider_id):
            return [{"id": 1, "is_available": True, "options": {"color": "Black", "size": "M"}, "price": 1200}]

    artwork = _qa_artwork(tmp_path)
    template = _qa_template()
    csv_path = tmp_path / "storefront_qa.csv"
    json_path = tmp_path / "storefront_qa.json"
    rows = run_storefront_qa(
        printify=DummyPrintify(),
        artworks=[artwork],
        templates=[template],
        export_csv_path=str(csv_path),
        export_json_path=str(json_path),
    )
    assert len(rows) == 1
    assert csv_path.exists()
    assert json_path.exists()
    exported = csv_path.read_text(encoding="utf-8")
    assert "artwork_filename" in exported
    assert "qa_status" in exported


def test_run_storefront_qa_cli_path_does_not_mutate(monkeypatch, tmp_path: Path):
    import printify_shopify_sync_pipeline as pipeline

    img = tmp_path / "images"
    img.mkdir()
    Image.new("RGBA", (1200, 1200), (0, 0, 0, 255)).save(img / "a.png")
    cfg = tmp_path / "templates.json"
    cfg.write_text(
        json.dumps(
            [
                {
                    "key": "tshirt_gildan",
                    "printify_blueprint_id": 6,
                    "printify_print_provider_id": 99,
                    "title_pattern": "{artwork_title}",
                    "description_pattern": "{generated_description}",
                    "placements": [{"placement_name": "front", "width_px": 1000, "height_px": 1000}],
                }
            ]
        ),
        encoding="utf-8",
    )
    state_path = tmp_path / "state.json"
    before = json.dumps(ensure_state_shape({}), sort_keys=True)
    state_path.write_text(before, encoding="utf-8")

    class DummyPrintify:
        dry_run = True

        def __init__(self, token, dry_run=False):
            self.dry_run = dry_run

        def list_variants(self, blueprint_id, provider_id):
            return [{"id": 1, "is_available": True, "options": {}, "price": 1200}]

    monkeypatch.setenv("PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "PrintifyClient", DummyPrintify)
    monkeypatch.setattr(pipeline, "resolve_shop_id", lambda *a, **k: 1)
    monkeypatch.setattr(pipeline, "run_catalog_cli", lambda **kwargs: False)
    monkeypatch.setattr(pipeline, "audit_printify_integration", lambda *a, **k: None)
    called = {"process": 0}
    monkeypatch.setattr(pipeline, "process_artwork", lambda **kwargs: called.__setitem__("process", called["process"] + 1))

    pipeline.run(
        cfg,
        dry_run=True,
        image_dir=img,
        state_path=state_path,
        storefront_qa=True,
        export_storefront_qa_report=str(tmp_path / "qa.csv"),
    )
    assert called["process"] == 0
    assert state_path.read_text(encoding="utf-8") == before


def test_parse_args_prompt_generation_flags(monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prog",
            "--generate-artwork-from-prompt",
            "--art-prompt",
            "retro tiger sunset",
            "--art-target-mode",
            "multi",
            "--art-count",
            "2",
        ],
    )
    args = pipeline.parse_args()
    assert args.generate_artwork_from_prompt is True
    assert args.art_prompt == "retro tiger sunset"
    assert args.art_target_mode == "multi"
    assert args.art_count == 2


def test_parse_args_family_aware_flags(monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prog",
            "--generate-artwork-from-prompt",
            "--art-prompt",
            "retro tiger sunset",
            "--art-family-aware",
            "--art-family-mode",
            "split",
            "--art-generate-poster-master",
            "--art-mug-tote-master",
            "square",
        ],
    )
    args = pipeline.parse_args()
    assert args.art_family_aware is True
    assert args.art_family_mode == "split"
    assert args.art_generate_poster_master is True
    assert args.art_mug_tote_master == "square"


def test_artwork_generation_target_planning_modes():
    assert choose_generation_aspect_modes(template_keys=["hoodie_unisex", "poster_24x36"], target_mode="auto") == ["portrait"]
    assert choose_generation_aspect_modes(template_keys=["mug_11oz", "tote_bag"], target_mode="auto") == ["square"]
    assert choose_generation_aspect_modes(template_keys=["hoodie_unisex", "mug_11oz"], target_mode="auto") == ["portrait", "square"]


def test_plan_generated_artwork_targets_multi():
    plan = plan_generated_artwork_targets(template_keys=["hoodie_unisex", "mug_11oz"], target_mode="auto")
    assert [target.mode for target in plan.targets] == ["portrait", "square"]
    assert any("Mixed template families" in reason for reason in plan.rationale)


def test_family_plan_auto_splits_poster_and_apparel():
    plan = plan_family_artwork_targets(
        template_keys=["hoodie_gildan", "poster_basic", "mug_new"],
        family_mode="auto",
        mug_tote_master="apparel",
    )
    families = {target.family for target in plan.targets}
    assert APPAREL_FAMILY in families
    assert POSTER_FAMILY in families


def test_family_plan_poster_only():
    plan = plan_family_artwork_targets(template_keys=["poster_basic"], family_mode="auto")
    assert [target.family for target in plan.targets] == [POSTER_FAMILY]


def test_family_plan_apparel_only():
    plan = plan_family_artwork_targets(template_keys=["hoodie_gildan", "sweatshirt_gildan"], family_mode="auto")
    assert [target.family for target in plan.targets] == [APPAREL_FAMILY]


def test_family_template_routing_maps_templates_to_family_assets(tmp_path: Path):
    apparel_asset = GeneratedArtworkAsset(path=tmp_path / "x-apparel-c01.png", mode="portrait", concept_index=1, family=APPAREL_FAMILY)
    poster_asset = GeneratedArtworkAsset(path=tmp_path / "x-poster-c01.png", mode="portrait", concept_index=1, family=POSTER_FAMILY)
    routing = route_templates_to_generated_assets(
        template_keys=["hoodie_gildan", "poster_basic"],
        assets=[apparel_asset, poster_asset],
        template_family_map={"hoodie_gildan": APPAREL_FAMILY, "poster_basic": POSTER_FAMILY},
    )
    by_template = {row.template_key: row for row in routing}
    assert by_template["hoodie_gildan"].asset_path == apparel_asset.path
    assert by_template["poster_basic"].asset_path == poster_asset.path


def test_openai_generation_client_can_be_mocked(tmp_path: Path):
    payload = base64.b64encode(b"pngbytes").decode("ascii")

    class StubImagesClient:
        def __init__(self):
            self.images = self

        def generate(self, **kwargs):
            return types.SimpleNamespace(data=[types.SimpleNamespace(b64_json=payload)])

    req = ArtworkGenerationRequest(prompt="wolf", output_dir=tmp_path, base_name="wolf", count=1)
    plan = plan_generated_artwork_targets(template_keys=["hoodie_unisex"], target_mode="portrait")
    assets = generate_artwork_with_openai(request=req, plan=plan, client=StubImagesClient())
    assert len(assets) == 1
    assert assets[0].path.exists()
    assert assets[0].path.read_bytes() == b"pngbytes"


def test_source_hygiene_filters_preview_and_tiny_assets(tmp_path: Path):
    preview_path = tmp_path / "my-removebg-preview.png"
    preview_path.write_bytes(b"x")
    assert is_preview_or_low_value_asset(preview_path) is True

    tiny = GeneratedArtworkAsset(path=tmp_path / "tiny.png", mode="square", concept_index=1, width=512, height=512)
    assert validate_generated_asset_for_templates(tiny, min_width=1024, min_height=1024).startswith("tiny_source_")


def test_build_generation_prompt_includes_pod_composition_rules():
    request = ArtworkGenerationRequest(prompt="vintage truck", visible_text="")
    prompt = build_generation_prompt(request, mode="portrait")
    assert "not a product mockup" in prompt
    assert "strong subject fill" in prompt
    assert "Do not add any text" in prompt


def test_build_generation_prompt_has_family_specific_guidance():
    request = ArtworkGenerationRequest(prompt="vintage truck")
    apparel_prompt = build_generation_prompt(request, mode="portrait", family=APPAREL_FAMILY)
    poster_prompt = build_generation_prompt(request, mode="portrait", family=POSTER_FAMILY)
    assert "isolated standalone graphic" in apparel_prompt
    assert "no poster rectangle" in apparel_prompt
    assert "rich scenic wall-art composition" in poster_prompt


def test_run_prompt_generation_family_result_routes_templates(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    hoodie = ProductTemplate(key="hoodie_gildan", printify_blueprint_id=1, printify_print_provider_id=1, title_pattern="{title}", description_pattern="{description_html}")
    poster = ProductTemplate(key="poster_basic", printify_blueprint_id=1, printify_print_provider_id=1, title_pattern="{title}", description_pattern="{description_html}")

    generated = [
        GeneratedArtworkAsset(path=tmp_path / "prompt-apparel-c01.png", mode="portrait", concept_index=1, width=1200, height=1800, family=APPAREL_FAMILY),
        GeneratedArtworkAsset(path=tmp_path / "prompt-poster-c01.png", mode="portrait", concept_index=1, width=1200, height=1800, family=POSTER_FAMILY),
    ]
    for row in generated:
        Image.new("RGBA", (1200, 1800), (255, 0, 0, 255)).save(row.path)

    monkeypatch.setattr(pipeline, "generate_artwork_with_openai", lambda **kwargs: generated)

    request = ArtworkGenerationRequest(prompt="forest", output_dir=tmp_path, base_name="prompt", family_aware=True)
    result = pipeline.run_prompt_artwork_generation(request=request, templates=[hoodie, poster])
    by_template = {row.template_key: row.asset_path.name for row in result.template_routing}
    assert by_template["hoodie_gildan"].endswith("apparel-c01.png")
    assert by_template["poster_basic"].endswith("poster-c01.png")


def test_non_family_aware_prompt_mode_still_returns_generated_paths(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    hoodie = ProductTemplate(key="hoodie_gildan", printify_blueprint_id=1, printify_print_provider_id=1, title_pattern="{title}", description_pattern="{description_html}")
    asset = GeneratedArtworkAsset(path=tmp_path / "prompt-portrait-c01.png", mode="portrait", concept_index=1, width=1200, height=1800, family="single")
    Image.new("RGBA", (1200, 1800), (255, 0, 0, 255)).save(asset.path)
    monkeypatch.setattr(pipeline, "generate_artwork_with_openai", lambda **kwargs: [asset])

    request = ArtworkGenerationRequest(prompt="forest", output_dir=tmp_path, base_name="prompt", family_aware=False)
    result = pipeline.run_prompt_artwork_generation(request=request, templates=[hoodie])
    assert result.generated_paths == [asset.path]
    assert result.template_routing == []


def test_family_routing_flows_into_create_publish_processing(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    cfg = tmp_path / "templates.json"
    cfg.write_text("[]", encoding="utf-8")
    img_dir = tmp_path / "images"
    img_dir.mkdir()
    state_path = tmp_path / "state.json"

    hoodie_template = ProductTemplate(key="hoodie_gildan", printify_blueprint_id=1, printify_print_provider_id=1, title_pattern="{title}", description_pattern="{description_html}")
    poster_template = ProductTemplate(key="poster_basic", printify_blueprint_id=2, printify_print_provider_id=2, title_pattern="{title}", description_pattern="{description_html}")

    apparel_path = img_dir / "prompt-apparel-c01.png"
    poster_path = img_dir / "prompt-poster-c01.png"
    Image.new("RGBA", (1200, 1800), (255, 0, 0, 255)).save(apparel_path)
    Image.new("RGBA", (1200, 1800), (0, 0, 255, 255)).save(poster_path)

    artworks = [
        Artwork(slug="prompt-apparel-c01", src_path=apparel_path, title="Apparel", description_html="<p>Apparel</p>", tags=["a"], image_width=1200, image_height=1800),
        Artwork(slug="prompt-poster-c01", src_path=poster_path, title="Poster", description_html="<p>Poster</p>", tags=["p"], image_width=1200, image_height=1800),
    ]

    monkeypatch.setenv("PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "load_templates", lambda _cfg: [hoodie_template, poster_template])
    monkeypatch.setattr(pipeline, "discover_artworks", lambda *_a, **_k: artworks)
    monkeypatch.setattr(pipeline, "run_catalog_cli", lambda **kwargs: False)
    monkeypatch.setattr(pipeline, "audit_printify_integration", lambda *a, **k: None)
    monkeypatch.setattr(pipeline, "resolve_shop_id", lambda *a, **k: 1)
    monkeypatch.setattr(pipeline, "PrintifyClient", lambda token, dry_run=False: types.SimpleNamespace(dry_run=dry_run))
    monkeypatch.setattr(
        pipeline,
        "run_prompt_artwork_generation",
        lambda **kwargs: PromptArtworkGenerationResult(
            generated_paths=[apparel_path, poster_path],
            template_routing=[
                TemplateAssetRouting(template_key="hoodie_gildan", family=APPAREL_FAMILY, concept_index=1, asset_path=apparel_path),
                TemplateAssetRouting(template_key="poster_basic", family=POSTER_FAMILY, concept_index=1, asset_path=poster_path),
            ],
        ),
    )
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        pipeline,
        "process_artwork",
        lambda **kwargs: calls.append((kwargs["templates"][0].key, kwargs["artwork"].src_path.name)),
    )

    run(
        cfg,
        dry_run=True,
        image_dir=img_dir,
        state_path=state_path,
        generate_artwork_from_prompt=True,
        art_prompt="forest wolf",
        art_family_aware=True,
        create_only=True,
    )
    assert ("hoodie_gildan", "prompt-apparel-c01.png") in calls
    assert ("poster_basic", "prompt-poster-c01.png") in calls


def test_local_image_batch_max_artworks_processes_first_n_across_templates(tmp_path: Path, monkeypatch):
    import printify_shopify_sync_pipeline as pipeline

    cfg = tmp_path / "templates.json"
    cfg.write_text("[]", encoding="utf-8")
    img_dir = tmp_path / "images"
    img_dir.mkdir()
    state_path = tmp_path / "state.json"

    art1 = Artwork("a1", img_dir / "a1.png", "A1", "", [], 100, 100)
    art2 = Artwork("a2", img_dir / "a2.png", "A2", "", [], 100, 100)
    art3 = Artwork("a3", img_dir / "a3.png", "A3", "", [], 100, 100)
    for art in [art1, art2, art3]:
        Image.new("RGBA", (100, 100), (255, 255, 255, 255)).save(art.src_path)

    templates = [
        ProductTemplate(key="hoodie_gildan", printify_blueprint_id=77, printify_print_provider_id=99, title_pattern="{artwork_title}", description_pattern="{artwork_title}"),
        ProductTemplate(key="tshirt_gildan", printify_blueprint_id=6, printify_print_provider_id=99, title_pattern="{artwork_title}", description_pattern="{artwork_title}"),
    ]
    monkeypatch.setenv("PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "PRINTIFY_API_TOKEN", "token")
    monkeypatch.setattr(pipeline, "load_templates", lambda _cfg: templates)
    monkeypatch.setattr(pipeline, "discover_artworks", lambda *_a, **_k: [art1, art2, art3])
    monkeypatch.setattr(pipeline, "run_catalog_cli", lambda **kwargs: False)
    monkeypatch.setattr(pipeline, "audit_printify_integration", lambda *a, **k: None)
    monkeypatch.setattr(pipeline, "resolve_shop_id", lambda *a, **k: 1)
    monkeypatch.setattr(pipeline, "PrintifyClient", lambda token, dry_run=False: types.SimpleNamespace(dry_run=dry_run))
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(pipeline, "process_artwork", lambda **kwargs: calls.append((kwargs["artwork"].slug, kwargs["templates"][0].key)))

    run(cfg, dry_run=True, image_dir=img_dir, state_path=state_path, max_artworks=2)
    assert len(calls) == 4
    assert {slug for slug, _ in calls} == {"a1", "a2"}


def test_poster_moderately_undersized_uses_bounded_poster_enhancement(tmp_path: Path):
    path = tmp_path / "posterish.png"
    Image.new("RGBA", (3200, 4800), (10, 10, 10, 255)).save(path)
    artwork = Artwork("posterish", path, "Posterish", "", [], 3200, 4800)
    placement = PlacementRequirement("front", 4500, 5400, allow_upscale=False, artwork_fit_mode="contain")
    template = ProductTemplate(
        key="poster_basic",
        printify_blueprint_id=852,
        printify_print_provider_id=73,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[placement],
        poster_safe_max_upscale_factor=1.55,
        poster_safe_min_source_ratio=0.34,
        poster_trim_fill_optimization=True,
    )
    result = resolve_artwork_for_placement(
        artwork,
        placement,
        template=template,
        template_key="poster_basic",
        allow_upscale=False,
        upscale_method="lanczos",
        skip_undersized=False,
        max_upscale_factor=None,
    )
    assert result.poster_enhancement_status == "applied"
    assert result.upscaled is True
    assert result.poster_fill_optimization_used is True


def test_poster_small_portrait_source_uses_bounded_small_source_tier(tmp_path: Path):
    path = tmp_path / "tiny-poster.png"
    Image.new("RGBA", (1024, 1536), (10, 10, 10, 255)).save(path)
    artwork = Artwork("tiny-poster", path, "Tiny Poster", "", [], 1024, 1536)
    placement = PlacementRequirement("front", 4500, 5400, allow_upscale=False, artwork_fit_mode="contain")
    template = ProductTemplate(
        key="poster_basic",
        printify_blueprint_id=852,
        printify_print_provider_id=73,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[placement],
        poster_safe_max_upscale_factor=1.55,
        poster_safe_min_source_ratio=0.34,
    )
    result = resolve_artwork_for_placement(
        artwork,
        placement,
        template=template,
        template_key="poster_basic",
        allow_upscale=False,
        upscale_method="lanczos",
        skip_undersized=False,
        max_upscale_factor=None,
    )
    assert result.poster_enhancement_status == "applied"
    assert result.poster_enhancement_tier == "bounded_small_source"
    assert result.poster_source_ratio > 0.22
    assert result.upscaled is True


def test_poster_tiny_weak_source_still_safely_falls_back(tmp_path: Path):
    path = tmp_path / "too-tiny-poster.png"
    Image.new("RGBA", (640, 900), (10, 10, 10, 255)).save(path)
    artwork = Artwork("too-tiny-poster", path, "Too Tiny Poster", "", [], 640, 900)
    placement = PlacementRequirement("front", 4500, 5400, allow_upscale=False, artwork_fit_mode="contain")
    template = ProductTemplate(
        key="poster_basic",
        printify_blueprint_id=852,
        printify_print_provider_id=73,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[placement],
        poster_safe_max_upscale_factor=1.55,
        poster_safe_min_source_ratio=0.34,
    )
    result = resolve_artwork_for_placement(
        artwork,
        placement,
        template=template,
        template_key="poster_basic",
        allow_upscale=False,
        upscale_method="lanczos",
        skip_undersized=False,
        max_upscale_factor=None,
    )
    assert result.poster_enhancement_status == "skipped_outside_safe_limits"
    assert result.poster_enhancement_tier == "none"
    assert result.upscaled is False


def test_non_poster_templates_do_not_use_poster_enhancement_path(tmp_path: Path):
    path = tmp_path / "shirt.png"
    Image.new("RGBA", (1024, 1536), (10, 10, 10, 255)).save(path)
    artwork = Artwork("shirt", path, "Shirt", "", [], 1024, 1536)
    placement = PlacementRequirement("front", 4500, 5400, allow_upscale=False, artwork_fit_mode="contain")
    template = ProductTemplate(
        key="hoodie_gildan",
        printify_blueprint_id=852,
        printify_print_provider_id=73,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        placements=[placement],
    )
    result = resolve_artwork_for_placement(
        artwork,
        placement,
        template=template,
        template_key="hoodie_gildan",
        allow_upscale=False,
        upscale_method="lanczos",
        skip_undersized=False,
        max_upscale_factor=None,
    )
    assert result.poster_enhancement_status == ""
    assert result.poster_enhancement_tier == ""


def test_tote_front_primary_publish_only_primary_side():
    templates = load_templates(Path("product_templates.json"))
    tote = next(t for t in templates if t.key == "tote_basic")
    artwork = Artwork("a", Path("a.png"), "A", "", [], 1000, 1000)
    variant_rows = [{"id": 1, "price": 1000, "is_available": True}]
    payload = build_printify_product_payload(
        artwork,
        tote,
        variant_rows,
        upload_map={"front": {"id": "upload-front"}, "back": {"id": "upload-back"}},
    )
    assert tote.preferred_primary_placement == "front"
    assert tote.publish_only_primary_placement is True
    assert len(payload["print_areas"]) == 1
    assert payload["print_areas"][0]["placeholders"][0]["position"] == "front"


def test_tshirt_template_added_and_longsleeve_copy_is_distinct(tmp_path: Path):
    templates = load_templates(Path("product_templates.json"))
    by_key = {template.key: template for template in templates}
    assert "tshirt_gildan" in by_key
    assert by_key["tshirt_gildan"].printify_blueprint_id == 6
    assert by_key["tshirt_gildan"].printify_print_provider_id == 99
    assert by_key["longsleeve_gildan"].product_type_label == "Long Sleeve T-Shirt"
    assert by_key["tshirt_gildan"].product_type_label == "T-Shirt"

    art_path = tmp_path / "art.png"
    Image.new("RGBA", (1200, 1800), (255, 255, 255, 255)).save(art_path)
    artwork = Artwork("wave-art", art_path, "Wave Art", "", [], 1200, 1800)
    long_title = render_product_title(by_key["longsleeve_gildan"], artwork)
    tee_title = render_product_title(by_key["tshirt_gildan"], artwork)
    long_desc = render_product_description(by_key["longsleeve_gildan"], artwork).lower()
    tee_desc = render_product_description(by_key["tshirt_gildan"], artwork).lower()
    long_tags = _render_listing_tags(by_key["longsleeve_gildan"], artwork)
    tee_tags = _render_listing_tags(by_key["tshirt_gildan"], artwork)

    assert "long sleeve" in long_title.lower()
    assert "t-shirt" in tee_title.lower() or "tee" in tee_title.lower()
    assert "long sleeve" in long_desc
    assert ("t-shirt" in tee_desc) or ("tee" in tee_desc)
    assert any("long sleeve" in tag.lower() for tag in long_tags)
    assert any(("t-shirt" in tag.lower()) or ("tee" in tag.lower()) for tag in tee_tags)


def test_top10_template_keys_exist_and_curated():
    templates = load_templates(Path("product_templates.json"))
    by_key = {template.key: template for template in templates}
    required = {
        "tshirt_gildan",
        "sweatshirt_gildan",
        "hoodie_gildan",
        "mug_new",
        "poster_basic",
        "tote_basic",
        "canvas_basic",
        "phone_case_basic",
        "sticker_kisscut",
        "blanket_basic",
    }
    assert required.issubset(set(by_key))
    assert by_key["longsleeve_gildan"].active is False
    assert by_key["tshirt_gildan"].enabled_colors == ["Black", "White", "Navy", "Sport Grey", "Sand"]
    assert by_key["tshirt_gildan"].enabled_sizes == ["S", "M", "L", "XL", "2XL", "3XL"]
    assert by_key["phone_case_basic"].max_enabled_variants <= 12
    assert by_key["sticker_kisscut"].printify_blueprint_id == 906
    assert by_key["sticker_kisscut"].printify_print_provider_id == 36
    assert by_key["sticker_kisscut"].max_enabled_variants == 4
    assert by_key["poster_basic"].enabled_sizes == ["11″ x 14″ (Vertical)", "12″ x 16″ (Vertical)", "16″ x 20″ (Vertical)"]


def test_guardrail_zero_enabled_variants_skip_before_payload_validation():
    class StubPrintify:
        dry_run = False
        created = False

        def create_product(self, shop_id, payload):
            self.created = True
            return {"id": "p1"}

    template = ProductTemplate(
        key="tshirt_gildan",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        base_price="10.00",
        markup_type="fixed",
        markup_value="0.00",
        min_margin_after_shipping="5.00",
        target_margin_after_shipping="5.00",
        reprice_variants_to_margin_floor=False,
        disable_variants_below_margin_floor=True,
        mark_template_nonviable_if_needed=True,
        publish_after_create=False,
        placements=[PlacementRequirement("front", 4500, 5400)],
    )
    artwork = Artwork("a", Path("a.png"), "A", "", [], 1000, 1000)
    printify = StubPrintify()
    result = upsert_in_printify(
        printify=printify,
        shop_id=1,
        artwork=artwork,
        template=template,
        variant_rows=[{"id": 9, "price": 1000, "cost": 1000, "shipping": 900, "is_available": True}],
        upload_map={"front": {"id": "u1"}},
        existing_product_id="",
        action="create",
        publish_mode="default",
        verify_publish=False,
    )
    assert result["status"] == "skipped_nonviable"
    assert result["reason"].startswith("no_enabled_variants_after_guardrails")
    assert printify.created is False


def test_longsleeve_nonviable_returns_structured_skip():
    class StubPrintify:
        dry_run = False

        def create_product(self, shop_id, payload):
            raise AssertionError("create_product should not be called for nonviable long-sleeve")

    template = ProductTemplate(
        key="longsleeve_gildan",
        printify_blueprint_id=1,
        printify_print_provider_id=1,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        min_margin_after_shipping="4.00",
        target_margin_after_shipping="5.00",
        reprice_variants_to_margin_floor=False,
        disable_variants_below_margin_floor=True,
        mark_template_nonviable_if_needed=True,
    )
    artwork = Artwork("a", Path("a.png"), "A", "", [], 1000, 1000)
    result = upsert_in_printify(
        printify=StubPrintify(),
        shop_id=1,
        artwork=artwork,
        template=template,
        variant_rows=[{"id": 2, "price": 1500, "cost": 1500, "shipping": 900, "is_available": True}],
        upload_map={"front": {"id": "u1"}},
        existing_product_id="",
        action="create",
        publish_mode="default",
        verify_publish=False,
    )
    assert result["status"] == "skipped_nonviable"
    assert result["guardrail_report"]["final_enabled_count"] == 0


def test_provider_selection_prefers_printify_choice_when_available():
    class StubPrintify:
        def list_print_providers(self, blueprint_id):
            return [{"id": 10, "title": "Other Provider"}, {"id": 11, "title": "Printify Choice"}]

        def list_variants(self, blueprint_id, provider_id, show_out_of_stock=True):
            return [{"id": provider_id, "is_available": True, "options": {"color": "Black", "size": "M"}, "cost": 1200, "price": 1200}]

    template = ProductTemplate(
        key="canvas_basic",
        printify_blueprint_id=13,
        printify_print_provider_id=10,
        title_pattern="{artwork_title}",
        description_pattern="{artwork_title}",
        provider_selection_strategy="prefer_printify_choice_then_ranked",
    )
    resolved = select_provider_for_template(printify=StubPrintify(), template=template)
    assert resolved.printify_print_provider_id == 11


def test_catalog_cache_persists_hit_miss(tmp_path: Path):
    cache_dir = tmp_path / "catalog-cache"
    cache = CatalogCache(cache_dir=cache_dir, ttl_hours=24, enabled=True)
    assert cache.get("providers:1") is None
    cache.set("providers:1", [{"id": 11}])
    assert cache.get("providers:1") == [{"id": 11}]
    cache_reload = CatalogCache(cache_dir=cache_dir, ttl_hours=24, enabled=True)
    assert cache_reload.get("providers:1") == [{"id": 11}]


def test_printify_client_reuses_cached_variants(tmp_path: Path):
    import printify_shopify_sync_pipeline as pipeline

    class SessionStub:
        def __init__(self):
            self.headers = {}

    cache = CatalogCache(cache_dir=tmp_path / "cache", ttl_hours=24, enabled=True)
    pipeline.requests.Session = SessionStub  # type: ignore[assignment]
    client = PrintifyClient("token", dry_run=True, catalog_cache=cache)
    calls = {"count": 0}

    def fake_get(path, **params):
        calls["count"] += 1
        return [{"id": 7, "is_available": True, "options": {"color": "Black", "size": "M"}}]

    client.get = fake_get  # type: ignore[assignment]
    first = client.list_variants(10, 20)
    second = client.list_variants(10, 20)
    assert first == second
    assert calls["count"] == 1


def test_high_volume_mode_sets_safe_defaults():
    defaults = apply_high_volume_mode_defaults(
        chunk_size=0,
        pause_between_chunks_seconds=0,
        catalog_request_spacing_ms=0,
        template_spacing_ms=0,
        artwork_spacing_ms=0,
        no_catalog_cache=True,
        publish_batch_size=0,
        pause_between_publish_batches_seconds=0,
        defer_publish=False,
    )
    assert defaults["chunk_size"] == 10
    assert defaults["catalog_request_spacing_ms"] == 150
    assert defaults["no_catalog_cache"] is False
    assert defaults["publish_batch_size"] == 5
