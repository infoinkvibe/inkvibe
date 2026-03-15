import json
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Lightweight stubs so tests run without external deps installed in CI sandbox.
sys.modules.setdefault("requests", types.SimpleNamespace(Session=lambda: None))
sys.modules.setdefault("dotenv", types.SimpleNamespace(load_dotenv=lambda: None))

import pytest
from PIL import Image

from r2_uploader import build_r2_public_url

from printify_shopify_sync_pipeline import (
    BaseApiClient,
    Artwork,
    ArtworkProcessingOptions,
    PlacementRequirement,
    ProductTemplate,
    PreparedArtwork,
    TemplateValidationError,
    choose_upload_strategy,
    _compute_backoff,
    choose_variants_from_catalog,
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
    save_json_atomic,
    DryRunMutationSkipped,
    NonRetryableRequestError,
    CatalogCliUsageError,
    run_catalog_cli,
    RunSummary,
    list_state_keys,
    inspect_state_key,
    load_artwork_metadata,
    filename_title_quality_reason,
    resolve_artwork_title,
    _render_listing_tags,
    preview_listing_copy,
    title_semantically_includes_product_label,
    validate_printify_payload_consistency,
    assess_update_compatibility,
    enforce_variant_safety_limit,
    upsert_in_printify,
    _row_status,
    write_csv_report,
    run,
    format_run_summary,
    template_blueprint_type_warning,
    generate_mug_template_snippet,
    parse_launch_plan_csv,
    compute_placement_transform_for_artwork,
    export_launch_plan_from_images,
    resolve_launch_plan_rows,
    build_resolved_template,
    build_printify_product_payload,
    normalize_printify_transform,
)


class DummyPrintify:
    dry_run = True

    def list_variants(self, blueprint_id, provider_id):
        return [{"id": 1, "is_available": True, "options": {"color": "Black", "size": "M"}, "price": 1200}]

    def upload_image(self, file_path):
        return {"id": "upload-1"}


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
    placement = PlacementRequirement("front", 4500, 5400)
    with pytest.raises(ValueError, match="image too small"):
        prepare_artwork_export(artwork, template, placement, tmp_path / "exports", ArtworkProcessingOptions())


def test_skip_behavior_with_skip_undersized(tmp_path: Path):
    artwork = _create_artwork(tmp_path, 495, 504)
    template = _template_for_variant_tests()
    placement = PlacementRequirement("front", 4500, 5400)
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
    placement = PlacementRequirement("front", 4500, 5400)
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
    assert "<ul>" in description
    assert "style upgrade" in description


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


def test_template_filtering_by_key_and_limit():
    templates = [
        ProductTemplate("tee", 1, 1, "{artwork_title}", "{artwork_title}"),
        ProductTemplate("mug", 2, 2, "{artwork_title}", "{artwork_title}"),
        ProductTemplate("poster", 3, 3, "{artwork_title}", "{artwork_title}"),
    ]
    selected = select_templates(templates, template_keys=["mug", "poster"], limit_templates=1)
    assert [template.key for template in selected] == ["mug"]


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
        kwargs["failure_rows"].append(pipeline.FailureReportRow("now", "a1.png", "a1", kwargs["templates"][0].key, "create", 1, 1, "auto", "RuntimeError", "boom", "fix"))
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
    mug = next(t for t in templates if t.key == "mug_11oz")
    assert mug.printify_blueprint_id == 68
    assert mug.printify_print_provider_id == 1
    assert mug.max_enabled_variants is not None and mug.max_enabled_variants <= 24


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
