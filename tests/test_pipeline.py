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
    load_artwork_metadata_map,
    resolve_artwork_metadata_for_path,
    resolve_artwork_metadata_with_source,
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
    resolve_tote_template_catalog_mapping,
    export_launch_plan_from_images,
    resolve_launch_plan_rows,
    build_resolved_template,
    build_printify_product_payload,
    normalize_printify_transform,
    resolve_artwork_for_placement,
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
)
from artwork_metadata_generator import (
    HeuristicArtworkMetadataGenerator,
    should_write_sidecar,
    write_artwork_sidecar,
    preview_generated_metadata,
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
    assert "description_generic_fallback_with_metadata" not in description_warnings
    assert "tags_generic_only" not in tag_warnings


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
