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


def test_process_artwork_updates_existing_product_by_default(tmp_path: Path):
    class UpdateCapablePrintify(DummyPrintify):
        dry_run = False

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
