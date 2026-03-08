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
    load_templates,
    normalize_catalog_variants_response,
    prepare_artwork_export,
    process_artwork,
    save_json_atomic,
    DryRunMutationSkipped,
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
