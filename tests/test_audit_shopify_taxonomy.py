import importlib.util
import json
import sys
from pathlib import Path


def _load_audit_module():
    module_path = Path(__file__).resolve().parents[1] / "tools" / "audit_shopify_taxonomy.py"
    spec = importlib.util.spec_from_file_location("audit_shopify_taxonomy", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_audit_taxonomy_extracts_expected_groups_and_conflicts(tmp_path: Path):
    module = _load_audit_module()

    templates_path = tmp_path / "product_templates.json"
    templates_path.write_text(
        json.dumps(
            [
                {"key": "tshirt_gildan", "active": True},
                {"key": "accent_mug_basic", "active": True},
                {"key": "disabled_one", "active": False},
            ]
        ),
        encoding="utf-8",
    )

    qa_rows = [
        {
            "template_key": "tshirt_gildan",
            "product_type": "T-Shirts",
            "department_key": "apparel",
            "department_label": "Apparel",
            "primary_collection_handle": "t-shirts",
            "primary_collection_title": "T-Shirts",
            "recommended_manual_collections": "featured, best-sellers",
            "recommended_smart_collection_tags": "family-tshirt, dept-apparel, theme-minimal",
        },
        {
            "template_key": "accent_mug_basic",
            "product_type": "Mugs",
            "department_key": "drinkware",
            "department_label": "Drinkware",
            "primary_collection_handle": "mugs",
            "primary_collection_title": "Mugs",
            "recommended_manual_collections": "featured, new-drops",
            "recommended_smart_collection_tags": "family-mug, dept-drinkware, season-holiday",
        },
        {
            "template_key": "accent_mug_basic",
            "product_type": "Mugs",
            "department_key": "drinkware",
            "department_label": "Drinkware",
            "primary_collection_handle": "mugs-dup",
            "primary_collection_title": "Mugs",
            "recommended_manual_collections": "featured",
            "recommended_smart_collection_tags": "family-mug",
        },
        {
            "template_key": "disabled_one",
            "product_type": "Ignored",
            "department_key": "ignored",
            "department_label": "Ignored",
            "primary_collection_handle": "ignored",
            "primary_collection_title": "Ignored",
            "recommended_manual_collections": "ignored",
            "recommended_smart_collection_tags": "family-ignored",
        },
    ]

    active_keys = module.load_active_template_keys(templates_path)
    audit = module.audit_taxonomy(template_keys=active_keys, qa_rows=qa_rows)

    assert audit["summary"]["active_template_count"] == 2
    assert audit["summary"]["active_qa_row_count"] == 3

    records = audit["records"]
    assert any(r["group_type"] == "product_type" and r["title"] == "Mugs" and r["handle"] == "mugs" for r in records)
    assert any(r["group_type"] == "department" and r["title"] == "Drinkware" and r["handle"] == "drinkware" for r in records)
    assert any(r["group_type"] == "manual_collection" and r["handle"] == "best-sellers" for r in records)
    assert any(r["group_type"] == "smart_tag" and r["tag"] == "season-holiday" for r in records)

    assert audit["conflicts"]["product_type_handle_conflicts"]["Mugs"] == ["mugs", "mugs-dup"]

    guide = module.build_markdown_guide(audit, qa_json_path=Path("storefront_qa.json"), templates_path=templates_path)
    assert "## Department collections" in guide
    assert "## Product-type collections" in guide
    assert "## Smart collection tag tokens" in guide
    assert "family-mug" in guide


def test_write_csv_emits_expected_columns(tmp_path: Path):
    module = _load_audit_module()

    csv_path = tmp_path / "audit.csv"
    module.write_csv(
        csv_path,
        [
            {
                "group_type": "smart_tag",
                "source_field": "recommended_smart_collection_tags",
                "template_keys": "accent_mug_basic",
                "title": "",
                "handle": "",
                "tag": "family-mug",
                "prefix_group": "family",
                "notes": "",
                "shopify_collection_type": "smart",
            }
        ],
    )

    content = csv_path.read_text(encoding="utf-8")
    assert "group_type,source_field,template_keys,title,handle,tag,prefix_group,notes,shopify_collection_type" in content
    assert "smart_tag" in content
    assert "family-mug" in content
