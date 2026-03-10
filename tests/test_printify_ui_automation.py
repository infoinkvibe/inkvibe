import csv
import json
from pathlib import Path

from printify_ui_automation import (
    build_targets,
    generate_shopify_theme_checklist,
    load_csv_rows,
    load_setup_packets,
    select_targets,
    write_report,
    ProductActionResult,
)


def _write_csv(path: Path, headers, rows):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_target_selection_and_filters(tmp_path: Path):
    checklist = tmp_path / "shopify_personalization_setup_checklist.csv"
    queue = tmp_path / "queue.csv"
    packets = tmp_path / "setup_packets"
    packets.mkdir()

    _write_csv(
        checklist,
        ["listing_slug", "printify_product_id", "should_enable_personalization", "variant_visibility_recommended"],
        [
            {
                "listing_slug": "text-tee",
                "printify_product_id": "pid-1",
                "should_enable_personalization": "YES",
                "variant_visibility_recommended": "in_stock_only",
            }
        ],
    )
    _write_csv(
        queue,
        ["listing_slug", "row_id", "synced_to_shopify", "manual_setup_required"],
        [
            {
                "listing_slug": "text-tee",
                "row_id": "row-7",
                "synced_to_shopify": "YES",
                "manual_setup_required": "YES",
            }
        ],
    )
    (packets / "text-tee.json").write_text(json.dumps({"listing_slug": "text-tee"}), encoding="utf-8")

    targets = build_targets(load_csv_rows(checklist), load_csv_rows(queue), load_setup_packets(packets))
    assert len(targets) == 1
    chosen = select_targets(targets, ["text-tee"], [], False)
    assert chosen[0].row_id == "row-7"

    chosen_synced = select_targets(targets, [], [], True)
    assert len(chosen_synced) == 1


def test_report_and_shopify_checklist_generation(tmp_path: Path):
    output_json = tmp_path / "ui_automation_report.json"
    write_report(
        [
            ProductActionResult(
                listing_slug="logo-hoodie",
                row_id="11",
                ui_automation_status="dry_run",
                ui_automation_last_run_at="2026-01-01T00:00:00+00:00",
                ui_automation_last_result="dry_run_completed",
                ui_automation_screenshot_paths=["a.png", "b.png"],
                action_log_path="action.json",
            )
        ],
        output_json,
    )
    loaded = json.loads(output_json.read_text(encoding="utf-8"))
    assert loaded[0]["listing_slug"] == "logo-hoodie"
    assert output_json.with_suffix(".csv").exists()

    checklist_path = tmp_path / "shopify_theme_personalization_checklist.md"
    generate_shopify_theme_checklist(checklist_path)
    text = checklist_path.read_text(encoding="utf-8")
    assert "Printify Personalize Button" in text
