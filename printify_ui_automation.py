from __future__ import annotations

import argparse
import csv
import json
import os
import pathlib
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class AutomationTarget:
    listing_slug: str
    product_id: str
    printify_product_url: str
    row_id: Optional[str]
    synced_to_shopify: bool
    manual_setup_required: bool
    should_enable_personalization: bool
    personalization_toggle_manual_required: bool
    printify_personalize_button_required: bool
    editable_fields_summary: str
    supports_text_edit: bool
    supports_photo_upload: bool
    supports_logo_upload: bool
    variant_visibility_recommended: str
    sync_details_recommended: str


@dataclass
class ProductActionResult:
    listing_slug: str
    row_id: Optional[str]
    ui_automation_status: str
    ui_automation_last_run_at: str
    ui_automation_last_result: str
    ui_automation_screenshot_paths: List[str]
    action_log_path: str


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_csv_rows(path: pathlib.Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def load_setup_packets(setup_packet_dir: pathlib.Path) -> Dict[str, Dict[str, Any]]:
    packets: Dict[str, Dict[str, Any]] = {}
    for packet_path in sorted(setup_packet_dir.glob("*.json")):
        data = json.loads(packet_path.read_text(encoding="utf-8"))
        slug = str(data.get("listing_slug") or packet_path.stem)
        packets[slug] = data
    return packets


def build_targets(
    checklist_rows: List[Dict[str, str]],
    queue_rows: List[Dict[str, str]],
    setup_packets: Dict[str, Dict[str, Any]],
) -> List[AutomationTarget]:
    queue_by_slug = {str(r.get("listing_slug", "")).strip(): r for r in queue_rows}
    targets: List[AutomationTarget] = []
    for row in checklist_rows:
        slug = str(row.get("listing_slug", "")).strip()
        if not slug:
            continue
        queue_row = queue_by_slug.get(slug, {})
        packet = setup_packets.get(slug, {})
        product_id = str(packet.get("printify_product_id") or queue_row.get("printify_product_id") or row.get("printify_product_id") or "").strip()
        if not product_id:
            continue
        printify_url = str(packet.get("printify_product_url") or queue_row.get("printify_product_url") or f"https://printify.com/app/products/{product_id}").strip()
        targets.append(
            AutomationTarget(
                listing_slug=slug,
                product_id=product_id,
                printify_product_url=printify_url,
                row_id=str(queue_row.get("row_id") or row.get("row_id") or "").strip() or None,
                synced_to_shopify=_parse_bool(queue_row.get("synced_to_shopify") or row.get("synced_to_shopify")),
                manual_setup_required=_parse_bool(queue_row.get("manual_setup_required") or row.get("manual_setup_required")),
                should_enable_personalization=_parse_bool(row.get("should_enable_personalization") or packet.get("should_enable_personalization")),
                personalization_toggle_manual_required=_parse_bool(row.get("personalization_toggle_manual_required") or packet.get("personalization_toggle_manual_required")),
                printify_personalize_button_required=_parse_bool(row.get("printify_personalize_button_required") or packet.get("printify_personalize_button_required")),
                editable_fields_summary=str(row.get("editable_fields_summary") or packet.get("editable_fields_summary") or "").strip(),
                supports_text_edit=_parse_bool(row.get("supports_text_edit") or packet.get("supports_text_edit")),
                supports_photo_upload=_parse_bool(row.get("supports_photo_upload") or packet.get("supports_photo_upload")),
                supports_logo_upload=_parse_bool(row.get("supports_logo_upload") or packet.get("supports_logo_upload")),
                variant_visibility_recommended=str(row.get("variant_visibility_recommended") or packet.get("variant_visibility_recommended") or "").strip(),
                sync_details_recommended=str(row.get("sync_details_recommended") or packet.get("sync_details_recommended") or "").strip(),
            )
        )
    return targets


def select_targets(
    targets: List[AutomationTarget],
    listing_slugs: List[str],
    row_ids: List[str],
    synced_manual_only: bool,
) -> List[AutomationTarget]:
    if not listing_slugs and not row_ids and not synced_manual_only:
        raise ValueError("Refusing to run without explicit --listing-slug, --row-id, or --synced-manual-only.")

    chosen = targets
    if listing_slugs:
        wanted = {s.strip() for s in listing_slugs}
        chosen = [t for t in chosen if t.listing_slug in wanted]
    if row_ids:
        wanted_rows = {r.strip() for r in row_ids}
        chosen = [t for t in chosen if t.row_id in wanted_rows]
    if synced_manual_only:
        chosen = [t for t in chosen if t.synced_to_shopify and t.manual_setup_required]
    return chosen


def _safe_filename(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in value)


def _write_action_log(output_dir: pathlib.Path, listing_slug: str, actions: List[Dict[str, Any]]) -> pathlib.Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{_safe_filename(listing_slug)}-actions.json"
    path.write_text(json.dumps(actions, indent=2), encoding="utf-8")
    return path


def generate_shopify_theme_checklist(path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "# Shopify Theme Personalization Checklist",
                "",
                "- [ ] Open Online Store > Themes > Customize.",
                "- [ ] Select the default product template used by Printify-synced products.",
                "- [ ] Verify the **Printify Personalize Button** app block is present on the product page.",
                "- [ ] If missing, add the app block and save.",
                "- [ ] Validate one text-only product and one logo/photo-upload product in preview.",
            ]
        ),
        encoding="utf-8",
    )


class PrintifyUiAutomator:
    def __init__(self, headless: bool = True, pause_per_product: bool = False):
        self.headless = headless
        self.pause_per_product = pause_per_product

    def run_product(self, target: AutomationTarget, output_dir: pathlib.Path, dry_run: bool = False, screenshot_only: bool = False) -> ProductActionResult:
        actions: List[Dict[str, Any]] = []
        screenshots: List[str] = []
        actions.append({"ts": _now_iso(), "action": "open_product", "url": target.printify_product_url})

        if self.pause_per_product and not self.headless:
            input(f"Review target {target.listing_slug}. Press Enter to continue...")

        if dry_run:
            actions.append({"ts": _now_iso(), "action": "dry_run_detect_personalization", "expected": target.should_enable_personalization})
            actions.append({"ts": _now_iso(), "action": "dry_run_find_publish_panel"})
            action_log = _write_action_log(output_dir, target.listing_slug, actions)
            return ProductActionResult(
                listing_slug=target.listing_slug,
                row_id=target.row_id,
                ui_automation_status="dry_run",
                ui_automation_last_run_at=_now_iso(),
                ui_automation_last_result="dry_run_completed",
                ui_automation_screenshot_paths=screenshots,
                action_log_path=str(action_log),
            )

        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # noqa: BLE001
            actions.append({"ts": _now_iso(), "action": "error", "message": f"playwright_import_failed: {exc}"})
            action_log = _write_action_log(output_dir, target.listing_slug, actions)
            return ProductActionResult(
                listing_slug=target.listing_slug,
                row_id=target.row_id,
                ui_automation_status="failed",
                ui_automation_last_run_at=_now_iso(),
                ui_automation_last_result="playwright_import_failed",
                ui_automation_screenshot_paths=screenshots,
                action_log_path=str(action_log),
            )

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context()
            page = context.new_page()
            try:
                page.goto(target.printify_product_url, wait_until="domcontentloaded", timeout=120000)
                before = output_dir / f"{_safe_filename(target.listing_slug)}-before.png"
                before.parent.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=str(before), full_page=True)
                screenshots.append(str(before))

                personalize_toggle = page.locator('label:has-text("Personalization"), text=/Personalization/i').first
                publish_panel = page.locator('text=/Publish|Republish|Selective publishing/i').first
                if personalize_toggle.count() == 0 or publish_panel.count() == 0:
                    raise RuntimeError("Required selectors not found; aborting without unsafe clicks.")

                actions.append({"ts": _now_iso(), "action": "detected_sections", "personalization": True, "publish_panel": True})
                if not screenshot_only:
                    if target.should_enable_personalization:
                        personalize_toggle.click()
                        actions.append({"ts": _now_iso(), "action": "enable_personalization_clicked"})

                    if target.variant_visibility_recommended.lower() == "in_stock_only":
                        variant_toggle = page.locator('label:has-text("In stock only"), text=/in stock only/i').first
                        if variant_toggle.count() == 0:
                            raise RuntimeError("Variant visibility selector missing.")
                        variant_toggle.click()
                        actions.append({"ts": _now_iso(), "action": "variant_visibility_set", "value": "in_stock_only"})

                    for label in ["Title", "Description", "Mockups", "Colors", "Sizes", "Prices", "SKUs", "Tags", "Shipping profile"]:
                        selector = page.locator(f'label:has-text("{label}"), text=/{label}/i').first
                        if selector.count() == 0:
                            raise RuntimeError(f"Selective publish checkbox for '{label}' not found.")
                        selector.click()
                        actions.append({"ts": _now_iso(), "action": "selective_publish_checked", "field": label})

                    publish_button = page.locator('button:has-text("Publish"), button:has-text("Republish")').first
                    if publish_button.count() == 0:
                        raise RuntimeError("Publish/Republish button not found.")
                    publish_button.click()
                    actions.append({"ts": _now_iso(), "action": "publish_clicked"})

                after = output_dir / f"{_safe_filename(target.listing_slug)}-after.png"
                page.screenshot(path=str(after), full_page=True)
                screenshots.append(str(after))
                status = "screenshot_only" if screenshot_only else "completed"
                result = "selectors_verified" if screenshot_only else "publish_flow_executed"
            except Exception as exc:  # noqa: BLE001
                status = "failed"
                result = str(exc)
                actions.append({"ts": _now_iso(), "action": "error", "message": str(exc)})
                diag = output_dir / f"{_safe_filename(target.listing_slug)}-diagnostic.png"
                try:
                    page.screenshot(path=str(diag), full_page=True)
                    screenshots.append(str(diag))
                except Exception:
                    pass
            finally:
                context.close()
                browser.close()

        action_log = _write_action_log(output_dir, target.listing_slug, actions)
        return ProductActionResult(
            listing_slug=target.listing_slug,
            row_id=target.row_id,
            ui_automation_status=status,
            ui_automation_last_run_at=_now_iso(),
            ui_automation_last_result=result,
            ui_automation_screenshot_paths=screenshots,
            action_log_path=str(action_log),
        )


def write_report(results: List[ProductActionResult], output_path: pathlib.Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [asdict(r) for r in results]
    output_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    csv_path = output_path.with_suffix(".csv")
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "listing_slug",
                "row_id",
                "ui_automation_status",
                "ui_automation_last_run_at",
                "ui_automation_last_result",
                "ui_automation_screenshot_paths",
                "action_log_path",
            ],
        )
        writer.writeheader()
        for row in rows:
            row = dict(row)
            row["ui_automation_screenshot_paths"] = "|".join(row["ui_automation_screenshot_paths"])
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Printify UI automation helper (Phase 8)")
    p.add_argument("--checklist-csv", type=pathlib.Path, required=True)
    p.add_argument("--queue-csv", type=pathlib.Path, required=True)
    p.add_argument("--setup-packet-dir", type=pathlib.Path, required=True)
    p.add_argument("--output-dir", type=pathlib.Path, default=pathlib.Path("./ui_automation"))
    p.add_argument("--listing-slug", action="append", default=[])
    p.add_argument("--row-id", action="append", default=[])
    p.add_argument("--synced-manual-only", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--screenshot-only", action="store_true")
    p.add_argument("--headless", action="store_true")
    p.add_argument("--pause-per-product", action="store_true")
    p.add_argument("--generate-shopify-theme-checklist", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    checklist_rows = load_csv_rows(args.checklist_csv)
    queue_rows = load_csv_rows(args.queue_csv)
    setup_packets = load_setup_packets(args.setup_packet_dir)
    targets = build_targets(checklist_rows, queue_rows, setup_packets)
    selected = select_targets(targets, args.listing_slug, args.row_id, args.synced_manual_only)

    if not selected:
        raise RuntimeError("No targets matched selection filters.")

    if args.generate_shopify_theme_checklist:
        generate_shopify_theme_checklist(args.output_dir / "shopify_theme_personalization_checklist.md")

    automator = PrintifyUiAutomator(headless=args.headless, pause_per_product=args.pause_per_product)
    results: List[ProductActionResult] = []
    for target in selected:
        results.append(
            automator.run_product(
                target=target,
                output_dir=args.output_dir / "artifacts",
                dry_run=args.dry_run,
                screenshot_only=args.screenshot_only,
            )
        )

    write_report(results, args.output_dir / "ui_automation_report.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
