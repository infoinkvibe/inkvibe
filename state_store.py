from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def ensure_state_shape(state: Dict[str, Any]) -> Dict[str, Any]:
    state.setdefault("processed", {})
    state.setdefault("uploads", {})
    state.setdefault("shopify", {})
    state.setdefault("printify", {})
    state.setdefault("debug_runs", {})
    state.setdefault("publish_queue", [])
    return state


def _compact_result(row: Dict[str, Any]) -> Dict[str, Any]:
    result = row.get("result", {}) if isinstance(row.get("result"), dict) else {}
    printify_result = result.get("printify", {}) if isinstance(result.get("printify"), dict) else {}
    verification = printify_result.get("verification", {}) if isinstance(printify_result.get("verification"), dict) else {}
    return {
        "status": result.get("status"),
        "error": result.get("error"),
        "printify": {
            "status": printify_result.get("status"),
            "action": printify_result.get("action"),
            "printify_product_id": printify_result.get("printify_product_id"),
            "publish_attempted": printify_result.get("publish_attempted"),
            "publish_verified": printify_result.get("publish_verified"),
            "verification": {
                "ok": verification.get("ok"),
                "warnings": verification.get("warnings"),
                "verified_title": verification.get("verified_title"),
                "verified_variant_count": verification.get("verified_variant_count"),
            } if verification else {},
        },
    }


def compact_state_row(
    *,
    state_key: str,
    artwork_fingerprint: str,
    template_key: str,
    completion_status: str,
    last_action: str,
    product_id: str,
    upload_ids_by_placement: Dict[str, str],
    publish_attempted: bool,
    publish_verified: bool,
    launch_plan_row: str = "",
    launch_plan_row_id: str = "",
    result: Optional[Dict[str, Any]] = None,
    title_source: str = "",
    rendered_title: str = "",
    blueprint_id: int = 0,
    print_provider_id: int = 0,
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "state_key": state_key,
        "artwork_fingerprint": artwork_fingerprint,
        "template": template_key,
        "blueprint_id": blueprint_id,
        "print_provider_id": print_provider_id,
        "upload_ids_by_placement": upload_ids_by_placement,
        "product_id": product_id,
        "completion_status": completion_status,
        "last_action": last_action,
        "publish_attempted": bool(publish_attempted),
        "publish_verified": bool(publish_verified),
        "last_updated_at": now,
        "last_verified_at": now if publish_verified else None,
        "title_source": title_source,
        "rendered_title": rendered_title,
        "launch_plan_row": launch_plan_row,
        "launch_plan_row_id": launch_plan_row_id,
        "result": _compact_result({"result": result or {}}),
    }


def migrate_legacy_row(row: Dict[str, Any]) -> Dict[str, Any]:
    result = row.get("result", {}) if isinstance(row.get("result"), dict) else {}
    printify_result = result.get("printify", {}) if isinstance(result.get("printify"), dict) else {}
    product_id = str(row.get("product_id") or printify_result.get("printify_product_id") or "")
    upload_map = row.get("upload_ids_by_placement")
    if not isinstance(upload_map, dict):
        upload_map = {}
    return {
        **row,
        "product_id": product_id,
        "upload_ids_by_placement": upload_map,
        "artwork_fingerprint": row.get("artwork_fingerprint", ""),
        "launch_plan_row": str(row.get("launch_plan_row") or ""),
        "launch_plan_row_id": str(row.get("launch_plan_row_id") or ""),
        "result": _compact_result(row),
    }


def derive_state_index(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    processed = state.get("processed", {}) if isinstance(state, dict) else {}
    index: Dict[str, Dict[str, Any]] = {}
    for artwork_record in processed.values():
        if not isinstance(artwork_record, dict):
            continue
        rows = artwork_record.get("products", [])
        if not isinstance(rows, list):
            continue
        for raw_row in rows:
            if not isinstance(raw_row, dict):
                continue
            row = migrate_legacy_row(raw_row)
            state_key = str(row.get("state_key") or "").strip()
            if state_key:
                index[state_key] = row
    return index


def list_state_keys(state: Dict[str, Any]) -> List[str]:
    return sorted(derive_state_index(state).keys())


def inspect_state_key(state: Dict[str, Any], state_key: str) -> Optional[Dict[str, Any]]:
    return derive_state_index(state).get(state_key)


def row_status(row: Dict[str, Any]) -> str:
    result = row.get("result", {}) if isinstance(row.get("result"), dict) else {}
    if result.get("error"):
        return "failure"
    completion_status = str(row.get("completion_status") or "").strip().lower()
    if completion_status == "dry-run-only":
        return "dry-run"
    printify_result = result.get("printify", {}) if isinstance(result.get("printify"), dict) else {}
    if str(printify_result.get("status") or "").lower() == "dry-run":
        return "dry-run"
    if str(printify_result.get("status") or "").lower() == "skipped":
        return "skipped"
    status = str(result.get("status") or "").lower()
    if status == "dry-run":
        return "dry-run"
    if status.startswith("skipped") or status == "no_matching_variants":
        return "skipped"
    return "success"


def latest_rows_by_state_key(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return derive_state_index(state)


def is_state_key_successful(state: Dict[str, Any], state_key: str) -> bool:
    row = latest_rows_by_state_key(state).get(state_key)
    if not row:
        return False
    return row_status(row) == "success"


def row_completion_label(row: Dict[str, Any]) -> str:
    status = row_status(row)
    if status == "success":
        return "real-completed"
    if status == "dry-run":
        return "dry-run-only"
    return status
