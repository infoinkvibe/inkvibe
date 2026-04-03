#!/usr/bin/env python3
"""Audit Shopify taxonomy fields from storefront QA export rows.

This tool reads active template keys from ``product_templates.json`` and canonical
Shopify-facing taxonomy strings from storefront QA export JSON rows.

Outputs are written to ``reports/`` by default:
- shopify_taxonomy_audit.json
- shopify_taxonomy_audit.csv
- shopify_collection_setup_guide.md
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


SMART_PREFIX_ORDER = ["family", "dept", "theme", "season", "audience"]


@dataclass(frozen=True)
class CollectionRecord:
    group_type: str
    source_field: str
    template_keys: Tuple[str, ...]
    title: str
    handle: str
    tag: str
    prefix_group: str
    notes: str
    shopify_collection_type: str

    def to_row(self) -> Dict[str, str]:
        return {
            "group_type": self.group_type,
            "source_field": self.source_field,
            "template_keys": ", ".join(self.template_keys),
            "title": self.title,
            "handle": self.handle,
            "tag": self.tag,
            "prefix_group": self.prefix_group,
            "notes": self.notes,
            "shopify_collection_type": self.shopify_collection_type,
        }


def _split_csv_like(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        vals = [str(v).strip() for v in raw]
        return [v for v in vals if v]
    text = str(raw).strip()
    if not text:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def _title_from_handle(handle: str) -> str:
    parts = [p for p in handle.replace("_", "-").split("-") if p]
    if not parts:
        return ""
    return " ".join(part.capitalize() for part in parts)


def load_active_template_keys(path: Path) -> Set[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Expected list in {path}")
    active = set()
    for row in payload:
        if not isinstance(row, dict):
            continue
        if not row.get("active", True):
            continue
        key = str(row.get("key") or "").strip()
        if key:
            active.add(key)
    return active


def load_qa_rows(path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Expected list in {path}")
    return [row for row in payload if isinstance(row, dict)]


def _find_conflicts(values: Dict[str, Set[str]]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for key, variants in values.items():
        cleaned = sorted(v for v in variants if v)
        if len(cleaned) > 1:
            out[key] = cleaned
    return out


def _sort_records(records: Iterable[CollectionRecord]) -> List[CollectionRecord]:
    return sorted(
        records,
        key=lambda r: (
            r.group_type,
            r.prefix_group,
            r.title,
            r.handle,
            r.tag,
            ",".join(r.template_keys),
        ),
    )


def _build_menu_tree(dept_records: List[CollectionRecord], product_records: List[CollectionRecord]) -> List[Dict[str, Any]]:
    dept_by_key = {rec.notes.split("department_key=", 1)[-1]: rec for rec in dept_records if "department_key=" in rec.notes}
    children: Dict[str, List[CollectionRecord]] = defaultdict(list)
    for rec in product_records:
        dept_key = ""
        for piece in rec.notes.split(";"):
            piece = piece.strip()
            if piece.startswith("department_key="):
                dept_key = piece.split("=", 1)[1]
                break
        children[dept_key].append(rec)

    nodes = []
    for dept_key, dept_rec in sorted(dept_by_key.items(), key=lambda item: item[1].title.lower()):
        child_records = sorted(children.get(dept_key, []), key=lambda r: r.title.lower())
        nodes.append(
            {
                "label": dept_rec.title,
                "handle": dept_rec.handle,
                "children": [{"label": c.title, "handle": c.handle} for c in child_records],
            }
        )
    return nodes


def audit_taxonomy(template_keys: Set[str], qa_rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    active_rows = [row for row in qa_rows if str(row.get("template_key") or "").strip() in template_keys]

    product_type_map: Dict[Tuple[str, str, str], Set[str]] = defaultdict(set)
    product_type_titles: Dict[str, Set[str]] = defaultdict(set)
    product_type_handles: Dict[str, Set[str]] = defaultdict(set)

    dept_map: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    dept_label_variants: Dict[str, Set[str]] = defaultdict(set)

    manual_map: Dict[str, Set[str]] = defaultdict(set)
    smart_map: Dict[str, Set[str]] = defaultdict(set)

    inferred_fields: List[Dict[str, str]] = []

    for row in active_rows:
        template_key = str(row.get("template_key") or "").strip()
        product_type = str(row.get("product_type") or "").strip()
        dept_key = str(row.get("department_key") or "").strip()
        dept_label = str(row.get("department_label") or "").strip()
        prim_handle = str(row.get("primary_collection_handle") or "").strip()
        prim_title = str(row.get("primary_collection_title") or "").strip()

        if not prim_handle and product_type:
            prim_handle = str(row.get("collection_handle") or "").strip()
            if prim_handle:
                inferred_fields.append({
                    "template_key": template_key,
                    "field": "primary_collection_handle",
                    "inferred_from": "collection_handle",
                    "value": prim_handle,
                })
        if not prim_title and product_type:
            prim_title = str(row.get("collection_title") or "").strip()
            if prim_title:
                inferred_fields.append({
                    "template_key": template_key,
                    "field": "primary_collection_title",
                    "inferred_from": "collection_title",
                    "value": prim_title,
                })

        if product_type:
            product_type_map[(product_type, prim_title, prim_handle)].add(template_key)
            if prim_title:
                product_type_titles[product_type].add(prim_title)
            if prim_handle:
                product_type_handles[product_type].add(prim_handle)

        if dept_key or dept_label:
            dept_map[(dept_key, dept_label)].add(template_key)
            if dept_label:
                dept_label_variants[dept_key].add(dept_label)

        for manual in _split_csv_like(row.get("recommended_manual_collections")):
            manual_map[manual].add(template_key)

        for tag in _split_csv_like(row.get("recommended_smart_collection_tags")):
            smart_map[tag].add(template_key)

    product_records: List[CollectionRecord] = []
    for (product_type, title, handle), template_set in product_type_map.items():
        notes = []
        dept_keys = sorted(
            {
                str(row.get("department_key") or "").strip()
                for row in active_rows
                if str(row.get("template_key") or "").strip() in template_set
            }
        )
        if dept_keys:
            notes.append(f"department_key={dept_keys[0]}")
        if not title:
            notes.append("missing_primary_collection_title")
        if not handle:
            notes.append("missing_primary_collection_handle")
        product_records.append(
            CollectionRecord(
                group_type="product_type",
                source_field="product_type + primary_collection_title + primary_collection_handle",
                template_keys=tuple(sorted(template_set)),
                title=title,
                handle=handle,
                tag="",
                prefix_group="",
                notes="; ".join(notes),
                shopify_collection_type="smart",
            )
        )

    dept_records: List[CollectionRecord] = []
    for (dept_key, dept_label), template_set in dept_map.items():
        dept_title = dept_label
        dept_handle = dept_key
        notes = [f"department_key={dept_key}"] if dept_key else []
        if not dept_title and dept_key:
            dept_title = _title_from_handle(dept_key)
            notes.append("inferred_department_title_from_department_key")
            inferred_fields.append(
                {
                    "template_key": ",".join(sorted(template_set)),
                    "field": "department_label",
                    "inferred_from": "department_key",
                    "value": dept_title,
                }
            )
        if not dept_handle and dept_label:
            dept_handle = dept_label.lower().replace(" ", "-")
            notes.append("inferred_department_handle_from_department_label")
            inferred_fields.append(
                {
                    "template_key": ",".join(sorted(template_set)),
                    "field": "department_key",
                    "inferred_from": "department_label",
                    "value": dept_handle,
                }
            )

        dept_records.append(
            CollectionRecord(
                group_type="department",
                source_field="department_label + department_key",
                template_keys=tuple(sorted(template_set)),
                title=dept_title,
                handle=dept_handle,
                tag="",
                prefix_group="",
                notes="; ".join(notes),
                shopify_collection_type="menu_only",
            )
        )

    manual_records: List[CollectionRecord] = []
    for handle, template_set in manual_map.items():
        title = _title_from_handle(handle)
        notes = "title_inferred_from_handle"
        manual_records.append(
            CollectionRecord(
                group_type="manual_collection",
                source_field="recommended_manual_collections",
                template_keys=tuple(sorted(template_set)),
                title=title,
                handle=handle,
                tag="",
                prefix_group="",
                notes=notes,
                shopify_collection_type="manual",
            )
        )

    smart_records: List[CollectionRecord] = []
    for tag, template_set in smart_map.items():
        prefix = tag.split("-", 1)[0] if "-" in tag else "other"
        smart_records.append(
            CollectionRecord(
                group_type="smart_tag",
                source_field="recommended_smart_collection_tags",
                template_keys=tuple(sorted(template_set)),
                title="",
                handle="",
                tag=tag,
                prefix_group=prefix,
                notes="",
                shopify_collection_type="smart",
            )
        )

    conflicts = {
        "product_type_title_conflicts": _find_conflicts(product_type_titles),
        "product_type_handle_conflicts": _find_conflicts(product_type_handles),
        "department_label_conflicts": _find_conflicts(dept_label_variants),
    }

    menu_tree = _build_menu_tree(dept_records=dept_records, product_records=product_records)

    all_records = _sort_records(product_records + dept_records + manual_records + smart_records)

    return {
        "summary": {
            "active_template_count": len(template_keys),
            "qa_row_count": len(qa_rows),
            "active_qa_row_count": len(active_rows),
            "product_type_group_count": len(product_records),
            "department_group_count": len(dept_records),
            "manual_collection_count": len(manual_records),
            "smart_tag_count": len(smart_records),
        },
        "records": [record.to_row() for record in all_records],
        "menu_tree": menu_tree,
        "conflicts": conflicts,
        "inferred_fields": inferred_fields,
    }


def _format_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    if not rows:
        return "_No rows found._\n"
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        lines.append("| " + " | ".join(str(v) for v in row) + " |")
    return "\n".join(lines) + "\n"


def build_markdown_guide(audit: Dict[str, Any], qa_json_path: Path, templates_path: Path) -> str:
    records = audit["records"]
    summary = audit["summary"]
    by_type: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in records:
        by_type[row["group_type"]].append(row)

    tag_records = sorted(by_type.get("smart_tag", []), key=lambda r: (SMART_PREFIX_ORDER.index(r["prefix_group"]) if r["prefix_group"] in SMART_PREFIX_ORDER else 99, r["prefix_group"], r["tag"]))

    lines: List[str] = []
    lines.append("# Shopify Collection Setup Guide (Audited)")
    lines.append("")
    lines.append("## Executive summary")
    lines.append(f"- Canonical source: `{qa_json_path}` (filtered by active templates from `{templates_path}`).")
    lines.append(f"- Active templates audited: **{summary['active_template_count']}**.")
    lines.append(f"- Active QA rows scanned: **{summary['active_qa_row_count']}** of {summary['qa_row_count']} total rows.")
    lines.append("")
    lines.append("## Active catalog overview")
    lines.append(f"- Product-type collections: **{summary['product_type_group_count']}**")
    lines.append(f"- Department collections: **{summary['department_group_count']}**")
    lines.append(f"- Manual merchandising collections: **{summary['manual_collection_count']}**")
    lines.append(f"- Smart-tag tokens: **{summary['smart_tag_count']}**")
    lines.append("")

    lines.append("## Department collections")
    dept_rows = [
        (r["title"], r["handle"], r["template_keys"], r["notes"])
        for r in sorted(by_type.get("department", []), key=lambda r: (r["title"], r["handle"]))
    ]
    lines.append(_format_table(["Title", "Handle", "Template Keys", "Notes"], dept_rows))

    lines.append("## Product-type collections")
    product_rows = [
        (r["title"], r["handle"], r["source_field"], r["template_keys"], r["notes"])
        for r in sorted(by_type.get("product_type", []), key=lambda r: (r["title"], r["handle"]))
    ]
    lines.append(_format_table(["Title", "Handle", "Source", "Template Keys", "Notes"], product_rows))

    lines.append("## Manual merchandising collections")
    manual_rows = [
        (r["title"], r["handle"], r["template_keys"], r["notes"])
        for r in sorted(by_type.get("manual_collection", []), key=lambda r: r["handle"])
    ]
    lines.append(_format_table(["Title", "Handle", "Template Keys", "Usage Notes"], manual_rows))

    lines.append("## Smart collection tag tokens")
    smart_rows = [(r["tag"], r["prefix_group"], r["template_keys"]) for r in tag_records]
    lines.append(_format_table(["Tag", "Prefix Group", "Template Keys"], smart_rows))

    lines.append("## Recommended navigation tree")
    for node in audit.get("menu_tree", []):
        lines.append(f"- {node['label']} (`{node['handle']}`)")
        for child in node.get("children", []):
            lines.append(f"  - {child['label']} (`{child['handle']}`)")
    lines.append("")

    lines.append("## Step-by-step Shopify admin instructions")
    lines.append("1. **Create smart collections** in Shopify Admin → Products → Collections → Create collection.")
    lines.append("2. Set **Collection type** to **Smart** and add condition: `Product tag` `is equal to` the exact token.")
    lines.append("3. For product-type collections, use `family-*` tokens where present, then assign title/handle exactly as listed above.")
    lines.append("4. **Create manual collections** with the exact title/handle table values.")
    lines.append("5. Add menu links in Shopify Admin → Content → Menus using the navigation tree order.")
    lines.append("6. Optional filters: enable `Product type`, `Availability`, and `Price` in Search & Discovery.")
    lines.append("")

    lines.append("## Copy/paste blocks for exact values")
    lines.append("### Smart tag tokens")
    lines.append("```")
    for row in smart_rows:
        lines.append(row[0])
    lines.append("```")
    lines.append("")
    lines.append("### Department collection handles")
    lines.append("```")
    for row in dept_rows:
        lines.append(row[1])
    lines.append("```")
    lines.append("")
    lines.append("### Product-type collection handles")
    lines.append("```")
    for row in product_rows:
        lines.append(row[1])
    lines.append("```")
    lines.append("")

    lines.append("## Conflicts")
    conflicts = audit.get("conflicts", {})
    if any(conflicts.get(name) for name in conflicts):
        for name, items in conflicts.items():
            if not items:
                continue
            lines.append(f"### {name}")
            for key, variants in sorted(items.items()):
                lines.append(f"- `{key}` has variants: {', '.join(f'`{v}`' for v in variants)}")
    else:
        lines.append("No conflicting spellings found in audited rows.")
    lines.append("")

    inferred = audit.get("inferred_fields", [])
    lines.append("## Inferred fallback fields")
    if inferred:
        for item in inferred:
            lines.append(
                f"- template `{item['template_key']}`: inferred `{item['field']}` from `{item['inferred_from']}` as `{item['value']}`"
            )
    else:
        lines.append("No fallback inference needed.")

    return "\n".join(lines).strip() + "\n"


def write_csv(path: Path, records: Sequence[Dict[str, str]]) -> None:
    fieldnames = [
        "group_type",
        "source_field",
        "template_keys",
        "title",
        "handle",
        "tag",
        "prefix_group",
        "notes",
        "shopify_collection_type",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in records:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def resolve_qa_json_path(explicit_path: Optional[str]) -> Path:
    if explicit_path:
        return Path(explicit_path)
    candidates = [
        Path("exports/storefront_qa_all_active_post_accent_mug.json"),
        Path("storefront_qa_all_active_post_accent_mug.json"),
        Path("exports/storefront_qa.json"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Could not find storefront QA JSON. Pass --qa-json-path explicitly."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit Shopify taxonomy from storefront QA export")
    parser.add_argument("--templates-path", default="product_templates.json")
    parser.add_argument("--qa-json-path", default="")
    parser.add_argument("--output-dir", default="reports")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    templates_path = Path(args.templates_path)
    qa_json_path = resolve_qa_json_path(args.qa_json_path or None)
    output_dir = Path(args.output_dir)

    template_keys = load_active_template_keys(templates_path)
    qa_rows = load_qa_rows(qa_json_path)
    audit = audit_taxonomy(template_keys=template_keys, qa_rows=qa_rows)

    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "shopify_taxonomy_audit.json"
    csv_path = output_dir / "shopify_taxonomy_audit.csv"
    guide_path = output_dir / "shopify_collection_setup_guide.md"

    json_path.write_text(json.dumps(audit, indent=2, ensure_ascii=False), encoding="utf-8")
    write_csv(csv_path, audit["records"])
    guide_path.write_text(build_markdown_guide(audit, qa_json_path=qa_json_path, templates_path=templates_path), encoding="utf-8")

    print(f"Wrote: {json_path}")
    print(f"Wrote: {csv_path}")
    print(f"Wrote: {guide_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
