from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class TitleResolution:
    raw_title_source: str
    cleaned_display_title: str
    title_source: str
    quality_reason: str


def _normalize_title_tokens(value: str) -> List[str]:
    cleaned = re.sub(r"\.[a-zA-Z0-9]{2,5}$", "", value).strip()
    cleaned = re.sub(r"[^a-zA-Z0-9]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return [token for token in cleaned.split(" ") if token]


def filename_slug_to_title(value: str) -> str:
    tokens = _normalize_title_tokens(value)
    if not tokens:
        return "Untitled Design"

    normalized_tokens: List[str] = []
    junk_tokens = {"flat", "fpad", "pad", "f8f8f8", "f"}
    for token in tokens:
        t = token.lower()
        if re.fullmatch(r"\d{8,}", t):
            continue
        if re.fullmatch(r"v\d+", t):
            continue
        if re.fullmatch(r"\d{2,4}x\d{2,4}", t) or re.fullmatch(r"\d{2,4}x", t):
            continue
        if re.fullmatch(r"\d{2,4}", t):
            continue
        if t in junk_tokens:
            continue
        normalized_tokens.append(token)

    if not normalized_tokens:
        return "Untitled Design"
    return " ".join(normalized_tokens).strip().title()


def filename_title_quality_reason(value: str) -> str:
    lowered = value.strip().lower()
    if not lowered:
        return "empty"
    if re.fullmatch(r"[a-f0-9]{24,64}", lowered):
        return "hex_like"
    if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", lowered):
        return "uuid_like"
    tokens = _normalize_title_tokens(lowered)
    alpha_tokens = [token for token in tokens if re.search(r"[a-z]", token)]
    if re.search(r"[0-9]{8,}", lowered) and len(alpha_tokens) < 2:
        return "long_numeric"
    normalized = re.sub(r"[_\-]+", "", lowered)
    if len(normalized) <= 4 and len(alpha_tokens) <= 1:
        return "very_short"
    return "usable"


def resolve_artwork_title(template: Any, artwork: Any) -> TitleResolution:
    metadata_title = str(artwork.metadata.get("title", "")).strip()
    if metadata_title:
        return TitleResolution(metadata_title, metadata_title, "metadata", "metadata_title")

    filename_stem = artwork.src_path.stem or artwork.slug
    quality_reason = filename_title_quality_reason(filename_stem)
    cleaned = filename_slug_to_title(filename_stem)
    if quality_reason in {"usable", "very_short"}:
        return TitleResolution(filename_stem, cleaned, "filename", "filename_clean")

    fallback = str(artwork.title or cleaned).strip() or cleaned
    return TitleResolution(filename_stem, fallback, "fallback", quality_reason)


def choose_artwork_display_title(artwork: Any) -> str:
    metadata_title = str(artwork.metadata.get("title", "")).strip()
    if metadata_title:
        return metadata_title
    return filename_slug_to_title(artwork.src_path.stem or artwork.slug)


def build_listing_context(template: Any, artwork: Any, *, overrides: Dict[str, str] | None = None) -> Dict[str, Any]:
    title_info = resolve_artwork_title(template, artwork)
    metadata = artwork.metadata or {}
    context = {
        "artwork_title": title_info.cleaned_display_title,
        "clean_artwork_title": title_info.cleaned_display_title,
        "audience": str(metadata.get("audience") or template.audience or "").strip(),
        "subtitle": str(metadata.get("subtitle", "")).strip(),
        "theme": str(metadata.get("theme", "")).strip(),
        "collection": str(metadata.get("collection", "")).strip(),
        "occasion": str(metadata.get("occasion", "")).strip(),
        "title_source": title_info.title_source,
        "title_quality": title_info.quality_reason,
        "raw_title_source": title_info.raw_title_source,
        "seo_keywords": ", ".join(template.seo_keywords or []),
    }
    if overrides:
        context.update({k: v for k, v in overrides.items() if isinstance(v, str) and v.strip()})
    return context
