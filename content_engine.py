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


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    return str(value).strip()


def _as_keywords(value: Any) -> List[str]:
    if isinstance(value, list):
        rows = value
    elif isinstance(value, str):
        rows = re.split(r"[,;]", value)
    else:
        return []
    deduped: List[str] = []
    seen: set[str] = set()
    for row in rows:
        cleaned = _as_text(row)
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(cleaned)
    return deduped


def build_listing_context(template: Any, artwork: Any, *, overrides: Dict[str, str] | None = None) -> Dict[str, Any]:
    title_info = resolve_artwork_title(template, artwork)
    metadata = artwork.metadata or {}
    style_keywords = _as_keywords(metadata.get("style_keywords"))
    seo_keywords = [*getattr(template, "seo_keywords", []), *_as_keywords(metadata.get("seo_keywords"))]
    deduped_seo: List[str] = []
    seo_seen: set[str] = set()
    for keyword in seo_keywords:
        cleaned = _as_text(keyword)
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seo_seen:
            continue
        seo_seen.add(key)
        deduped_seo.append(cleaned)
    context = {
        "artwork_title": title_info.cleaned_display_title,
        "clean_artwork_title": title_info.cleaned_display_title,
        "audience": _as_text(metadata.get("audience") or getattr(template, "audience", "") or ""),
        "subtitle": _as_text(metadata.get("subtitle")),
        "theme": _as_text(metadata.get("theme")),
        "collection": _as_text(metadata.get("collection")),
        "occasion": _as_text(metadata.get("occasion")),
        "color_story": _as_text(metadata.get("color_story")),
        "artist_note": _as_text(metadata.get("artist_note")),
        "style_keywords": ", ".join(style_keywords[:6]),
        "seo_keywords": ", ".join(deduped_seo[:8]),
        "style_keywords_list": style_keywords,
        "seo_keywords_list": deduped_seo,
        "family": infer_product_family(template),
        "family_label": family_title_suffix(template),
        "title_source": title_info.title_source,
        "title_quality": title_info.quality_reason,
        "raw_title_source": title_info.raw_title_source,
    }
    if overrides:
        context.update({k: v for k, v in overrides.items() if isinstance(v, str) and v.strip()})
    return context


_FAMILY_CONFIG: Dict[str, Dict[str, Any]] = {
    "hoodie": {
        "suffix": "Hoodie",
        "tags": ["hoodie", "gift idea", "graphic apparel", "wearable art"],
        "description_closing": "Made to feel expressive, easy to wear, and full of personality.",
    },
    "sweatshirt": {
        "suffix": "Sweatshirt",
        "tags": ["sweatshirt", "gift idea", "graphic apparel", "wearable art"],
        "description_closing": "Built for cozy style with a modern statement look.",
    },
    "long_sleeve": {
        "suffix": "Long Sleeve T-Shirt",
        "tags": ["long sleeve shirt", "gift idea", "graphic apparel", "wearable art"],
        "description_closing": "A clean layer made for everyday style and standout detail.",
    },
    "tshirt": {
        "suffix": "T-Shirt",
        "tags": ["t-shirt", "tee", "short sleeve tee", "gift idea", "graphic apparel"],
        "description_closing": "An easy everyday tee with comfortable feel and standout artwork.",
    },
    "tote": {
        "suffix": "Tote Bag",
        "tags": ["tote bag", "gift idea", "everyday carry"],
        "description_closing": "Great for daily essentials with a bold, art-forward vibe.",
    },
    "poster": {
        "suffix": "Poster",
        "tags": ["poster", "gift idea", "wall art"],
        "description_closing": "A simple way to add personality to your wall and space.",
    },
    "mug": {
        "suffix": "Mug",
        "tags": ["mug", "gift idea", "drinkware"],
        "description_closing": "Designed to bring character to your coffee, tea, or desk setup.",
    },
    "default": {
        "suffix": "Product",
        "tags": ["gift idea", "statement art"],
        "description_closing": "Made for expressive everyday style.",
    },
}


def infer_product_family(template: Any) -> str:
    hint = " ".join(
        [
            str(getattr(template, "key", "") or ""),
            str(getattr(template, "product_type_label", "") or ""),
            str(getattr(template, "shopify_product_type", "") or ""),
        ]
    ).lower()
    if "longsleeve" in hint or "long sleeve" in hint:
        return "long_sleeve"
    if "sweatshirt" in hint or "crewneck" in hint:
        return "sweatshirt"
    if "hoodie" in hint:
        return "hoodie"
    if "tshirt" in hint or "t-shirt" in hint or " tee" in hint:
        return "tshirt"
    if "tote" in hint:
        return "tote"
    if "poster" in hint:
        return "poster"
    if "mug" in hint or "drinkware" in hint:
        return "mug"
    return "default"


def family_title_suffix(template: Any) -> str:
    family = infer_product_family(template)
    return str(_FAMILY_CONFIG.get(family, _FAMILY_CONFIG["default"])["suffix"])


def family_tags(template: Any) -> List[str]:
    family = infer_product_family(template)
    return list(_FAMILY_CONFIG.get(family, _FAMILY_CONFIG["default"])["tags"])


def build_branded_description(*, artwork_title: str, short_description: str, template: Any) -> str:
    family = infer_product_family(template)
    closing = str(_FAMILY_CONFIG.get(family, _FAMILY_CONFIG["default"])["description_closing"]).strip()
    lead = short_description.strip() if short_description.strip() else f"{artwork_title} brings a fresh visual mood with signature InkVibe character."
    return (
        f"<p><strong>{artwork_title}</strong> by InkVibe. {lead}</p>"
        f"<p>{closing}</p>"
    )
