from __future__ import annotations

import json
import logging
import os
import pathlib
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency for non-AI runs.
    OpenAI = None  # type: ignore[assignment]


logger = logging.getLogger("inkvibeauto")

SUPPORTED_FAMILIES = {"hoodie", "mug", "tshirt", "sweatshirt", "poster", "phone_case"}
COPY_CACHE_VERSION = "v1"
DEFAULT_COPY_MODEL = "gpt-4.1-mini"
COPY_CACHE_FIELD = "ai_product_copy"

_ANGLE_CHOICES = [
    "cozy_giftable",
    "artistic_expressive",
    "everyday_lifestyle",
    "morning_ritual",
    "desk_home",
    "wearable_everyday_art",
    "cozy_layering",
    "wall_decor_mood",
    "expressive_utility",
]

_FAMILY_COPY_GUIDANCE: Dict[str, Dict[str, Any]] = {
    "hoodie": {
        "prioritize": ["comfort", "layering", "everyday wear", "giftability", "expressive wearable art"],
        "avoid": ["perfect for any occasion", "elevate your style", "must-have", "high-quality design"],
    },
    "mug": {
        "prioritize": ["morning routine", "desk or home use", "cozy giftability", "daily usefulness", "artistic personality"],
        "avoid": ["perfect for any occasion", "elevate your style", "must-have", "high-quality design"],
    },
    "tshirt": {
        "prioritize": ["wearable everyday art", "easy styling", "giftable"],
        "avoid": ["perfect for any occasion", "elevate your style", "must-have", "high-quality design"],
    },
    "sweatshirt": {
        "prioritize": ["comfort", "cozy layering", "casual warmth"],
        "avoid": ["perfect for any occasion", "elevate your style", "must-have", "high-quality design"],
    },
    "poster": {
        "prioritize": ["wall decor", "room mood", "visual centerpiece"],
        "avoid": ["perfect for any occasion", "elevate your style", "must-have", "high-quality design"],
    },
    "phone_case": {
        "prioritize": ["everyday carry", "expressive utility", "giftable accessory"],
        "avoid": ["perfect for any occasion", "elevate your style", "must-have", "high-quality design"],
    },
}


@dataclass
class GeneratedProductCopy:
    title: str
    title_alternatives: List[str]
    short_description: str
    long_description: str
    seo_title: str
    meta_description: str
    tags: List[str]
    chosen_angle: str


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    return str(value).strip()


def _sanitize_text(value: Any, *, max_len: int = 220) -> str:
    text = _as_text(value)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    banned = [
        r"\bmachine wash\b",
        r"\bshipping\b",
        r"\bcompliant\b",
        r"\bperformance\b",
        r"\bmaterial\b",
    ]
    for pattern in banned:
        text = re.sub(pattern, "", text, flags=re.I)
    return text[:max_len].strip(" -,.")


def _sanitize_tags(rows: Any) -> List[str]:
    values = rows if isinstance(rows, list) else str(rows or "").split(",")
    tags: List[str] = []
    seen: set[str] = set()
    for row in values:
        cleaned = re.sub(r"[^a-z0-9\s\-]+", "", _as_text(row).lower()).strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        tags.append(cleaned[:48].strip())
        if len(tags) >= 14:
            break
    return tags


def _build_payload(*, template: Any, artwork: Any, context: Dict[str, Any], family: str) -> Dict[str, Any]:
    metadata = artwork.metadata or {}
    guidance = _FAMILY_COPY_GUIDANCE.get(family, {})
    return {
        "family": family,
        "template_key": _as_text(getattr(template, "key", "")),
        "artwork_slug": _as_text(getattr(artwork, "slug", "")),
        "artwork_title": _as_text(context.get("artwork_title")),
        "subtitle": _as_text(metadata.get("subtitle")),
        "theme": _as_text(metadata.get("theme")),
        "collection": _as_text(metadata.get("collection")),
        "occasion": _as_text(metadata.get("occasion")),
        "audience": _as_text(metadata.get("audience")),
        "color_story": _as_text(metadata.get("color_story")),
        "style_keywords": context.get("style_keywords_list") or [],
        "seo_keywords": context.get("seo_keywords_list") or [],
        "product_type_label": _as_text(getattr(template, "product_type_label", "") or getattr(template, "shopify_product_type", "")),
        "angles": list(_ANGLE_CHOICES),
        "family_copy_guidance": guidance,
    }


def _build_prompt(payload: Dict[str, Any]) -> str:
    return (
        "You are generating ecommerce copy for a print-on-demand listing.\n"
        "Only use facts from INPUT. Never invent material specs, shipping promises, care instructions, "
        "or compliance claims.\n"
        "Tone should be natural and varied (not templated).\n"
        "Avoid awkward tag fragments; each tag should be readable and complete.\n"
        "Return JSON only with keys: "
        "title, title_alternatives, short_description, long_description, seo_title, meta_description, tags, chosen_angle.\n"
        f"INPUT: {json.dumps(payload, ensure_ascii=False)}"
    )


def _extract_json_response(response: Any) -> Dict[str, Any]:
    output_text = _as_text(getattr(response, "output_text", ""))
    if not output_text:
        raise ValueError("responses_api_empty")
    payload = json.loads(output_text)
    if not isinstance(payload, dict):
        raise ValueError("responses_api_non_object")
    return payload


def _validate_generated_copy(raw: Dict[str, Any]) -> GeneratedProductCopy:
    chosen_angle = _sanitize_text(raw.get("chosen_angle"), max_len=64) or "everyday_lifestyle"
    if chosen_angle not in _ANGLE_CHOICES:
        chosen_angle = "everyday_lifestyle"
    alternatives = raw.get("title_alternatives")
    alt_rows = alternatives if isinstance(alternatives, list) else []
    title_alternatives = [_sanitize_text(v, max_len=120) for v in alt_rows if _sanitize_text(v, max_len=120)]
    return GeneratedProductCopy(
        title=_sanitize_text(raw.get("title"), max_len=140),
        title_alternatives=title_alternatives[:4],
        short_description=_sanitize_text(raw.get("short_description"), max_len=280),
        long_description=_sanitize_text(raw.get("long_description"), max_len=900),
        seo_title=_sanitize_text(raw.get("seo_title"), max_len=140),
        meta_description=_sanitize_text(raw.get("meta_description"), max_len=220),
        tags=_sanitize_tags(raw.get("tags")),
        chosen_angle=chosen_angle,
    )


def _cache_key(*, template_key: str, family: str, model: str) -> str:
    return f"{COPY_CACHE_VERSION}:{family}:{template_key}:{model}"


def _load_cached_copy(metadata: Dict[str, Any], *, key: str) -> Optional[GeneratedProductCopy]:
    bucket = metadata.get(COPY_CACHE_FIELD)
    if not isinstance(bucket, dict):
        return None
    entry = bucket.get(key)
    if not isinstance(entry, dict):
        return None
    try:
        return _validate_generated_copy(entry)
    except Exception:
        return None


def _persist_cached_copy(*, artwork_path: pathlib.Path, metadata: Dict[str, Any], key: str, generated: GeneratedProductCopy) -> None:
    sidecar_path = artwork_path.with_suffix(".json")
    raw_sidecar: Dict[str, Any] = {}
    if sidecar_path.exists():
        try:
            payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                raw_sidecar = dict(payload)
        except Exception:
            raw_sidecar = {}
    cache_bucket = raw_sidecar.get(COPY_CACHE_FIELD)
    if not isinstance(cache_bucket, dict):
        cache_bucket = {}
    cache_bucket[key] = {
        **asdict(generated),
        "cache_version": COPY_CACHE_VERSION,
        "model": key.split(":")[-1],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    raw_sidecar[COPY_CACHE_FIELD] = cache_bucket
    sidecar_path.write_text(json.dumps(raw_sidecar, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    metadata[COPY_CACHE_FIELD] = cache_bucket


def maybe_generate_product_copy(
    *,
    template: Any,
    artwork: Any,
    context: Dict[str, Any],
    family: str,
    enabled: bool,
    model: str,
    api_key: str,
    timeout_seconds: float = 25.0,
) -> Optional[GeneratedProductCopy]:
    if family not in SUPPORTED_FAMILIES:
        return None
    if not enabled:
        logger.debug("AI product copy disabled for family=%s", family)
        return None
    if not api_key:
        logger.info("AI product copy fallback: OPENAI_API_KEY missing for %s", getattr(artwork, "slug", "unknown"))
        return None
    if OpenAI is None:
        logger.info("AI product copy fallback: openai package unavailable")
        return None

    resolved_model = (model or os.getenv("OPENAI_MODEL") or DEFAULT_COPY_MODEL).strip()
    key = _cache_key(template_key=_as_text(getattr(template, "key", "")), family=family, model=resolved_model)
    cached = _load_cached_copy(artwork.metadata or {}, key=key)
    if cached:
        logger.info("AI product copy cache hit artwork=%s template=%s", getattr(artwork, "slug", ""), getattr(template, "key", ""))
        return cached

    payload = _build_payload(template=template, artwork=artwork, context=context, family=family)
    prompt = _build_prompt(payload)
    try:
        client = OpenAI(api_key=api_key, timeout=timeout_seconds)
        response = client.responses.create(
            model=resolved_model,
            input=[
                {"role": "system", "content": "Write concise conversion-focused copy while staying factual."},
                {"role": "user", "content": prompt},
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "product_copy_v1",
                    "schema": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "title": {"type": "string"},
                            "title_alternatives": {"type": "array", "items": {"type": "string"}},
                            "short_description": {"type": "string"},
                            "long_description": {"type": "string"},
                            "seo_title": {"type": "string"},
                            "meta_description": {"type": "string"},
                            "tags": {"type": "array", "items": {"type": "string"}},
                            "chosen_angle": {"type": "string"},
                        },
                        "required": [
                            "title",
                            "title_alternatives",
                            "short_description",
                            "long_description",
                            "seo_title",
                            "meta_description",
                            "tags",
                            "chosen_angle",
                        ],
                    },
                    "strict": True,
                }
            },
        )
        generated = _validate_generated_copy(_extract_json_response(response))
        if not generated.title or not generated.long_description:
            logger.info("AI product copy fallback: incomplete response artwork=%s", getattr(artwork, "slug", ""))
            return None
        _persist_cached_copy(artwork_path=artwork.src_path, metadata=artwork.metadata, key=key, generated=generated)
        logger.info("AI product copy cache miss->write artwork=%s template=%s", getattr(artwork, "slug", ""), getattr(template, "key", ""))
        return generated
    except Exception as exc:
        logger.warning(
            "AI product copy fallback due to generation error artwork=%s template=%s err=%s",
            getattr(artwork, "slug", ""),
            getattr(template, "key", ""),
            exc,
        )
        return None
