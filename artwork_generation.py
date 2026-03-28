from __future__ import annotations

import base64
import logging
import os
import pathlib
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore[assignment]

logger = logging.getLogger("inkvibeauto")

PREVIEW_NAME_PATTERN = re.compile(r"(removebg-preview|preview|thumbnail)", re.IGNORECASE)


@dataclass
class ArtworkGenerationRequest:
    prompt: str
    count: int = 1
    style: str = ""
    negative_prompt: str = ""
    visible_text: str = ""
    quality: str = "high"
    background: str = "auto"
    generator: str = "openai"
    openai_model: str = ""
    base_name: str = "generated-art"
    output_dir: pathlib.Path = pathlib.Path("./images")
    target_mode: str = "auto"
    dry_run_plan: bool = False
    skip_existing_generated: bool = False
    min_source_width: int = 1024
    min_source_height: int = 1024
    family_aware: bool = False
    family_mode: str = "auto"
    generate_poster_master: bool = False
    generate_apparel_master: bool = False
    mug_tote_master: str = "apparel"
    apparel_style: str = ""
    poster_style: str = ""


@dataclass
class ArtworkGenerationTarget:
    mode: str
    label: str
    openai_size: str
    family: str = "single"


@dataclass
class ArtworkGenerationPlan:
    targets: List[ArtworkGenerationTarget]
    template_keys: List[str]
    rationale: List[str] = field(default_factory=list)
    family_aware: bool = False
    template_family_map: Dict[str, str] = field(default_factory=dict)


@dataclass
class GeneratedArtworkAsset:
    path: pathlib.Path
    mode: str
    concept_index: int
    width: int = 0
    height: int = 0
    source: str = "openai"
    skipped_reason: str = ""
    family: str = "single"


@dataclass
class FamilyArtworkPlan:
    family: str
    mode: str
    openai_size: str
    label: str


@dataclass
class FamilyGeneratedAsset:
    path: pathlib.Path
    family: str
    mode: str
    concept_index: int


@dataclass
class TemplateAssetRouting:
    template_key: str
    family: str
    concept_index: int
    asset_path: pathlib.Path


APPAREL_FAMILY = "apparel"
POSTER_FAMILY = "poster"
SQUARE_FAMILY = "square"


def classify_template_family(template_key: str, *, mug_tote_master: str = "apparel") -> str:
    key = (template_key or "").strip().lower()
    if "poster" in key:
        return POSTER_FAMILY
    if "mug" in key or "tote" in key:
        if mug_tote_master == "square":
            return SQUARE_FAMILY
        return APPAREL_FAMILY
    return APPAREL_FAMILY


def plan_family_artwork_targets(
    *,
    template_keys: Sequence[str],
    family_mode: str = "auto",
    generate_poster_master: bool = False,
    generate_apparel_master: bool = False,
    mug_tote_master: str = "apparel",
) -> ArtworkGenerationPlan:
    normalized_mode = (family_mode or "auto").strip().lower()
    template_family_map = {
        key: classify_template_family(key, mug_tote_master=mug_tote_master)
        for key in template_keys
    }
    families_in_templates = set(template_family_map.values())
    required_families: List[str] = []
    rationale: List[str] = []

    if normalized_mode == "single":
        required_families = [APPAREL_FAMILY]
        rationale.append("Family-aware single mode enabled; generating apparel master only.")
    elif normalized_mode == "split":
        required_families = [APPAREL_FAMILY, POSTER_FAMILY]
        if SQUARE_FAMILY in families_in_templates:
            required_families.append(SQUARE_FAMILY)
        rationale.append("Family-aware split mode enabled; generating dedicated family masters.")
    else:
        if POSTER_FAMILY in families_in_templates:
            required_families.append(POSTER_FAMILY)
        if APPAREL_FAMILY in families_in_templates:
            required_families.append(APPAREL_FAMILY)
        if SQUARE_FAMILY in families_in_templates:
            required_families.append(SQUARE_FAMILY)
        if not required_families:
            required_families.append(APPAREL_FAMILY)
        rationale.append("Family-aware auto mode selected family masters from template mix.")

    if generate_apparel_master and APPAREL_FAMILY not in required_families:
        required_families.append(APPAREL_FAMILY)
        rationale.append("Forced apparel master via CLI override.")
    if generate_poster_master and POSTER_FAMILY not in required_families:
        required_families.append(POSTER_FAMILY)
        rationale.append("Forced poster master via CLI override.")

    size_by_mode = {"portrait": "1024x1536", "square": "1024x1024"}
    mode_by_family = {
        APPAREL_FAMILY: "portrait",
        POSTER_FAMILY: "portrait",
        SQUARE_FAMILY: "square",
    }
    targets = [
        ArtworkGenerationTarget(
            mode=mode_by_family[family],
            label=f"{family}_master",
            openai_size=size_by_mode[mode_by_family[family]],
            family=family,
        )
        for family in required_families
    ]
    return ArtworkGenerationPlan(
        targets=targets,
        template_keys=list(template_keys),
        rationale=rationale,
        family_aware=True,
        template_family_map=template_family_map,
    )


def _load_dotenv_if_available() -> None:
    if load_dotenv is not None:
        try:
            load_dotenv()
        except Exception:
            return


def choose_generation_aspect_modes(*, template_keys: Sequence[str], target_mode: str = "auto") -> List[str]:
    requested = (target_mode or "auto").strip().lower()
    if requested in {"portrait", "square"}:
        return [requested]
    if requested == "multi":
        return ["portrait", "square"]

    portrait_tokens = ("hoodie", "sweatshirt", "longsleeve", "long-sleeve", "poster", "shirt", "tee")
    square_tokens = ("mug", "tote", "sticker", "coaster", "square")
    portrait_hits = 0
    square_hits = 0
    for key in template_keys:
        lowered = key.lower()
        if any(token in lowered for token in portrait_tokens):
            portrait_hits += 1
        if any(token in lowered for token in square_tokens):
            square_hits += 1

    if portrait_hits and square_hits:
        return ["portrait", "square"]
    if square_hits and not portrait_hits:
        return ["square"]
    return ["portrait"]


def plan_generated_artwork_targets(*, template_keys: Sequence[str], target_mode: str = "auto") -> ArtworkGenerationPlan:
    modes = choose_generation_aspect_modes(template_keys=template_keys, target_mode=target_mode)
    size_by_mode = {
        "portrait": "1024x1536",
        "square": "1024x1024",
    }
    rationale: List[str] = []
    if len(modes) > 1:
        rationale.append("Mixed template families detected; generating portrait and square source masters.")
    elif modes[0] == "portrait":
        rationale.append("Portrait-heavy template mix detected; prioritizing portrait source generation.")
    else:
        rationale.append("Square-heavy template mix detected; prioritizing square source generation.")

    targets = [ArtworkGenerationTarget(mode=mode, label=f"{mode}_master", openai_size=size_by_mode[mode], family="single") for mode in modes]
    return ArtworkGenerationPlan(targets=targets, template_keys=list(template_keys), rationale=rationale)


def build_generation_prompt(request: ArtworkGenerationRequest, *, mode: str, family: str = "single") -> str:
    orientation = "portrait orientation" if mode == "portrait" else "square orientation"
    chunks = [
        "Create standalone print-ready artwork only (not a product mockup, not a scene).",
        f"Composition: {orientation}, centered primary subject, strong subject fill, clean margins.",
        "Avoid tiny isolated subjects on large blank space unless explicitly requested.",
        "No watermark, no signature, no border, and no product photo.",
        "High visual clarity, production-safe framing for print-on-demand.",
        f"Core concept: {request.prompt.strip()}",
    ]
    if request.style.strip():
        chunks.append(f"Style direction: {request.style.strip()}")
    if request.visible_text.strip():
        chunks.append(f"Include visible text exactly: {request.visible_text.strip()}")
    else:
        chunks.append("Do not add any text in the artwork.")
    if request.negative_prompt.strip():
        chunks.append(f"Avoid: {request.negative_prompt.strip()}")
    if family == APPAREL_FAMILY:
        chunks.extend([
            "Output as isolated standalone graphic optimized for garment printing.",
            "Keep transparent or very clean background; no frame and no poster rectangle.",
            "No scenic full-bleed background; avoid wall-art framing.",
        ])
        if request.apparel_style.strip():
            chunks.append(f"Apparel style guidance: {request.apparel_style.strip()}")
    elif family == POSTER_FAMILY:
        chunks.extend([
            "Output as rich scenic wall-art composition with full-background coverage.",
            "Poster-like framing and cinematic detail are encouraged; avoid clipart look.",
        ])
        if request.poster_style.strip():
            chunks.append(f"Poster style guidance: {request.poster_style.strip()}")
    return " ".join(chunks)


def _openai_image_client(*, api_key: str = "", timeout_seconds: float = 45.0, client: Any = None) -> Any:
    if client is not None:
        return client
    _load_dotenv_if_available()
    resolved_key = (api_key or os.getenv("OPENAI_API_KEY", "")).strip()
    if not resolved_key:
        raise RuntimeError("OPENAI_API_KEY is required for prompt artwork generation")
    if OpenAI is None:
        raise RuntimeError("openai SDK is unavailable; install openai package to use generation mode")
    return OpenAI(api_key=resolved_key, timeout=timeout_seconds)


def generate_artwork_with_openai(
    *,
    request: ArtworkGenerationRequest,
    plan: ArtworkGenerationPlan,
    client: Any = None,
) -> List[GeneratedArtworkAsset]:
    if request.generator != "openai":
        raise RuntimeError(f"Unsupported art generator: {request.generator}")

    resolved_model = (request.openai_model or os.getenv("OPENAI_IMAGE_MODEL", "gpt-image-1")).strip()
    image_client = _openai_image_client(client=client)
    assets: List[GeneratedArtworkAsset] = []
    request.output_dir.mkdir(parents=True, exist_ok=True)

    for concept_index in range(1, max(1, int(request.count)) + 1):
        for target in plan.targets:
            suffix_token = target.family if (target.family and target.family != "single") else target.mode
            suffix = f"{suffix_token}-c{concept_index:02d}"
            output_name = f"{request.base_name}-{suffix}.png"
            output_path = request.output_dir / output_name
            if request.skip_existing_generated and output_path.exists():
                assets.append(
                    GeneratedArtworkAsset(path=output_path, mode=target.mode, concept_index=concept_index, source="existing")
                )
                continue
            prompt = build_generation_prompt(request, mode=target.mode, family=target.family)
            response = image_client.images.generate(
                model=resolved_model,
                prompt=prompt,
                size=target.openai_size,
                quality=request.quality,
                background=request.background,
            )
            data = getattr(response, "data", []) or []
            if not data:
                raise RuntimeError("OpenAI image generation returned no images")
            image_b64 = data[0].b64_json if hasattr(data[0], "b64_json") else data[0].get("b64_json")
            if not image_b64:
                raise RuntimeError("OpenAI image generation did not return image bytes")
            save_generated_artwork(base64.b64decode(image_b64), output_path)
            assets.append(GeneratedArtworkAsset(path=output_path, mode=target.mode, concept_index=concept_index, source="openai", family=target.family))
    return assets


def save_generated_artwork(image_bytes: bytes, output_path: pathlib.Path) -> pathlib.Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(image_bytes)
    return output_path


def is_preview_or_low_value_asset(path: pathlib.Path) -> bool:
    return bool(PREVIEW_NAME_PATTERN.search(path.name))


def choose_preferred_generated_asset(assets: Iterable[GeneratedArtworkAsset]) -> List[GeneratedArtworkAsset]:
    grouped: Dict[tuple[int, str, str], List[GeneratedArtworkAsset]] = {}
    for asset in assets:
        key = (asset.concept_index, asset.mode, asset.family)
        grouped.setdefault(key, []).append(asset)
    preferred: List[GeneratedArtworkAsset] = []
    for group_assets in grouped.values():
        preferred.append(max(group_assets, key=lambda item: int(item.width or 0) * int(item.height or 0)))
    return preferred


def route_templates_to_generated_assets(
    *,
    template_keys: Sequence[str],
    assets: Sequence[GeneratedArtworkAsset],
    template_family_map: Optional[Dict[str, str]] = None,
    mug_tote_master: str = "apparel",
) -> List[TemplateAssetRouting]:
    by_family_concept: Dict[tuple[str, int], GeneratedArtworkAsset] = {}
    for asset in assets:
        by_family_concept[(asset.family or "single", int(asset.concept_index))] = asset
    routing: List[TemplateAssetRouting] = []
    concepts = sorted({int(asset.concept_index) for asset in assets}) or [1]
    for concept_index in concepts:
        for template_key in template_keys:
            family = (template_family_map or {}).get(template_key) or classify_template_family(
                template_key,
                mug_tote_master=mug_tote_master,
            )
            candidate = by_family_concept.get((family, concept_index))
            if candidate is None:
                candidate = by_family_concept.get((APPAREL_FAMILY, concept_index))
            if candidate is None:
                candidate = by_family_concept.get(("single", concept_index))
            if candidate is None:
                continue
            routing.append(
                TemplateAssetRouting(
                    template_key=template_key,
                    family=family,
                    concept_index=concept_index,
                    asset_path=candidate.path,
                )
            )
    return routing


def validate_generated_asset_for_templates(
    asset: GeneratedArtworkAsset,
    *,
    min_width: int,
    min_height: int,
) -> Optional[str]:
    if is_preview_or_low_value_asset(asset.path):
        return "preview_or_thumbnail_name"
    if asset.width < int(min_width) or asset.height < int(min_height):
        return f"tiny_source_{asset.width}x{asset.height}_below_{min_width}x{min_height}"
    return None
