from __future__ import annotations

import json
import pathlib
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, Sequence, Tuple

from PIL import Image, ImageStat


SUPPORTED_ARTWORK_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}


@dataclass
class GeneratedArtworkMetadata:
    title: str
    subtitle: str = ""
    description: str = ""
    tags: List[str] | None = None
    seo_keywords: List[str] | None = None
    audience: str = ""
    style_keywords: List[str] | None = None
    theme: str = ""
    collection: str = ""
    occasion: str = ""
    artist_note: str = ""

    def as_sidecar_dict(self) -> Dict[str, Any]:
        return {
            "title": self.title.strip(),
            "subtitle": self.subtitle.strip(),
            "description": self.description.strip(),
            "tags": _dedupe(self.tags or []),
            "seo_keywords": _dedupe(self.seo_keywords or []),
            "audience": self.audience.strip(),
            "style_keywords": _dedupe(self.style_keywords or []),
            "theme": self.theme.strip(),
            "collection": self.collection.strip(),
            "occasion": self.occasion.strip(),
            "artist_note": self.artist_note.strip(),
        }


@dataclass
class GeneratedArtworkMetadataCandidate:
    image_path: pathlib.Path
    sidecar_path: pathlib.Path
    metadata: GeneratedArtworkMetadata
    generator: str
    rationale: str = ""


class ArtworkMetadataGenerator(Protocol):
    name: str

    def generate_metadata_for_artwork(self, image_path: pathlib.Path) -> GeneratedArtworkMetadataCandidate:
        ...


COLOR_NAMES: Sequence[Tuple[str, Tuple[int, int, int]]] = (
    ("Crimson", (189, 43, 64)),
    ("Amber", (214, 137, 16)),
    ("Gold", (215, 173, 71)),
    ("Emerald", (41, 126, 72)),
    ("Teal", (53, 128, 129)),
    ("Azure", (58, 117, 196)),
    ("Violet", (117, 83, 168)),
    ("Rose", (202, 121, 146)),
    ("Slate", (110, 123, 139)),
    ("Ivory", (224, 219, 205)),
    ("Midnight", (40, 53, 77)),
    ("Charcoal", (68, 68, 72)),
)


class HeuristicArtworkMetadataGenerator:
    """Deterministic local fallback generator based on pixel statistics."""

    name = "heuristic_local"

    def generate_metadata_for_artwork(self, image_path: pathlib.Path) -> GeneratedArtworkMetadataCandidate:
        info = _analyze_image(image_path)
        title = _build_title(info)
        subtitle = _build_subtitle(info)
        description = _build_description(info, title=title)
        tags = _build_tags(info)
        seo_keywords = _build_seo_keywords(info, title=title)
        style_keywords = _build_style_keywords(info)
        audience = _build_audience(info)
        theme = _build_theme(info)

        metadata = GeneratedArtworkMetadata(
            title=title,
            subtitle=subtitle,
            description=description,
            tags=tags,
            seo_keywords=seo_keywords,
            audience=audience,
            style_keywords=style_keywords,
            theme=theme,
            collection=theme,
            occasion=_build_occasion(info),
            artist_note=_build_artist_note(info),
        )
        return GeneratedArtworkMetadataCandidate(
            image_path=image_path,
            sidecar_path=image_path.with_suffix(".json"),
            metadata=metadata,
            generator=self.name,
            rationale=(
                f"Palette={info['palette_name']} mood={info['mood']} contrast={info['contrast_bucket']} "
                f"orientation={info['orientation']}"
            ),
        )


def discover_artwork_images(image_dir: pathlib.Path) -> List[pathlib.Path]:
    return [
        path
        for path in sorted(image_dir.glob("**/*"))
        if path.is_file() and path.suffix.lower() in SUPPORTED_ARTWORK_EXTENSIONS
    ]


def should_write_sidecar(
    sidecar_path: pathlib.Path,
    *,
    overwrite_sidecars: bool,
    only_missing: bool,
) -> bool:
    if overwrite_sidecars:
        return True
    if only_missing:
        return not sidecar_path.exists()
    return True


def write_artwork_sidecar(
    *,
    candidate: GeneratedArtworkMetadataCandidate,
    output_dir: Optional[pathlib.Path] = None,
) -> pathlib.Path:
    destination = _resolve_sidecar_path(candidate.image_path, output_dir)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(candidate.metadata.as_sidecar_dict(), indent=2) + "\n", encoding="utf-8")
    return destination


def preview_generated_metadata(candidates: Sequence[GeneratedArtworkMetadataCandidate]) -> str:
    lines: List[str] = []
    for idx, candidate in enumerate(candidates, start=1):
        payload = candidate.metadata.as_sidecar_dict()
        lines.append(f"[{idx}] {candidate.image_path.name} -> {candidate.sidecar_path.name} ({candidate.generator})")
        lines.append(f"    rationale: {candidate.rationale}")
        lines.append(f"    title: {payload['title']}")
        if payload["subtitle"]:
            lines.append(f"    subtitle: {payload['subtitle']}")
        lines.append(f"    description: {payload['description']}")
        lines.append(f"    tags: {', '.join(payload['tags'])}")
        lines.append(f"    seo_keywords: {', '.join(payload['seo_keywords'])}")
        lines.append(f"    audience: {payload['audience']}")
        lines.append(f"    style_keywords: {', '.join(payload['style_keywords'])}")
        lines.append(f"    theme: {payload['theme']}")
        if payload["occasion"]:
            lines.append(f"    occasion: {payload['occasion']}")
    return "\n".join(lines)


def _resolve_sidecar_path(image_path: pathlib.Path, output_dir: Optional[pathlib.Path]) -> pathlib.Path:
    if not output_dir:
        return image_path.with_suffix(".json")
    return output_dir / f"{image_path.stem}.json"


def _analyze_image(image_path: pathlib.Path) -> Dict[str, Any]:
    with Image.open(image_path) as im:
        rgb = im.convert("RGB")
        stat = ImageStat.Stat(rgb)
        mean = tuple(int(channel) for channel in stat.mean[:3])
        stddev = tuple(float(channel) for channel in stat.stddev[:3])
        orientation = "square"
        if rgb.width > rgb.height:
            orientation = "landscape"
        elif rgb.height > rgb.width:
            orientation = "portrait"
        brightness = sum(mean) / 3
        contrast = sum(stddev) / 3
        saturation = (max(mean) - min(mean)) / 255
        swatches = rgb.resize((96, 96)).quantize(colors=5).convert("RGB").getcolors(96 * 96) or []
        rgb.close()

    ranked_colors = sorted(swatches, key=lambda row: row[0], reverse=True)
    palette_rgb = [color for _, color in ranked_colors[:3]] or [mean]
    palette_names = [_nearest_color_name(color) for color in palette_rgb]
    palette_name = palette_names[0] if palette_names else _nearest_color_name(mean)

    mood = "balanced"
    if brightness >= 175 and saturation >= 0.23:
        mood = "bright"
    elif brightness <= 95:
        mood = "moody"
    elif contrast >= 68:
        mood = "dramatic"

    contrast_bucket = "medium"
    if contrast < 35:
        contrast_bucket = "soft"
    elif contrast > 70:
        contrast_bucket = "bold"

    return {
        "palette_name": palette_name,
        "palette": palette_names,
        "mean": mean,
        "brightness": brightness,
        "contrast": contrast,
        "contrast_bucket": contrast_bucket,
        "saturation": saturation,
        "mood": mood,
        "orientation": orientation,
    }


def _nearest_color_name(color: Tuple[int, int, int]) -> str:
    def distance(item: Tuple[str, Tuple[int, int, int]]) -> float:
        _, anchor = item
        return sum((int(color[i]) - anchor[i]) ** 2 for i in range(3))

    return min(COLOR_NAMES, key=distance)[0]


def _build_theme(info: Dict[str, Any]) -> str:
    mood = info["mood"]
    palette_name = info["palette_name"]
    if mood == "bright":
        return f"{palette_name} Energy"
    if mood == "moody":
        return f"{palette_name} Atmosphere"
    if mood == "dramatic":
        return f"{palette_name} Contrast"
    return f"{palette_name} Balance"


def _build_title(info: Dict[str, Any]) -> str:
    base = _build_theme(info)
    return re.sub(r"\s+", " ", base).strip()


def _build_subtitle(info: Dict[str, Any]) -> str:
    phrases = {
        "bright": "Luminous palette with upbeat motion",
        "moody": "Deep tones with a cinematic pull",
        "dramatic": "High-contrast color story with momentum",
        "balanced": "Clean composition with grounded rhythm",
    }
    return phrases.get(info["mood"], phrases["balanced"])


def _build_description(info: Dict[str, Any], *, title: str) -> str:
    palette = ", ".join(info["palette"][:2]) if info["palette"] else info["palette_name"]
    sentence_a = f"{title} highlights a {info['mood']} visual tone anchored by {palette.lower()} hues."
    sentence_b = (
        f"The {info['contrast_bucket']} contrast profile and {info['orientation']} composition make it easy "
        "to style across multiple spaces and product families."
    )
    return f"{sentence_a} {sentence_b}"


def _build_tags(info: Dict[str, Any]) -> List[str]:
    tags = [
        "wall art",
        "digital artwork",
        info["palette_name"].lower(),
        info["mood"],
        f"{info['contrast_bucket']} contrast",
        f"{info['orientation']} composition",
        "contemporary style",
    ]
    return _dedupe(tags)


def _build_seo_keywords(info: Dict[str, Any], *, title: str) -> List[str]:
    return _dedupe(
        [
            title.lower(),
            f"{info['palette_name'].lower()} artwork",
            f"{info['mood']} wall art",
            "modern digital art print",
            "contemporary home decor art",
        ]
    )


def _build_style_keywords(info: Dict[str, Any]) -> List[str]:
    return _dedupe(["modern", "contemporary", info["contrast_bucket"], info["orientation"]])


def _build_audience(info: Dict[str, Any]) -> str:
    if info["mood"] == "bright":
        return "Color-forward decor fans"
    if info["mood"] == "moody":
        return "Modern and atmospheric art lovers"
    return "Modern art and design enthusiasts"


def _build_occasion(info: Dict[str, Any]) -> str:
    if info["mood"] == "bright":
        return "housewarming"
    if info["mood"] == "moody":
        return "office refresh"
    return ""


def _build_artist_note(info: Dict[str, Any]) -> str:
    return (
        f"Generated from visual analysis of color balance, contrast, and composition. "
        f"Primary palette signal: {info['palette_name']}."
    )


def _dedupe(values: Sequence[str]) -> List[str]:
    seen = set()
    output: List[str] = []
    for value in values:
        cleaned = re.sub(r"\s+", " ", str(value).strip())
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(cleaned)
    return output
