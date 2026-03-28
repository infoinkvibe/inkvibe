from __future__ import annotations

import json
import logging
import mimetypes
import os
import pathlib
import re
import base64
import csv
from collections import Counter
from enum import Enum
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Protocol, Sequence, Tuple

from PIL import Image, ImageStat

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency in constrained test envs.
    load_dotenv = None

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency when OpenAI mode is unused.
    OpenAI = None  # type: ignore[assignment]


SUPPORTED_ARTWORK_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
OPENAI_METADATA_MODEL_DEFAULT = "gpt-4.1-mini"
logger = logging.getLogger("inkvibeauto")


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
    source_signals: List[str] | None = None
    debug_signals: Dict[str, Any] | None = None


@dataclass
class MetadataReviewDecision:
    approval_status: str
    confidence: float
    review_reasons: List[str]
    quality_flags: List[str]
    would_write_sidecar: bool = False


@dataclass
class MetadataReviewRow:
    artwork_filename: str
    proposed_title: str
    subtitle: str
    short_description_preview: str
    tags_preview: str
    detected_subject: str
    visible_text: str
    confidence: float
    generator_mode: str
    provenance_markers: str
    approval_status: str
    review_reasons: str
    quality_flags: str
    would_write_sidecar: bool


class MetadataGeneratorMode(str, Enum):
    HEURISTIC = "heuristic"
    VISION = "vision"
    OPENAI = "openai"
    AUTO = "auto"


class ArtworkMetadataGenerator(Protocol):
    name: str

    def generate_metadata_for_artwork(self, image_path: pathlib.Path) -> GeneratedArtworkMetadataCandidate:
        ...


class VisionAnalyzer(Protocol):
    name: str

    def analyze_image(self, image_path: pathlib.Path) -> Optional["VisionAnalysis"]:
        ...


@dataclass
class VisionAnalysis:
    primary_subject: str = ""
    secondary_subjects: List[str] | None = None
    subject: str = ""
    supporting_subjects: List[str] | None = None
    style_keywords: List[str] | None = None
    mood: str = ""
    palette: List[str] | None = None
    visible_text: List[str] | None = None
    buyer_appeal: List[str] | None = None
    confidence: float = 0.0
    rationale: str = ""

    def resolved_subject(self) -> str:
        return (self.primary_subject or self.subject).strip()

    def resolved_supporting_subjects(self) -> List[str]:
        return _dedupe((self.secondary_subjects or []) + (self.supporting_subjects or []))


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
            source_signals=["heuristic_palette"],
        )


class NullVisionAnalyzer:
    """Default analyzer: signals that no vision backend is configured."""

    name = "unconfigured_vision"

    def analyze_image(self, image_path: pathlib.Path) -> Optional[VisionAnalysis]:
        return None


class LocalVisionAnalyzer:
    """
    Lightweight local analyzer that infers likely subjects from filename and
    visible design text. This keeps the analyzer fully local and dependency-free
    while still providing structured, mockable vision signals.
    """

    name = "local_keyword_vision"

    SUBJECT_SYNONYMS: Dict[str, Sequence[str]] = {
        "lion": ("lion", "leo", "big-cat"),
        "wolf": ("wolf", "wolves", "lupine"),
        "tiger": ("tiger", "tigress"),
        "bear": ("bear", "grizzly", "polar-bear"),
        "eagle": ("eagle", "hawk", "falcon"),
        "owl": ("owl",),
        "fox": ("fox",),
        "deer": ("deer", "stag"),
        "horse": ("horse", "stallion"),
        "dog": ("dog", "puppy", "canine"),
        "cat": ("cat", "kitten", "feline"),
        "mountain": ("mountain", "alpine"),
        "forest": ("forest", "woods", "woodland"),
        "ocean": ("ocean", "sea", "wave"),
        "sunset": ("sunset",),
        "skull": ("skull",),
        "motorcycle": ("motorcycle", "bike", "biker"),
        "car": ("car", "mustang", "muscle-car"),
        "astronaut": ("astronaut", "space", "cosmonaut"),
    }
    STYLE_TOKENS = {
        "retro": "retro",
        "vintage": "vintage",
        "grunge": "grunge",
        "minimal": "minimal",
        "minimalist": "minimal",
        "boho": "boho",
        "abstract": "abstract",
        "line-art": "line art",
        "cyberpunk": "cyberpunk",
    }
    MOOD_TOKENS = {
        "happy": "bright",
        "bright": "bright",
        "sunny": "bright",
        "neon": "dramatic",
        "dramatic": "dramatic",
        "dark": "moody",
        "moody": "moody",
        "calm": "balanced",
        "serene": "balanced",
    }
    _TOKEN_PATTERN = re.compile(r"[a-z0-9]+")

    def analyze_image(self, image_path: pathlib.Path) -> Optional[VisionAnalysis]:
        info = _analyze_image(image_path)
        tokens = self._extract_tokens(image_path=image_path)
        subject_hits = self._subject_hits(tokens)
        if not subject_hits:
            return None
        ranked_subjects = [subject for subject, _ in subject_hits.most_common(3)]
        primary_subject = ranked_subjects[0]
        secondary_subjects = ranked_subjects[1:]
        style_keywords = self._style_keywords(tokens)
        mood = self._mood(tokens=tokens, fallback=info["mood"])
        visible_text = self._extract_visible_text(tokens)
        confidence = min(0.98, 0.55 + (0.16 * subject_hits[primary_subject]) + (0.04 * len(secondary_subjects)))
        rationale = (
            f"subject={primary_subject}; source=filename_or_text; matches={subject_hits[primary_subject]}; "
            f"analyzer={self.name}; confidence={confidence:.2f}"
        )
        return VisionAnalysis(
            primary_subject=primary_subject,
            secondary_subjects=secondary_subjects,
            style_keywords=_dedupe(style_keywords + ["subject-aware"]),
            mood=mood,
            palette=info["palette"],
            visible_text=visible_text,
            buyer_appeal=self._buyer_appeal(primary_subject),
            confidence=confidence,
            rationale=rationale,
        )

    def _extract_tokens(self, *, image_path: pathlib.Path) -> List[str]:
        stem_tokens = self._TOKEN_PATTERN.findall(image_path.stem.lower())
        sidecar = image_path.with_suffix(".json")
        if not sidecar.exists():
            return stem_tokens
        try:
            payload = json.loads(sidecar.read_text(encoding="utf-8"))
        except Exception:
            return stem_tokens
        text_chunks = [payload.get("title", ""), payload.get("description", ""), " ".join(payload.get("tags", []))]
        text_tokens = self._TOKEN_PATTERN.findall(" ".join(str(chunk) for chunk in text_chunks).lower())
        return stem_tokens + text_tokens

    def _subject_hits(self, tokens: Sequence[str]) -> Counter[str]:
        hits: Counter[str] = Counter()
        token_set = set(tokens)
        for subject, synonyms in self.SUBJECT_SYNONYMS.items():
            for token in synonyms:
                if token in token_set:
                    hits[subject] += 1
        return hits

    def _style_keywords(self, tokens: Sequence[str]) -> List[str]:
        return _dedupe([label for token, label in self.STYLE_TOKENS.items() if token in set(tokens)])

    def _mood(self, *, tokens: Sequence[str], fallback: str) -> str:
        for token in tokens:
            if token in self.MOOD_TOKENS:
                return self.MOOD_TOKENS[token]
        return fallback

    def _extract_visible_text(self, tokens: Sequence[str]) -> List[str]:
        meaningful = [token for token in tokens if len(token) >= 4 and token not in self.MOOD_TOKENS]
        return _dedupe(meaningful[:2])

    def _buyer_appeal(self, subject: str) -> List[str]:
        mapping = {
            "lion": ["wildlife", "majestic"],
            "wolf": ["wild", "adventure"],
            "motorcycle": ["adventure", "retro"],
            "astronaut": ["sci-fi", "space"],
        }
        return mapping.get(subject, [subject])


@dataclass
class OpenAiArtworkMetadataResponse:
    main_subject: str
    supporting_subjects: List[str]
    visible_design_text: List[str]
    visual_style: List[str]
    mood: str
    color_story: List[str]
    likely_buyer_appeal: List[str]
    title: str
    subtitle: str
    description: str
    tags: List[str]
    seo_keywords: List[str]
    audience: str
    style_keywords: List[str]
    theme: str
    collection: str
    occasion: str
    artist_note: str

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "OpenAiArtworkMetadataResponse":
        return cls(
            main_subject=_clean_text(payload.get("main_subject", "")),
            supporting_subjects=_clean_list(payload.get("supporting_subjects", [])),
            visible_design_text=_clean_list(payload.get("visible_design_text", [])),
            visual_style=_clean_list(payload.get("visual_style", [])),
            mood=_clean_text(payload.get("mood", "")),
            color_story=_clean_list(payload.get("color_story", [])),
            likely_buyer_appeal=_clean_list(payload.get("likely_buyer_appeal", [])),
            title=_clean_text(payload.get("title", "")),
            subtitle=_clean_text(payload.get("subtitle", "")),
            description=_clean_text(payload.get("description", "")),
            tags=_clean_list(payload.get("tags", [])),
            seo_keywords=_clean_list(payload.get("seo_keywords", [])),
            audience=_clean_text(payload.get("audience", "")),
            style_keywords=_clean_list(payload.get("style_keywords", [])),
            theme=_clean_text(payload.get("theme", "")),
            collection=_clean_text(payload.get("collection", "")),
            occasion=_clean_text(payload.get("occasion", "")),
            artist_note=_clean_text(payload.get("artist_note", "")),
        )

    def to_vision_analysis(self) -> VisionAnalysis:
        return VisionAnalysis(
            primary_subject=self.main_subject,
            secondary_subjects=self.supporting_subjects,
            style_keywords=_dedupe(self.visual_style + self.style_keywords + ["openai-vision"]),
            mood=self.mood,
            palette=self.color_story,
            visible_text=self.visible_design_text,
            buyer_appeal=self.likely_buyer_appeal,
            confidence=0.95,
            rationale="source=openai_responses_api; analyze=image_pixels",
        )

    def to_sidecar_metadata(self) -> GeneratedArtworkMetadata:
        return GeneratedArtworkMetadata(
            title=self.title,
            subtitle=self.subtitle,
            description=self.description,
            tags=self.tags,
            seo_keywords=self.seo_keywords,
            audience=self.audience,
            style_keywords=self.style_keywords,
            theme=self.theme,
            collection=self.collection,
            occasion=self.occasion,
            artist_note=self.artist_note,
        )

    def validate(self) -> None:
        if not self.main_subject:
            raise ValueError("openai metadata missing main_subject")
        if not self.title:
            raise ValueError("openai metadata missing title")
        if not self.description:
            raise ValueError("openai metadata missing description")
        if not self.tags:
            raise ValueError("openai metadata missing tags")


class OpenAiVisionAnalyzer:
    """Vision analyzer powered by OpenAI Responses API image understanding."""

    name = "openai_vision"

    def __init__(
        self,
        *,
        api_key: str = "",
        model: str = "",
        timeout_seconds: float = 30.0,
        client: Any = None,
    ):
        _load_dotenv_if_available()
        self.api_key = (api_key or os.getenv("OPENAI_API_KEY", "")).strip()
        self.model = (model or os.getenv("OPENAI_MODEL", OPENAI_METADATA_MODEL_DEFAULT)).strip()
        self.timeout_seconds = float(timeout_seconds)
        self._client = client
        logger.info(
            "OpenAI metadata analyzer configured model=%s api_key_configured=%s timeout_seconds=%.1f",
            self.model,
            bool(self.api_key),
            self.timeout_seconds,
        )

    def analyze_image(self, image_path: pathlib.Path) -> Optional[VisionAnalysis]:
        response = self.analyze_image_with_metadata(image_path)
        if not response:
            return None
        return response.to_vision_analysis()

    def analyze_image_with_metadata(self, image_path: pathlib.Path) -> Optional[OpenAiArtworkMetadataResponse]:
        if not self.api_key:
            logger.info("OpenAI metadata skipped: OPENAI_API_KEY not configured.")
            return None
        if OpenAI is None and self._client is None:
            logger.warning("OpenAI metadata skipped: openai SDK unavailable.")
            return None
        image_payload = self._build_data_url(image_path)
        prompt = self._build_prompt()
        schema = self._response_schema()
        client = self._client or OpenAI(api_key=self.api_key, timeout=self.timeout_seconds)
        max_attempts = 2
        for attempt in range(max_attempts):
            try:
                resp = client.responses.create(
                    model=self.model,
                    input=[
                        {"role": "system", "content": [{"type": "input_text", "text": prompt}]},
                        {
                            "role": "user",
                            "content": [
                                {"type": "input_text", "text": "Analyze this artwork image and return JSON only."},
                                {"type": "input_image", "image_url": image_payload},
                            ],
                        },
                    ],
                    text={"format": schema},
                )
                payload = self._extract_json_payload(resp)
                parsed = OpenAiArtworkMetadataResponse.from_payload(payload)
                parsed.validate()
                logger.info("OpenAI metadata generation succeeded model=%s image=%s", self.model, image_path.name)
                return parsed
            except Exception as exc:
                safe_error = str(exc).replace("\n", " ")[:240]
                logger.warning(
                    "OpenAI metadata generation failed model=%s image=%s attempt=%s/%s reason=%s",
                    self.model,
                    image_path.name,
                    attempt + 1,
                    max_attempts,
                    safe_error,
                )
        return None

    def _extract_json_payload(self, response: Any) -> Dict[str, Any]:
        output_text = getattr(response, "output_text", "")
        if output_text:
            return json.loads(output_text)
        output = getattr(response, "output", []) or []
        for item in output:
            content = item.get("content", []) if isinstance(item, dict) else getattr(item, "content", [])
            for block in content:
                text = block.get("text") if isinstance(block, dict) else getattr(block, "text", "")
                if text:
                    return json.loads(text)
        raise ValueError("openai response missing parsable JSON output")

    def _build_data_url(self, image_path: pathlib.Path) -> str:
        mime_type = mimetypes.guess_type(str(image_path))[0] or "image/png"
        encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
        return f"data:{mime_type};base64,{encoded}"

    def _build_prompt(self) -> str:
        return (
            "You are generating storefront metadata for a single artwork image. Analyze image pixels only; do not infer from filename. "
            "Return concise, product-agnostic, storefront-safe metadata. "
            "Avoid filler terms like 'AI generated'. Use design text only if truly visible in the artwork. "
            "Avoid speculative details when uncertain. "
            "Return valid JSON matching the schema exactly."
        )

    def _response_schema(self) -> Dict[str, Any]:
        required = [
            "main_subject",
            "supporting_subjects",
            "visible_design_text",
            "visual_style",
            "mood",
            "color_story",
            "likely_buyer_appeal",
            "title",
            "subtitle",
            "description",
            "tags",
            "seo_keywords",
            "audience",
            "style_keywords",
            "theme",
            "collection",
            "occasion",
            "artist_note",
        ]
        return {
            "type": "json_schema",
            "name": "openai_artwork_metadata",
            "strict": True,
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "main_subject": {"type": "string"},
                    "supporting_subjects": {"type": "array", "items": {"type": "string"}},
                    "visible_design_text": {"type": "array", "items": {"type": "string"}},
                    "visual_style": {"type": "array", "items": {"type": "string"}},
                    "mood": {"type": "string"},
                    "color_story": {"type": "array", "items": {"type": "string"}},
                    "likely_buyer_appeal": {"type": "array", "items": {"type": "string"}},
                    "title": {"type": "string"},
                    "subtitle": {"type": "string"},
                    "description": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "seo_keywords": {"type": "array", "items": {"type": "string"}},
                    "audience": {"type": "string"},
                    "style_keywords": {"type": "array", "items": {"type": "string"}},
                    "theme": {"type": "string"},
                    "collection": {"type": "string"},
                    "occasion": {"type": "string"},
                    "artist_note": {"type": "string"},
                },
                "required": required,
            },
        }


class OpenAiArtworkMetadataGenerator:
    """Direct OpenAI-backed metadata generator with strict schema validation."""

    name = "openai_subject"

    def __init__(self, analyzer: Optional[OpenAiVisionAnalyzer] = None):
        self.analyzer = analyzer or OpenAiVisionAnalyzer()

    def generate_metadata_for_artwork(self, image_path: pathlib.Path) -> GeneratedArtworkMetadataCandidate:
        analysis = self.analyzer.analyze_image_with_metadata(image_path)
        if not analysis:
            raise RuntimeError("openai analyzer unavailable or did not return metadata")
        metadata = analysis.to_sidecar_metadata()
        return GeneratedArtworkMetadataCandidate(
            image_path=image_path,
            sidecar_path=image_path.with_suffix(".json"),
            metadata=metadata,
            generator=self.name,
            rationale=f"subject={analysis.main_subject}; model={self.analyzer.model}; analyzer={self.analyzer.name}",
            source_signals=_dedupe(
                [
                    "openai_vision_subject",
                    "openai_vision_text" if analysis.visible_design_text else "",
                    "openai_vision_style" if analysis.visual_style else "",
                ]
            ),
            debug_signals={
                "detected_subject": analysis.main_subject,
                "visible_text": analysis.visible_design_text,
                "confidence": 0.95,
                "model": self.analyzer.model,
            },
        )


class VisionArtworkMetadataGenerator:
    """Subject-aware generator driven by an injectable vision analyzer."""

    name = "vision_subject"

    def __init__(self, analyzer: Optional[VisionAnalyzer] = None):
        self.analyzer = analyzer or NullVisionAnalyzer()

    def generate_metadata_for_artwork(self, image_path: pathlib.Path) -> GeneratedArtworkMetadataCandidate:
        analysis = self.analyzer.analyze_image(image_path)
        if not analysis or not analysis.resolved_subject():
            raise RuntimeError("vision analyzer unavailable or did not return a subject")

        subject = analysis.resolved_subject()
        style_keywords = _dedupe((analysis.style_keywords or []) + ["subject-aware"])
        mood = analysis.mood.strip().lower() or "balanced"
        palette = _dedupe(analysis.palette or [])
        visible_text = _dedupe(analysis.visible_text or [])
        supporting_subjects = analysis.resolved_supporting_subjects()
        buyer_appeal = _dedupe(analysis.buyer_appeal or [])

        title = _build_vision_title(subject=subject, mood=mood, style_keywords=style_keywords, visible_text=visible_text)
        description = _build_vision_description(
            title=title,
            subject=subject,
            supporting_subjects=supporting_subjects,
            mood=mood,
            style_keywords=style_keywords,
            palette=palette,
            buyer_appeal=buyer_appeal,
            visible_text=visible_text,
        )
        tags = _build_vision_tags(
            subject=subject,
            supporting_subjects=supporting_subjects,
            style_keywords=style_keywords,
            mood=mood,
            buyer_appeal=buyer_appeal,
            visible_text=visible_text,
        )
        seo_keywords = _build_vision_seo_keywords(title=title, subject=subject, style_keywords=style_keywords, mood=mood)
        theme = _build_vision_theme(subject=subject, mood=mood, style_keywords=style_keywords)

        metadata = GeneratedArtworkMetadata(
            title=title,
            subtitle=_build_vision_subtitle(subject=subject, mood=mood),
            description=description,
            tags=tags,
            seo_keywords=seo_keywords,
            audience=_build_vision_audience(subject=subject, buyer_appeal=buyer_appeal),
            style_keywords=style_keywords,
            theme=theme,
            collection=theme,
            occasion="",
            artist_note="Generated from subject-aware vision analysis and conservative style inference.",
        )
        subject_signal = "vision_subject"
        text_signal = "vision_text"
        if "local" in self.analyzer.name:
            subject_signal = "local_vision_subject"
            text_signal = "local_vision_text"
        source_signals = [subject_signal]
        if visible_text:
            source_signals.append(text_signal)
        if analysis.rationale:
            source_signals.append("vision_rationale")
        return GeneratedArtworkMetadataCandidate(
            image_path=image_path,
            sidecar_path=image_path.with_suffix(".json"),
            metadata=metadata,
            generator=self.name,
            rationale=analysis.rationale.strip()
            or f"subject={subject}; mood={mood}; analyzer={self.analyzer.name}; confidence={analysis.confidence:.2f}",
            source_signals=source_signals,
            debug_signals={
                "detected_subject": subject,
                "visible_text": visible_text,
                "confidence": round(float(analysis.confidence or 0.0), 3),
            },
        )


class CompositeArtworkMetadataGenerator:
    """Try primary generator first and fall back to heuristic metadata."""

    def __init__(self, primary: ArtworkMetadataGenerator, fallback: ArtworkMetadataGenerator, *, name: str = "auto"):
        self.name = name
        self.primary = primary
        self.fallback = fallback

    def generate_metadata_for_artwork(self, image_path: pathlib.Path) -> GeneratedArtworkMetadataCandidate:
        try:
            candidate = self.primary.generate_metadata_for_artwork(image_path)
            candidate.generator = f"{self.name}:{candidate.generator}"
            return candidate
        except Exception as exc:
            logger.warning(
                "Metadata generator fallback engaged mode=%s image=%s primary=%s fallback=%s reason=%s",
                self.name,
                image_path.name,
                getattr(self.primary, "name", type(self.primary).__name__),
                getattr(self.fallback, "name", type(self.fallback).__name__),
                f"{type(exc).__name__}: {exc}"[:240],
            )
            fallback_candidate = self.fallback.generate_metadata_for_artwork(image_path)
            fallback_candidate.generator = f"{self.name}:{fallback_candidate.generator}"
            rationale = fallback_candidate.rationale
            fallback_candidate.rationale = f"{rationale} | fallback_reason={type(exc).__name__}: {exc}"
            fallback_candidate.source_signals = _dedupe((fallback_candidate.source_signals or []) + ["fallback"])
            return fallback_candidate


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
        if candidate.source_signals:
            lines.append(f"    sources: {', '.join(candidate.source_signals)}")
        if candidate.debug_signals:
            if candidate.debug_signals.get("detected_subject"):
                lines.append(f"    detected_subject: {candidate.debug_signals['detected_subject']}")
            if candidate.debug_signals.get("visible_text"):
                lines.append(f"    visible_text: {', '.join(candidate.debug_signals['visible_text'])}")
            lines.append(f"    confidence: {candidate.debug_signals.get('confidence', 0):.3f}")
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


def should_auto_approve_metadata(decision: MetadataReviewDecision) -> bool:
    return decision.approval_status == "auto_approved"


def evaluate_generated_metadata(
    candidate: GeneratedArtworkMetadataCandidate,
    *,
    min_confidence: float = 0.9,
    max_tags: int = 18,
) -> MetadataReviewDecision:
    payload = candidate.metadata.as_sidecar_dict()
    debug = candidate.debug_signals or {}
    confidence = _safe_float(debug.get("confidence"), default=0.0)
    detected_subject = str(debug.get("detected_subject") or "").strip().lower()
    visible_text = _clean_list(debug.get("visible_text", []))
    title = payload.get("title", "").strip()
    description = payload.get("description", "").strip()
    tags = _dedupe(payload.get("tags", []))
    sources = [str(item).strip().lower() for item in (candidate.source_signals or []) if str(item).strip()]

    review_reasons: List[str] = []
    quality_flags: List[str] = []
    rejection_flags: List[str] = []
    generic_title_phrases = {
        "signature product",
        "signature design",
        "graphic design",
        "digital artwork",
        "art print",
    }
    generic_description_phrases = {
        "perfect for any occasion",
        "great gift idea",
        "style upgrade",
        "versatile piece",
    }
    generic_tags = {
        "art",
        "design",
        "graphic",
        "print",
        "poster",
        "wall art",
        "gift",
        "home decor",
        "creative",
    }

    if not title:
        rejection_flags.append("missing_title")
    if not description:
        rejection_flags.append("missing_description")
    if not tags:
        rejection_flags.append("missing_tags")

    if confidence < float(min_confidence):
        review_reasons.append("confidence_below_threshold")
    if not detected_subject and ("openai" in candidate.generator or "vision" in candidate.generator):
        review_reasons.append("no_detected_subject")
    if title.lower() in generic_title_phrases or "signature product" in title.lower():
        quality_flags.append("title_generic_filler")
    if len(title) > 96:
        quality_flags.append("title_too_long")
    if len(description.split()) < 8:
        quality_flags.append("description_too_vague")
    if len(description) > 420:
        quality_flags.append("description_too_long")
    if any(phrase in description.lower() for phrase in generic_description_phrases):
        quality_flags.append("description_generic_filler")
    if len(tags) > max_tags:
        quality_flags.append("tags_too_many")
    if len(set(term.lower() for term in tags)) != len(tags):
        quality_flags.append("tags_duplicate_terms")
    if tags and all(tag.lower() in generic_tags for tag in tags):
        quality_flags.append("tags_generic_only")
    if detected_subject:
        subject_tokens = [token for token in re.split(r"[^a-z0-9]+", detected_subject) if token]
        haystack = " ".join([title.lower(), description.lower(), " ".join(tag.lower() for tag in tags)])
        if subject_tokens and not any(token in haystack for token in subject_tokens):
            quality_flags.append("subject_signal_mismatch")
    if "fallback" in sources:
        review_reasons.append("fallback_chain_triggered")
    if visible_text and confidence < (float(min_confidence) + 0.05):
        review_reasons.append("visible_text_signal_used_while_uncertain")

    if rejection_flags:
        return MetadataReviewDecision(
            approval_status="rejected",
            confidence=confidence,
            review_reasons=_dedupe(review_reasons + rejection_flags),
            quality_flags=_dedupe(quality_flags + rejection_flags),
        )

    all_reasons = _dedupe(review_reasons + quality_flags)
    if confidence >= float(min_confidence) and not all_reasons:
        return MetadataReviewDecision(
            approval_status="auto_approved",
            confidence=confidence,
            review_reasons=[],
            quality_flags=[],
        )
    return MetadataReviewDecision(
        approval_status="needs_review",
        confidence=confidence,
        review_reasons=all_reasons,
        quality_flags=quality_flags,
    )


def build_metadata_review_row(
    candidate: GeneratedArtworkMetadataCandidate,
    decision: MetadataReviewDecision,
) -> MetadataReviewRow:
    payload = candidate.metadata.as_sidecar_dict()
    debug = candidate.debug_signals or {}
    return MetadataReviewRow(
        artwork_filename=candidate.image_path.name,
        proposed_title=payload.get("title", ""),
        subtitle=payload.get("subtitle", ""),
        short_description_preview=_truncate_text(payload.get("description", ""), 140),
        tags_preview=", ".join(payload.get("tags", [])[:12]),
        detected_subject=str(debug.get("detected_subject", "")).strip(),
        visible_text=", ".join(_clean_list(debug.get("visible_text", []))),
        confidence=round(float(decision.confidence), 3),
        generator_mode=candidate.generator,
        provenance_markers=", ".join(candidate.source_signals or []),
        approval_status=decision.approval_status,
        review_reasons=", ".join(decision.review_reasons),
        quality_flags=", ".join(decision.quality_flags),
        would_write_sidecar=bool(decision.would_write_sidecar),
    )


def export_metadata_review_csv(rows: Sequence[MetadataReviewRow], output_path: pathlib.Path) -> pathlib.Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(asdict(rows[0]).keys()) if rows else list(MetadataReviewRow.__dataclass_fields__.keys())
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))
    return output_path


def export_metadata_review_json(rows: Sequence[MetadataReviewRow], output_path: pathlib.Path) -> pathlib.Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(row) for row in rows]
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return output_path


def select_artwork_metadata_generator(
    *,
    mode: str = MetadataGeneratorMode.HEURISTIC.value,
    vision_analyzer: Optional[VisionAnalyzer] = None,
    openai_analyzer: Optional[OpenAiVisionAnalyzer] = None,
    openai_model: str = "",
    openai_timeout_seconds: float = 30.0,
) -> ArtworkMetadataGenerator:
    normalized = (mode or MetadataGeneratorMode.HEURISTIC.value).strip().lower()
    heuristic = HeuristicArtworkMetadataGenerator()
    vision = VisionArtworkMetadataGenerator(analyzer=vision_analyzer or LocalVisionAnalyzer())
    openai_generator = OpenAiArtworkMetadataGenerator(
        analyzer=openai_analyzer or OpenAiVisionAnalyzer(model=openai_model, timeout_seconds=openai_timeout_seconds)
    )
    openai_with_vision_fallback = CompositeArtworkMetadataGenerator(
        primary=openai_generator,
        fallback=vision,
        name=MetadataGeneratorMode.OPENAI.value,
    )
    if normalized == MetadataGeneratorMode.HEURISTIC.value:
        return heuristic
    if normalized == MetadataGeneratorMode.VISION.value:
        return CompositeArtworkMetadataGenerator(primary=vision, fallback=heuristic, name=MetadataGeneratorMode.VISION.value)
    if normalized == MetadataGeneratorMode.OPENAI.value:
        return CompositeArtworkMetadataGenerator(
            primary=openai_with_vision_fallback,
            fallback=heuristic,
            name=MetadataGeneratorMode.OPENAI.value,
        )
    if normalized == MetadataGeneratorMode.AUTO.value:
        return CompositeArtworkMetadataGenerator(
            primary=openai_with_vision_fallback,
            fallback=heuristic,
            name=MetadataGeneratorMode.AUTO.value,
        )
    raise ValueError(f"unsupported metadata generator mode: {mode}")


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


def _build_vision_title(
    *,
    subject: str,
    mood: str,
    style_keywords: Sequence[str],
    visible_text: Sequence[str],
) -> str:
    canonical_subject = _title_case_phrase(subject)
    if visible_text:
        txt = visible_text[0][:36]
        return _title_case_phrase(f"{canonical_subject} · {txt}")
    if "retro" in {token.lower() for token in style_keywords}:
        return f"Retro {canonical_subject}"
    if mood == "moody":
        return f"{canonical_subject} After Dark"
    if mood == "bright":
        return f"{canonical_subject} in Color"
    return canonical_subject


def _build_vision_subtitle(*, subject: str, mood: str) -> str:
    if mood == "bright":
        return f"Subject-led art with vibrant energy around {subject.lower()}."
    if mood == "moody":
        return f"Atmospheric composition centered on {subject.lower()}."
    return f"Subject-forward artwork featuring {subject.lower()}."


def _build_vision_description(
    *,
    title: str,
    subject: str,
    supporting_subjects: Sequence[str],
    mood: str,
    style_keywords: Sequence[str],
    palette: Sequence[str],
    buyer_appeal: Sequence[str],
    visible_text: Sequence[str],
) -> str:
    style = ", ".join(style_keywords[:2]) if style_keywords else "illustrative"
    palette_text = ", ".join(palette[:3]).lower() if palette else "balanced tones"
    support = ""
    if supporting_subjects:
        support = f" Supporting elements include {', '.join(supporting_subjects[:2]).lower()}."
    text_note = ""
    if visible_text:
        text_note = f" Visible design text is used selectively: \"{visible_text[0][:30]}\"."
    appeal = ""
    if buyer_appeal:
        appeal = f" Great for shoppers looking for {', '.join(buyer_appeal[:2]).lower()} themes."
    return (
        f"{title} focuses on {subject.lower()} with a {mood or 'balanced'} mood and {style} styling. "
        f"The color story leans on {palette_text}.{support}{text_note}{appeal}"
    ).strip()


def _build_vision_tags(
    *,
    subject: str,
    supporting_subjects: Sequence[str],
    style_keywords: Sequence[str],
    mood: str,
    buyer_appeal: Sequence[str],
    visible_text: Sequence[str],
) -> List[str]:
    tags = ["wall art", "digital artwork", subject.lower(), f"{subject.lower()} art", mood, "subject-aware"]
    tags.extend(item.lower() for item in supporting_subjects[:3])
    tags.extend(item.lower() for item in style_keywords[:3])
    tags.extend(item.lower() for item in buyer_appeal[:3])
    if visible_text:
        tags.append(visible_text[0][:24].lower())
    return _dedupe(tags)


def _build_vision_seo_keywords(*, title: str, subject: str, style_keywords: Sequence[str], mood: str) -> List[str]:
    style = style_keywords[0].lower() if style_keywords else "modern"
    return _dedupe(
        [
            f"{subject.lower()} wall art",
            f"{style} {subject.lower()} print",
            f"{mood} {subject.lower()} decor",
            title.lower(),
            "subject aware artwork",
        ]
    )


def _build_vision_theme(*, subject: str, mood: str, style_keywords: Sequence[str]) -> str:
    style = _title_case_phrase(style_keywords[0]) if style_keywords else "Contemporary"
    mood_prefix = _title_case_phrase(mood or "balanced")
    return f"{mood_prefix} {style} { _title_case_phrase(subject)}".strip()


def _build_vision_audience(*, subject: str, buyer_appeal: Sequence[str]) -> str:
    if buyer_appeal:
        return f"Fans of {', '.join(buyer_appeal[:2]).lower()} aesthetics"
    return f"{subject.lower()} and nature-inspired art shoppers"


def _title_case_phrase(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).title()


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _clean_list(value: Any) -> List[str]:
    if isinstance(value, str):
        return _dedupe(token.strip() for token in value.split(","))
    if isinstance(value, (list, tuple, set)):
        return _dedupe(str(item) for item in value)
    return []


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _truncate_text(value: str, limit: int) -> str:
    normalized = re.sub(r"\s+", " ", str(value or "").strip())
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[: max(0, limit - 3)].rstrip()}..."


def _load_dotenv_if_available() -> None:
    if load_dotenv:
        try:
            load_dotenv()
        except Exception:
            return


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
