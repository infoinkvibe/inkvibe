import json
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Lightweight stubs so tests run without external deps installed in CI sandbox.
sys.modules.setdefault("requests", types.SimpleNamespace(Session=lambda: None))
sys.modules.setdefault("dotenv", types.SimpleNamespace(load_dotenv=lambda: None))
img_stub = types.SimpleNamespace(LANCZOS=1)
sys.modules.setdefault("PIL", types.SimpleNamespace(Image=img_stub, ImageOps=types.SimpleNamespace(exif_transpose=lambda i: i)))
sys.modules.setdefault("PIL.Image", img_stub)
sys.modules.setdefault("PIL.ImageOps", types.SimpleNamespace(exif_transpose=lambda i: i))

import pytest

from printify_shopify_sync_pipeline import (
    TemplateValidationError,
    _compute_backoff,
    ensure_state_shape,
    load_templates,
    save_json_atomic,
)


def test_template_validation_rejects_missing_fields(tmp_path: Path):
    config = tmp_path / "product_templates.json"
    config.write_text(json.dumps([{"key": "t1"}]), encoding="utf-8")
    with pytest.raises(TemplateValidationError):
        load_templates(config)


def test_save_json_atomic_roundtrip(tmp_path: Path):
    path = tmp_path / "state.json"
    data = {"processed": {"a": 1}}
    save_json_atomic(path, data)
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded == data


def test_ensure_state_shape():
    shaped = ensure_state_shape({})
    assert set(["processed", "uploads", "shopify", "printify"]).issubset(shaped.keys())


def test_backoff_increases():
    assert _compute_backoff(3) >= _compute_backoff(1)
