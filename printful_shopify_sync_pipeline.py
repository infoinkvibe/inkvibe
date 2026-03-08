"""Backward-compatible wrapper for the renamed Printify pipeline."""

from pathlib import Path
from printify_shopify_sync_pipeline import run, TEMPLATES_CONFIG


if __name__ == "__main__":
    run(Path(TEMPLATES_CONFIG))
