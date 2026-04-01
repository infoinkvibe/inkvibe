# Next Profitable Product Wave (Phase 1)

## Added in this patch

- **Activated:** `canvas_basic` (wall-art / home-decor path).
- **Activated:** `tote_basic` (accessories path, primary front placement publish preserved).
- **Added + activated:** `framed_poster_basic`.
- **Added + activated:** `tumbler_20oz_basic`.
- **Added + activated:** `travel_mug_basic`.

## Deferred in this patch

- **Deferred:** `blanket_basic` remains inactive pending stricter viability confirmation.
- **Deferred:** pillow/throw-pillow template was not added in this patch to keep rollout narrow and low-risk.
- **Deferred:** embroidered hats (intentionally out of scope) because embroidery needs a separate, safe workflow and should not reuse full-art print assumptions.

## Artwork and quality caveats

- Canvas and framed poster templates require high-resolution vertical artwork and disable risky upscaling.
- Drinkware launch is intentionally capped to tight variant filters (single size targets) to reduce matrix complexity.
- Tote keeps front-primary publish behavior (`publish_only_primary_placement`) to avoid noisy duplicate placement launches.

## Recommended launch order after this patch

1. Canvas prints
2. Framed posters
3. Tote bags
4. Tumblers + travel mugs
5. Revisit blankets/pillows only after proving artwork-fit and margin thresholds in live preflight checks
6. Revisit hats only with an embroidery-safe, isolated path

## One-time migration note

- Existing runs that load the default active template set will now include the newly activated wave-1 templates.
- Viability guardrails remain in place; templates should still mark nonviable rather than forcing mismatched catalog launches.
