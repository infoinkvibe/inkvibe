# Next Product Wave (Blankets + Throw Pillows, Embroidery-Safe Hats)

## Launched now

- **Activated:** `blanket_basic` with pinned mapping (`blueprint_id=50`, `provider_id=1`) and strict high-resolution gating.
- **Added (deferred activation):** `throw_pillow_basic` as a home-decor-safe template with conservative square cover fit and strict resolution thresholds.

## Deferred in this patch

- **Deferred:** `embroidered_hat_basic` remains intentionally inactive/unimplemented in template rollout.
- Reason: hats require a separate embroidery-safe workflow and should not use full-art print assumptions.
- Current safeguards now support future hat isolation at the family/taxonomy/schema level, but launch is blocked until embroidery-specific artwork validation is wired to runtime eligibility.

## Artwork requirements

### Blankets
- Require large source artwork (`>=6000x4800`) with cover-fit eligibility checks.
- Skip safely when source resolution cannot meet quality thresholds.

### Throw pillows
- Require square-oriented high-resolution artwork (`>=4500x4500`, short edge threshold enabled).
- Use conservative cover-fit behavior and skip safely if source quality is insufficient.

## Why hats are isolated

Embroidery products are production-safe only when artwork passes embroidery-aware constraints (simplified shapes, no tiny text, no thin lines, no distressed detail). Until runtime embroidery compatibility checks are implemented end-to-end, hats stay deferred.

## Recommended next dry/live test order

1. Dry-run `blanket_basic` on 5 high-resolution artworks (preflight + variant viability + publish simulation).
2. Dry-run `throw_pillow_basic` with the same artwork set and confirm safe nonviable behavior when family mapping is unresolved.
3. Limited live launch for blankets first.
4. Limited live launch for throw pillows after mapping confirmation.
5. Add embroidery runtime eligibility checks, then introduce `embroidered_hat_basic` behind a separate opt-in path.
