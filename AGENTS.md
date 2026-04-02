# AGENTS.md

## Purpose

This repository powers the InkVibeAuto print-on-demand pipeline for creating and syncing products to Printify and Shopify.

The top priority is to preserve working production behavior while making targeted improvements. Stability is more important than cleverness. Small safe patches are preferred over broad refactors.

## Core rule

Do not break existing functionality.

If a task can be completed with a narrow change, do that instead of restructuring the codebase.

## What matters most

1. Preserve working product creation, update, rebuild, publish, verification, and sync behavior.
2. Preserve current pricing, margin guardrails, provider/blueprint resolution, upload logic, publish queue behavior, and state handling.
3. Improve customer-facing merchandising quality without destabilizing the pipeline.
4. Prefer deterministic fallbacks when adding AI-assisted behavior.
5. Prioritize correctness of artwork metadata and listing copy over aggressive cache reuse.

## Current validated production baseline

Current validated active baseline (14 templates from `product_templates.json`):

- tshirt_gildan
- longsleeve_gildan
- sweatshirt_gildan
- hoodie_gildan
- mug_new
- tumbler_20oz_basic
- travel_mug_basic
- poster_basic
- framed_poster_basic
- canvas_basic
- phone_case_basic
- tote_basic
- blanket_basic
- sticker_kisscut

Baseline notes:
- `longsleeve_gildan` is intentionally active and aligned with publish-on-create behavior.
- This is now a multi-category baseline (apparel, drinkware, decor, accessories), not the old 7-template set.
- Treat this list as the source-of-truth validated set unless `product_templates.json` changes again.

Expected rerun behavior for this baseline:
- fresh create runs should succeed
- unchanged reruns should mostly skip
- update should happen only when material or intended mutable fields changed
- rebuild should remain fallback only

## Current priorities

Current development priority order:

1. Preserve working create/update/skip/rebuild behavior across the current active template set.
2. Protect metadata provenance correctness: the right artwork must always receive the right listing copy and metadata source.
3. Preserve AI-copy cache identity safety and observability (no cross-artwork cache leakage; clear cache hit/miss/bypass reasons).
4. Preserve storefront QA/export provenance and report schema stability while improving merchandising quality.
5. Continue targeted template metadata quality improvements (SEO/tags/audience/copy quality) in narrow, low-risk patches.
6. Expand coverage only through safe adjacent-family rollout discipline: one new family/template at a time in small PRs with regression checks.
7. Keep collections and broader automation expansion secondary to correctness and stability.

## Current rerun/update priority

The pipeline should not rely on rebuild as the normal rerun path.

Desired rerun behavior:
- If nothing material changed, prefer skip.
- If only mutable listing/product fields changed, prefer update.
- Use rebuild only as a fallback when Printify truly rejects compatibility or editability.

When working on Printify update behavior:
- Preserve existing create success behavior.
- Preserve existing 404 / 8251 / 8252 recovery paths.
- Prefer reducing unnecessary PUT/update calls over broad fallback expansion.
- Treat repeated rebuild-on-rerun behavior as a bug to reduce, not a success state.
- On the first run after introducing new rerun-state metadata, seeding that metadata is acceptable; after that, unchanged reruns should prefer skip/update over rebuild.

## Material-change guidance

When deciding whether a rerun should update:
- Consider blueprint/provider, enabled variants, print-area variant ids, artwork/upload identity, placement transforms, and intended mutable listing fields.
- If those are unchanged, prefer skip.
- Do not rebuild just because the run was repeated.

## Metadata and copy correctness priority

Customer-facing titles, descriptions, tags, SEO fields, and AI-generated copy must always belong to the correct artwork and product family.

When resolving metadata or cached AI copy:
- Prefer exact artwork-sidecar identity over fuzzy matching.
- Do not reuse cached copy across different artworks.
- If metadata candidates are ambiguous, prefer safe fallback over wrong assignment.
- Correctness is more important than cache reuse.
- Avoid silently cross-matching based only on weak slug similarity.
- If identity is unclear, log the ambiguity at a high level and fall back safely.

## Repo change philosophy

When making changes:

- Prefer additive changes over replacing working logic.
- Avoid large rewrites.
- Do not rename major functions or files unless absolutely necessary.
- Do not change CLI behavior unless required for the task.
- If new behavior is optional, gate it behind config/env/CLI flags.
- Keep backward compatibility wherever practical.

## High-risk areas — avoid changing unless explicitly requested

Do not casually modify these areas:

- pricing logic
- margin guardrails
- catalog resolution
- provider/blueprint mapping
- variant filtering
- upload strategy behavior
- publish queue / deferred publish behavior
- state persistence behavior
- Shopify collection sync behavior
- retry / rate-limit handling
- rerun fingerprint logic that is already working for unchanged reruns

If a task touches one of these, keep the patch minimal and explain the impact clearly.

## Safe improvement areas

These are preferred areas for improvement:

- listing copy generation
- title/description/tag quality
- SEO metadata quality
- copy sanitization
- metadata enrichment
- metadata identity validation
- AI copy cache identity validation
- category-specific merchandising logic
- fallback copy behavior
- caching of generated copy
- QA/reporting visibility
- storefront QA/export observability and schema protection
- targeted template metadata quality upgrades (SEO/tags/audience)
- adjacent-family enablement in narrow, one-family rollout PRs
- test coverage for copy-related logic

## Expected architecture behavior

Assume the repository already has working flows for:

- artwork discovery
- artwork metadata loading / sidecars
- content generation helpers
- product template loading
- Printify payload construction
- Shopify sync
- state tracking
- publish queue handling
- run/failure reporting

New features should plug into these flows instead of bypassing them.

## AI-assisted copy generation rules

Current supported AI-copy families:

- hoodie
- mug
- tshirt
- sweatshirt
- poster
- phone_case

Current excluded family:
- sticker

Rollout note:
- `longsleeve_gildan` is active for template execution, but AI-copy support should only be added when explicitly validated in a narrow rollout PR.

If implementing or changing AI-assisted product copy:

- Keep all non-copy business logic deterministic.
- Use AI only for customer-facing copy fields such as:
  - title
  - title alternatives
  - short description
  - long description
  - tags
  - SEO title
  - meta description
  - copy angle
- Do not invent:
  - material claims
  - shipping claims
  - care instructions unless already verified
  - performance or compliance claims
  - unverified product specs
- Always provide deterministic fallback behavior.
- If AI generation fails, the pipeline must still complete successfully using existing copy logic.
- Prefer structured output and validation over free-form text scraping.
- Cache generated copy so repeated runs do not regenerate unnecessarily.
- Cache reuse must be identity-safe for the current artwork and family/template.

## Merchandising guidance

### Hoodies
Prioritize:
- comfort
- layering
- everyday wear
- giftability
- expressive / wearable art

Avoid generic filler like:
- perfect for any occasion
- elevate your style
- must-have
- high-quality design

### Mugs
Prioritize:
- morning routine
- desk / home use
- cozy giftability
- simple daily usefulness
- artistic personality

Avoid generic filler and repetitive phrasing.

### T-Shirts
Prioritize:
- wearable everyday art
- easy styling
- giftability
- simple expressive design
- casual versatility

### Sweatshirts
Prioritize:
- comfort
- cozy layering
- casual warmth
- relaxed daily wear
- giftability

### Posters
Prioritize:
- wall decor
- room mood
- visual centerpiece
- giftable art
- simple display value

### Phone Cases
Prioritize:
- expressive everyday utility
- giftability
- visual personality
- practical carry item
- accessory-style phrasing

### Stickers
Sticker economics and priority remain weaker than the core categories.
Do not spend disproportionate effort on sticker merchandising unless explicitly requested.

## Validation priority

For rerun-related patches, validate:
- first run create behavior remains intact
- rerun of unchanged artwork/templates mostly skips or updates across the active baseline
- rebuild remains available only for genuine incompatibility/edit-lock fallback

For metadata/copy correctness patches, validate:
- exact sidecar match wins over fuzzy/fallback matching
- ambiguous slug/fallback matching does not cross-assign wrong metadata
- AI copy cache does not leak across artworks
- cache identity mismatch paths are observable and safe (bypass + fallback behavior)
- correct artwork/template cache reuse still works
- storefront QA/export provenance fields remain backward-compatible
- no regression to existing rerun skip behavior
- adjacent-family rollout changes are isolated and verified one family/template at a time

## Testing expectations

When changing behavior:

- add or update focused tests when a test suite already exists
- do not remove passing tests to make a patch easier
- prefer narrow tests around the changed behavior
- keep existing behavior unchanged outside the scoped feature

If changing AI copy or metadata/copy resolution, test at minimum:

- disabled AI path falls back cleanly
- missing API key falls back cleanly
- supported family path can use generated copy
- unsupported families still use deterministic copy
- exact sidecar match wins
- ambiguous metadata does not cross-assign
- AI copy cache does not leak across artworks
- unchanged reruns still skip on the current baseline

## Logging expectations

When adding new behavior:

- log important decisions clearly
- do not add noisy logs for every tiny step
- log fallback activation when AI copy is skipped or fails
- log cache hit vs cache miss for generated copy where practical
- log metadata source chosen at a high level
- log cache bypass reason when identity validation fails
- avoid giant payload dumps unless debug-only

## File and code style expectations

- Keep functions small and readable.
- Prefer explicit names over clever abstractions.
- Reuse existing helpers and patterns where possible.
- Keep imports tidy.
- Avoid introducing new dependencies unless necessary.
- If adding a dependency, explain why it is needed.

## Deliverable expectations for code changes

When completing a task, provide:

1. a short summary of what changed
2. the files changed
3. any new env vars, config, or CLI flags
4. fallback behavior details
5. any follow-up recommendations for the next phase

## What not to do

Do not:

- refactor unrelated modules
- redesign the whole pipeline
- switch core API providers without being asked
- remove current fallback logic
- silently change pricing behavior
- silently change publish behavior
- silently change template selection behavior
- silently broaden fuzzy metadata matching
- silently reuse cached copy across different artworks

## Preferred implementation style for copy improvements

When possible:

- isolate AI copy logic in a separate module
- keep integration points small
- preserve existing function signatures where practical
- store generated results in reusable metadata/cache form
- make rollout reversible with a flag

## Default assumption

If something is ambiguous, choose the safer option that preserves current production behavior and favors correctness over convenience.
