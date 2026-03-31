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

## Current priorities

Current development priority order:

1. Improve product-page copy for the strongest categories first:
   - hoodies
   - mugs
2. Improve titles, descriptions, tags, and SEO-style listing text so outputs feel less templated.
3. Keep all existing automation behavior intact.
4. Only after merchandising quality improves should more backend automation be expanded.

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

If a task touches one of these, keep the patch minimal and explain the impact clearly.

## Safe improvement areas

These are preferred areas for improvement:

- listing copy generation
- title/description/tag quality
- SEO metadata quality
- copy sanitization
- metadata enrichment
- category-specific merchandising logic
- fallback copy behavior
- caching of generated copy
- QA/reporting visibility
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

If implementing AI-assisted product copy:

- Scope phase 1 to hoodie and mug families only.
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

## Merchandising guidance

For category copy:

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

## Testing expectations

When changing behavior:

- add or update focused tests when a test suite already exists
- do not remove passing tests to make a patch easier
- prefer narrow tests around the changed behavior
- keep existing behavior unchanged outside the scoped feature

If adding AI copy generation, test at minimum:

- disabled AI path falls back cleanly
- missing API key falls back cleanly
- hoodie path can use generated copy
- mug path can use generated copy
- other product families still use existing deterministic copy

## Logging expectations

When adding new behavior:

- log important decisions clearly
- do not add noisy logs for every tiny step
- log fallback activation when AI copy is skipped or fails
- log cache hit vs cache miss for generated copy where practical

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
- broaden AI-generated behavior to all categories in phase 1

## Preferred implementation style for copy improvements

When possible:

- isolate AI copy logic in a separate module
- keep integration points small
- preserve existing function signatures where practical
- store generated results in reusable metadata/cache form
- make rollout reversible with a flag

## Default assumption

If something is ambiguous, choose the safer option that preserves current production behavior.
