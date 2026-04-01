# Shopify Taxonomy Normalization (Metadata-Only)

This document describes InkVibeAuto's normalized organization metadata used to make products ready for Shopify menu grouping and smart-collection rules.

## Family keys and primary collections

| Family key | Collection title | Collection handle | Department |
|---|---|---|---|
| tshirt | T-Shirts | t-shirts | apparel |
| long_sleeve | Long Sleeve Shirts | long-sleeve-shirts | apparel |
| hoodie | Hoodies | hoodies | apparel |
| sweatshirt | Sweatshirts | sweatshirts | apparel |
| mug | Mugs | mugs | drinkware |
| poster | Posters | posters | wall-art |
| phone_case | Phone Cases | phone-cases | accessories |
| sticker | Stickers | stickers | accessories |
| tote | Tote Bags | tote-bags | accessories |
| canvas | Canvas Prints | canvas-prints | home-decor |
| blanket | Blankets | blankets | home-decor |

## Department mapping

Normalized department keys:
- apparel
- drinkware
- wall-art
- accessories
- home-decor

## Normalized theme keys

- nature-wildlife
- ocean-coastal
- food-fun
- minimal-bold
- outdoor-vibes
- giftable-art

## Normalized audience keys

- unisex
- men
- women
- youth
- giftable
- home-decor-shoppers
- coffee-lovers
- phone-accessory-shoppers
- sticker-lovers

## Normalized season keys

- spring
- summer
- fall
- winter
- holiday
- evergreen

When no clear seasonal signal is found, `evergreen` is used.

## Final tag format

Normalized taxonomy tags are appended in Shopify-safe hyphen format:
- `family-<family-key>`
- `dept-<department-key>`
- `theme-<theme-key>`
- `audience-<audience-key>`
- `season-<season-key>`

Examples:
- `family-phone-case`
- `dept-accessories`
- `theme-ocean-coastal`
- `audience-giftable`
- `season-evergreen`

## What is ready now

- Deterministic normalized organization metadata in listing context.
- Family-to-collection/department single source of truth.
- Theme/audience/season normalization with safe fallback behavior.
- Stable taxonomy tags merged with existing listing tags and deduped.

## Future work

- Automated Shopify smart-collection creation and mutation.
- Metafield-backed collection and menu orchestration.
- Expanded manual merchandising routing (Featured/New Drops/Best Sellers) beyond metadata-only handles.
