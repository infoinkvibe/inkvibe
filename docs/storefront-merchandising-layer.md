# Storefront Merchandising Layer (Post-Launch)

This document describes the additive storefront-cleanup metadata now available for Shopify-facing organization and QA review.

## Live-proven family set (preserved)

- tote
- tumbler
- travel_mug
- canvas
- blanket

## Family to department mapping

- apparel: tshirt, long_sleeve, hoodie, sweatshirt
- drinkware: mug, tumbler, travel_mug
- wall-art: poster, framed_poster
- accessories: tote, phone_case, sticker, embroidered_hat
- home-decor: canvas, blanket, throw_pillow

## Family to product type mapping

Deterministic `family -> recommended_product_type` mapping includes:

- tshirt -> T-Shirts
- hoodie -> Hoodies
- sweatshirt -> Sweatshirts
- mug -> Mugs
- tumbler -> Tumblers
- travel_mug -> Travel Mugs
- canvas -> Canvas Prints
- framed_poster -> Framed Posters
- blanket -> Blankets
- tote -> Tote Bags
- phone_case -> Phone Cases
- sticker -> Stickers

## Family to primary collection mapping

Primary family collection metadata is available from normalized organization fields:

- `primary_collection_title`
- `primary_collection_handle`
- `department_key`
- `department_label`
- `shop_menu_group`

Examples:

- canvas -> `canvas-prints` / Wall Art-style merchandising
- blanket -> `blankets` / Home Decor
- tumbler -> `tumblers` / Drinkware
- travel_mug -> `travel-mugs` / Drinkware
- tote -> `tote-bags` / Accessories

## Manual vs smart collection recommendations

All families now provide additive recommendations:

- `recommended_manual_collections`: Featured, New Drops, Best Sellers
- `recommended_smart_collection_tags`: family/dept/theme/audience/season tags

Smart tags are normalized and deterministic for easy smart-collection rule authoring.

## Shopify-ready fields available now

The normalized storefront organization layer now exposes:

- `recommended_product_type`
- `recommended_shopify_category_label`
- `primary_collection_title`
- `primary_collection_handle`
- `department_key`
- `department_label`
- `shop_menu_group`
- `recommended_manual_collections`
- `recommended_smart_collection_tags`
- `normalized_theme_keys`
- `normalized_audience_keys`
- `normalized_season_keys`

These are included in storefront QA output to support pre-publish audits.

## Future work (still manual)

- Shopify API writes for collections/menus remain out of scope in this patch.
- Menu hierarchy and collection rules still need to be applied in Shopify Admin.
- Category IDs (if required by specific Shopify workflows) can be layered in a future patch once taxonomy governance is finalized.
