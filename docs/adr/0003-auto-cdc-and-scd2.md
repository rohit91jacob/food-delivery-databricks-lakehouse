# ADR 0003: AUTO CDC for silver; SCD2 only where history is used

**Status:** accepted

## Decision
- Each silver table is a streaming table maintained by `dp.create_auto_cdc_flow`, fed by a temporary view that
  carries the expectations. CDC feeds (restaurants, menu items, customers, riders, promotions) sequence by
  `updated_at` and treat `op = 'DELETE'` as a delete. Fact feeds sequence by `_ingested_at` (last arrival wins).
- **SCD2** applies only to `silver_restaurants` (tracked: price_for_two, commission_rate, name, cuisines, area)
  and `silver_menu_items` (tracked: price, name, category, is_veg). Gold depends on both histories:
  commission is recomputed from the rate in force at placement, and order lines are checked against the menu
  price in force at placement. The weekly rating refresh is untracked, so it updates the current version in
  place and doesn't create history churn.
- Everything else is SCD1.

## Consequences
Point-in-time correctness is verified on every run (`order_lines_priced_from_menu_scd2`,
`orders_join_restaurant_version`, and commission inside the manifest reconciliation).
