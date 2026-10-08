# Data dictionary

All tables live in one Unity Catalog schema per target: `workspace.fooddelivery` in prod, and
`workspace.dev_<user>_fooddelivery` in dev (the bundle prefixes it). A name prefix marks each table's layer.
**`business_date`** is the order placement date in Asia/Kolkata. Every feed is partitioned and reconciled by it.

| Layer | Prefix | Built by | Mutability |
|---|---|---|---|
| Landing | `/Volumes/<cat>/<schema>/landing/<feed>/dt=YYYY-MM-DD/part-00000-v<N>.json.gz` | Airflow (`upload_batch`) | One file per feed/date/generator version |
| Bronze | `bronze_<feed>` | Auto Loader streaming tables | Append-only, raw, plus `_rescued_data`, `_source_file`, `_ingested_at` |
| Quarantine | `quarantine_<feed>` | Streaming tables | Rows that broke a drop rule, plus `_failed_rules`, `_quarantined_at` |
| Silver | `silver_<feed>` | `create_auto_cdc_flow` (SCD1 / SCD2) | Typed, validated, de-duplicated |
| Gold | `fct_*`, `gold_*` | Materialized views | Recomputed (incrementally where possible) |
| Ops | `dq_results` | Job task `data_quality` (`fd-dq`) | Append-only audit of every check run |

## Raw feeds (landing to bronze)

Explicit schemas live in `src/fooddelivery/transforms/schemas.py`. Every record carries `business_date` and
`record_version` (the generator version).

| Feed | Grain / natural key | Notes |
|---|---|---|
| `restaurants` | `restaurant_id` + `updated_at` (CDC: `op` = UPSERT/DELETE) | Day 0 is a full snapshot; then onboarding, price-for-two and commission changes, Monday rating refreshes, and churn (DELETE) |
| `menu_items` | `item_id` + `updated_at` (CDC) | Day-0 snapshot; then price changes (mostly 04:00-06:00, some intraday), new dishes, removals |
| `customers` | `customer_id` + `updated_at` (CDC) | Day-0 snapshot of the existing base; then daily sign-ups and address moves |
| `riders` | `rider_id` + `updated_at` (CDC) | Joins and attrition (`status` inactive) |
| `promotions` | `promo_code` + `updated_at` (CDC) | National promo catalog |
| `city_conditions` | `city`, `hour_start` | Hourly weather, rain mm, traffic level, riders on shift, surge multiplier |
| `rider_shifts` | `shift_id` | Planned log-in windows per rider and day |
| `orders` | `order_id` | Header with nested `items[]` (line_no, item_id, item_name, quantity, unit_price). **Contains injected exact duplicates and corrupt `X…` records on purpose** |
| `order_events` | `event_id` | placed → accepted → rider_assigned → preparing → ready → picked_up → delivered, or cancelled (`reason`, `actor`). Contains injected duplicates |
| `payments` | `payment_id` | One per order: captured / failed / voided (COD cancelled) |
| `refunds` | `refund_id` | order_cancelled, late_delivery, missing_or_damaged_item; `funded_by` platform/restaurant |
| `ratings` | `order_id` | Food and delivery rating (1-5) for about 45% of delivered orders |
| `rider_earnings` | `earning_id` | base + distance + surge + tip, or cancellation pay |
| `rider_locations` | `ping_id` | GPS pings every `FD_GPS_PING_SECONDS` while on an order (to_restaurant / at_restaurant / to_customer) |
| `manifests` (`_manifests/`) | `business_date`, `city` | Generator ground truth: counts and money totals used for reconciliation |

## Silver

| Table | Keys | SCD | Sequence | Tracked history | Drop rules (quarantined) |
|---|---|---|---|---|---|
| `silver_restaurants` | restaurant_id | 2 | updated_at | price_for_two, commission_rate, name, cuisines, area (rating updates in place) | id present, timestamp parsed, op valid |
| `silver_menu_items` | item_id | 2 | updated_at | price, name, category, is_veg | id present, timestamp parsed, op valid |
| `silver_customers` / `silver_riders` / `silver_promotions` | natural key | 1 | updated_at | n/a | key present |
| `silver_orders` | order_id | 1 | _ingested_at | n/a | ids present, placed_at parsed, has items, total ≥ 0, subtotal = Σ lines, total = subtotal − discount + fees + tax + tip |
| `silver_order_items` | order_id, line_no | 1 | _ingested_at | n/a | Exploded from **valid** orders only; quantity > 0, unit_price > 0 |
| `silver_order_events` | event_id | 1 | _ingested_at | n/a | known status, timestamp parsed |
| `silver_payments` / `silver_refunds` / `silver_ratings` / `silver_rider_earnings` / `silver_rider_locations` / `silver_city_conditions` / `silver_rider_shifts` / `silver_manifests` | natural key | 1 | _ingested_at | n/a | see `SPECS` in `transforms/silver.py` |

SCD2 tables expose `__START_AT` / `__END_AT` (the current version has `__END_AT IS NULL`). Point-in-time joins
use `__START_AT <= ts < coalesce(__END_AT, +inf)`.

## Gold

| Table | Grain | Highlights |
|---|---|---|
| `fct_orders` | order_id | Lifecycle timestamps, `order_status`, cancellation reason/actor, money, **commission from the SCD2 rate in force at placement**, promo funding split, refunds, rider pay, ratings, durations (accept, assignment, prep, pickup wait, travel, delivery), `is_late`, `eta_error_minutes`, `contribution_margin` |
| `fct_order_items` | order_id, line_no | Category, cuisine, veg flag and **menu price as-of placement** (`price_matches_menu`) |
| `gold_daily_kpis` | business_date, city | Orders, deliveries, cancellations by reason, late deliveries, GMV, AOV, fees, taxes, tips, commission, refunds, rider payout, contribution margin, on-time and cancellation rates. **Reconciles 1:1 with the manifest** |
| `gold_delivery_sla_hourly` | business_date, city, placed_hour, weather, traffic_level | On-time rate, p50/p90 delivery minutes, ETA MAE and bias, average prep/travel/assignment |
| `gold_restaurant_daily` | business_date, restaurant_id | Acceptance rate, prep time avg/p90, food rating, food revenue, commission, restaurant-funded discounts |
| `gold_rider_daily` | business_date, rider_id | Shift minutes, busy minutes, **utilisation**, deliveries, delivered km and GPS km, earnings, tips, surge pay, delivery rating |
| `gold_customer_cohorts` | cohort_month, activity_month | Active customers, orders, GMV, retention rate by months since first order |
| `gold_customer_ltv` | customer_id | First/last order, delivered/cancelled orders, lifetime GMV, contribution, refunds, AOV, tenure |
| `gold_cancellations_daily` | business_date, city, reason, actor | Cancelled orders, GMV lost, minutes to cancel |
| `gold_refunds_daily` | business_date (of the order), city, reason, funded_by, method | Refund count and amount |
| `gold_menu_popularity_weekly` | week_start (Mon), city, cuisine, item_name | Quantity, orders, revenue, rank in city-week |
| `gold_surge_promo_daily` | business_date, city, surge_band, promo | Orders, AOV, platform/restaurant discount cost, delivery-fee revenue, delivery minutes, late rate |

Metric definitions are in [metrics.md](metrics.md).

## `dq_results`

`run_date, check_name, severity, failing_rows, passed, detail, description, checked_at, job_run_id`. One row per
check per job run. The checks themselves live in `src/fooddelivery/quality/checks.py`.
