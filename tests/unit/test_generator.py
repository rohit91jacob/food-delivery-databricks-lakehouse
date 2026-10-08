from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal

from fdtest import DAYS, SMALL
from fooddelivery.generator.simulator import ENTITIES, Simulator
from fooddelivery.generator.writer import read_checksums, read_manifest, write_batch

P = datetime.fromisoformat


def paise(x) -> int:
    return int(Decimal(repr(float(x))) * 100)


def test_every_entity_is_emitted(batches):
    first = batches[DAYS[0]]
    assert set(first.records) == set(ENTITIES)
    for entity in ENTITIES:
        assert first.records[entity], f"{entity} is empty on day 0"


def test_generation_is_byte_for_byte_reproducible(small_seed, batches, tmp_path):
    again = Simulator(SMALL, small_seed).generate_day(DAYS[1])  # fresh process state, out of order
    a, b = tmp_path / "a", tmp_path / "b"
    write_batch(batches[DAYS[1]], a)
    write_batch(again, b)
    assert read_checksums(a, DAYS[1].isoformat()) == read_checksums(b, DAYS[1].isoformat())


def test_records_carry_business_date_and_version(batches):
    for d, batch in batches.items():
        for entity, rows in batch.records.items():
            assert {r["business_date"] for r in rows} <= {d.isoformat()}, entity
            assert {r["record_version"] for r in rows} <= {1}


def test_manifest_matches_recomputed_ground_truth(batches):
    for batch in batches.values():
        orders = {}
        for o in batch.records["orders"]:
            if o["order_id"].startswith("O"):
                orders[o["order_id"]] = o
        events = defaultdict(dict)
        for e in batch.records["order_events"]:
            events[e["order_id"]][e["status"]] = e
        by_city = defaultdict(Counter)
        for oid, o in orders.items():
            c = by_city[o["city"]]
            c["orders_placed"] += 1
            if "delivered" in events[oid]:
                c["orders_delivered"] += 1
                c["gmv"] += paise(o["total_amount"])
                c["tips"] += paise(o["tip"])
            elif "cancelled" in events[oid]:
                c["orders_cancelled"] += 1
                c["cancelled_" + events[oid]["cancelled"]["reason"]] += 1
        for r in batch.records["refunds"]:
            by_city[orders[r["order_id"]]["city"]]["refunds_amount"] += paise(r["amount"])
        for m in batch.manifests:
            c = by_city[m["city"]]
            for k in ("orders_placed", "orders_delivered", "orders_cancelled", "cancelled_no_rider_available"):
                assert m[k] == c[k], (m["city"], k)
            for k in ("gmv", "tips", "refunds_amount"):
                assert paise(m[k]) == c[k], (m["city"], k)


def test_order_totals_add_up_and_lifecycle_is_ordered(batches):
    order = ["placed", "accepted", "preparing", "ready", "picked_up", "delivered"]
    for batch in batches.values():
        events = defaultdict(dict)
        for e in batch.records["order_events"]:
            events[e["order_id"]][e["status"]] = P(e["event_at"])
        for o in batch.records["orders"]:
            if not o["order_id"].startswith("O"):
                continue
            parts = (
                paise(o["subtotal"])
                - paise(o["discount"])
                + paise(o["delivery_fee"])
                + paise(o["platform_fee"])
                + paise(o["tax"])
                + paise(o["tip"])
            )
            assert parts == paise(o["total_amount"])
            assert paise(o["subtotal"]) == sum(paise(i["unit_price"]) * i["quantity"] for i in o["items"])
            ts = events[o["order_id"]]
            if "delivered" in ts:
                seq = [ts[s] for s in order]
                assert seq == sorted(seq), o["order_id"]
                assert ts["rider_assigned"] >= ts["accepted"]


def test_injected_duplicates_and_corruptions_are_counted(batches):
    for batch in batches.values():
        ids = Counter(o["order_id"] for o in batch.records["orders"])
        dups = sum(n - 1 for n in ids.values() if n > 1)
        bad = sum(1 for i in ids if i.startswith("X"))
        assert dups == sum(m["duplicate_order_records"] for m in batch.manifests)
        assert bad == sum(m["invalid_order_records"] for m in batch.manifests)


def test_day_zero_is_a_snapshot_and_later_days_are_changes(batches):
    d0, d1 = batches[DAYS[0]], batches[DAYS[1]]
    assert len(d0.records["restaurants"]) >= SMALL.restaurants_per_city
    assert len(d1.records["restaurants"]) < len(d0.records["restaurants"])
    assert len(d0.records["customers"]) >= SMALL.initial_customers_per_city * len(SMALL.cities)
    assert {r["op"] for r in d0.records["menu_items"]} == {"UPSERT"}


def test_rates_are_plausible(batches):
    placed = delivered = late = 0
    for batch in batches.values():
        for m in batch.manifests:
            placed += m["orders_placed"]
            delivered += m["orders_delivered"]
            late += m["late_deliveries"]
    assert 0.85 < delivered / placed < 0.99
    assert late / delivered < 0.45


def test_written_manifest_round_trips(landing):
    rows = read_manifest(landing, DAYS[0].isoformat())
    assert {r["city"] for r in rows} == set(SMALL.cities)
