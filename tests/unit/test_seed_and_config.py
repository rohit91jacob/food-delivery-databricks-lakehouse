from __future__ import annotations

from datetime import date

import pytest

from fooddelivery.config import GeneratorConfig, LakehouseTarget, validate_identifier
from fooddelivery.generator.seed import SeedError, normalize_swiggy_rows, read_seed, write_seed


def row(**kw):
    base = {
        "ID": "1",
        "Area": "koramangala",
        "City": "Bangalore",
        "Restaurant": " Tandoor  Hut ",
        "Price": "300.0",
        "Avg ratings": "4.4",
        "Total ratings": "100",
        "Food type": "Biryani,Chinese, Biryani",
        "Address": "5Th Block",
        "Delivery time": "59",
    }
    return base | kw


def test_normalizes_dedupes_and_filters():
    seeds = normalize_swiggy_rows(
        [
            row(),
            row(ID="2"),  # same city/area/name -> duplicate
            row(ID="3", City="Atlantis"),  # unknown city
            row(ID="4", Restaurant="Cheap Eats", Price="0", **{"Avg ratings": "0"}),
        ]
    )
    assert [s.source_id for s in seeds] == ["1", "4"]
    first = seeds[0]
    assert first.name == "Tandoor Hut" and first.area == "Koramangala"
    assert first.cuisines == ("Biryani", "Chinese")
    assert seeds[1].price_for_two == 300  # imputed from the city median
    assert seeds[1].rating == 3.8


def test_rejects_unexpected_columns():
    with pytest.raises(SeedError):
        normalize_swiggy_rows([{"ID": "1"}])


def test_seed_round_trip(tmp_path):
    seeds = normalize_swiggy_rows([row()])
    meta = write_seed(seeds, tmp_path / "s.jsonl", source="test", license_name="CC0-1.0")
    assert meta["restaurants"] == 1 and len(meta["sha256"]) == 64
    assert read_seed(tmp_path / "s.jsonl") == seeds


def test_generator_config_from_env():
    cfg = GeneratorConfig.from_env(
        {"FD_SEED": "7", "FD_START_DATE": "2026-10-01", "FD_CITIES": "Mumbai, Pune", "FD_GPS_PING_SECONDS": "60"}
    )
    assert cfg.seed == 7 and cfg.start_date == date(2026, 10, 1)
    assert cfg.cities == ("Mumbai", "Pune") and cfg.gps_ping_seconds == 60


@pytest.mark.parametrize("env", [{"FD_CITIES": "Gotham"}, {"FD_GPS_PING_SECONDS": "5"}, {"FD_INVALID_RATE": "0.5"}])
def test_generator_config_rejects_bad_values(env):
    with pytest.raises(ValueError):
        GeneratorConfig.from_env(env)


def test_lakehouse_target_quotes_and_validates():
    t = LakehouseTarget.from_env({"FD_CATALOG": "workspace", "FD_SCHEMA": "dev_me_fooddelivery"})
    assert t.volume_path == "/Volumes/workspace/dev_me_fooddelivery/landing"
    assert t.table("gold_daily_kpis") == "`workspace`.`dev_me_fooddelivery`.`gold_daily_kpis`"
    with pytest.raises(ValueError):
        LakehouseTarget(schema="x; DROP TABLE y")
    with pytest.raises(ValueError):
        validate_identifier("a-b", "thing")
