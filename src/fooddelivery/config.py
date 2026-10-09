"""Environment-driven configuration (see ``.env.example`` for every variable)."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value: str, what: str) -> str:
    """Unity Catalog identifiers end up inside SQL text, so only plain identifiers are allowed."""
    if not _IDENTIFIER.match(value):
        raise ValueError(f"{what} must match {_IDENTIFIER.pattern!r}, got {value!r}")
    return value


def _get(env: Mapping[str, str], key: str, default: str) -> str:
    value = env.get(key, "").strip()
    return value or default


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 42
    start_date: date = date(2026, 9, 1)
    cities: tuple[str, ...] = ("Bangalore", "Mumbai", "Delhi", "Hyderabad", "Pune")
    restaurants_per_city: int = 120
    base_orders_per_city: int = 600
    initial_customers_per_city: int = 4000
    # Fleet size scales with each city's demand: riders = base orders x demand scale x this / 100.
    riders_per_100_orders: float = 18.0
    gps_ping_seconds: int = 120
    # Share of order records duplicated / corrupted on purpose so dedupe and quarantine are exercised.
    duplicate_rate: float = 0.003
    invalid_rate: float = 0.002

    def __post_init__(self) -> None:
        from fooddelivery.generator.geo import CITIES

        unknown = [c for c in self.cities if c not in CITIES]
        if unknown:
            raise ValueError(f"unknown cities {unknown}; choose from {sorted(CITIES)}")
        if self.gps_ping_seconds < 15:
            raise ValueError("FD_GPS_PING_SECONDS must be >= 15")
        for name in (
            "restaurants_per_city",
            "base_orders_per_city",
            "initial_customers_per_city",
            "riders_per_100_orders",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if not (0 <= self.duplicate_rate < 0.2 and 0 <= self.invalid_rate < 0.2):
            raise ValueError("duplicate/invalid rates must be in [0, 0.2)")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> GeneratorConfig:
        env = os.environ if env is None else env
        d = cls()
        return cls(
            seed=int(_get(env, "FD_SEED", str(d.seed))),
            start_date=date.fromisoformat(_get(env, "FD_START_DATE", d.start_date.isoformat())),
            cities=tuple(c.strip() for c in _get(env, "FD_CITIES", ",".join(d.cities)).split(",") if c.strip()),
            restaurants_per_city=int(_get(env, "FD_RESTAURANTS_PER_CITY", str(d.restaurants_per_city))),
            base_orders_per_city=int(_get(env, "FD_BASE_ORDERS_PER_CITY", str(d.base_orders_per_city))),
            initial_customers_per_city=int(
                _get(env, "FD_INITIAL_CUSTOMERS_PER_CITY", str(d.initial_customers_per_city))
            ),
            riders_per_100_orders=float(_get(env, "FD_RIDERS_PER_100_ORDERS", str(d.riders_per_100_orders))),
            gps_ping_seconds=int(_get(env, "FD_GPS_PING_SECONDS", str(d.gps_ping_seconds))),
            duplicate_rate=float(_get(env, "FD_DUPLICATE_RATE", str(d.duplicate_rate))),
            invalid_rate=float(_get(env, "FD_INVALID_RATE", str(d.invalid_rate))),
        )


@dataclass(frozen=True)
class LakehouseTarget:
    """Where data lands in Unity Catalog. Must match the deployed bundle target."""

    catalog: str = "workspace"
    schema: str = "fooddelivery"
    gold_schema: str = ""  # defaults to <schema>_gold
    volume: str = "landing"
    job_name: str = "fooddelivery_daily"
    warehouse_name: str = "Serverless Starter Warehouse"

    def __post_init__(self) -> None:
        validate_identifier(self.catalog, "FD_CATALOG")
        validate_identifier(self.schema, "FD_SCHEMA")
        if not self.gold_schema:
            object.__setattr__(self, "gold_schema", f"{self.schema}_gold")
        validate_identifier(self.gold_schema, "FD_GOLD_SCHEMA")
        validate_identifier(self.volume, "FD_VOLUME")

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    def table(self, name: str) -> str:
        validate_identifier(name, "table name")
        return f"`{self.catalog}`.`{self.schema}`.`{name}`"

    def gold_table(self, name: str) -> str:
        validate_identifier(name, "table name")
        return f"`{self.catalog}`.`{self.gold_schema}`.`{name}`"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> LakehouseTarget:
        env = os.environ if env is None else env
        d = cls()
        return cls(
            catalog=_get(env, "FD_CATALOG", d.catalog),
            schema=_get(env, "FD_SCHEMA", d.schema),
            gold_schema=_get(env, "FD_GOLD_SCHEMA", ""),
            volume=_get(env, "FD_VOLUME", d.volume),
            job_name=_get(env, "FD_DATABRICKS_JOB_NAME", d.job_name),
            warehouse_name=_get(env, "FD_SQL_WAREHOUSE_NAME", d.warehouse_name),
        )


@dataclass(frozen=True)
class Paths:
    data_dir: Path = field(default_factory=lambda: Path("data"))

    @property
    def seed_file(self) -> Path:
        return self.data_dir / "seed" / "restaurants.jsonl"

    @property
    def downloads(self) -> Path:
        return self.data_dir / "downloads"

    @property
    def landing(self) -> Path:
        return self.data_dir / "landing"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Paths:
        env = os.environ if env is None else env
        return cls(Path(_get(env, "FD_DATA_DIR", "data")).expanduser())
