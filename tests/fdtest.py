"""Shared test constants (importable as `fdtest`; `conftest` names clash across directories)."""

from __future__ import annotations

import importlib.util
from datetime import timedelta
from pathlib import Path

from fooddelivery.config import GeneratorConfig

REPO = Path(__file__).resolve().parents[1]
HAS_SPARK = importlib.util.find_spec("pyspark") is not None and importlib.util.find_spec("delta") is not None

# A small but complete world: every feed, CDC change, duplicate and corrupt record shows up within 3 days.
SMALL = GeneratorConfig(
    cities=("Bangalore", "Pune"),
    restaurants_per_city=25,
    base_orders_per_city=90,
    initial_customers_per_city=400,
    duplicate_rate=0.03,
    invalid_rate=0.03,
)
DAYS = [SMALL.start_date + timedelta(days=i) for i in range(3)]
