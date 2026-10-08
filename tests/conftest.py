from __future__ import annotations

import importlib.util
from datetime import date, timedelta
from pathlib import Path

import pytest

from fooddelivery.config import GeneratorConfig
from fooddelivery.generator.seed import synthetic_seed
from fooddelivery.generator.simulator import Simulator
from fooddelivery.generator.writer import write_batch

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


def pytest_collection_modifyitems(config, items):
    skip = pytest.mark.skip(reason="pyspark/delta-spark not installed (uv sync --group spark)")
    for item in items:
        if "spark" in item.keywords and not HAS_SPARK:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def small_seed():
    return synthetic_seed(SMALL.cities, 40)


@pytest.fixture(scope="session")
def simulator(small_seed) -> Simulator:
    return Simulator(SMALL, small_seed)


@pytest.fixture(scope="session")
def batches(simulator):
    return {d: simulator.generate_day(d) for d in DAYS}


@pytest.fixture(scope="session")
def landing(tmp_path_factory, batches) -> Path:
    root = tmp_path_factory.mktemp("landing")
    for b in batches.values():
        write_batch(b, root)
    return root


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from fooddelivery.local.lakehouse import local_spark

    session = local_spark(tmp_path_factory.mktemp("warehouse"), "fooddelivery-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def emulated(spark, landing):
    """The real pipeline sources run once over the 3-day landing zone."""
    from fooddelivery.local.lakehouse import PipelineEmulator

    emulator = PipelineEmulator(spark, landing)
    emulator.run()
    return emulator


def day(n: int) -> date:
    return DAYS[n]
