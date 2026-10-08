from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from fdtest import DAYS, HAS_SPARK, SMALL
from fooddelivery.generator.seed import synthetic_seed
from fooddelivery.generator.simulator import Simulator
from fooddelivery.generator.writer import write_batch

# Without the `spark` dependency group the Spark test modules can't even be imported.
collect_ignore_glob = [] if HAS_SPARK else ["spark/*"]


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
