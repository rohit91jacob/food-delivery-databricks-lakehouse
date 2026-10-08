"""Offline emulator for the Lakeflow pipeline: runs the *actual* pipeline source files locally.

``src/fooddelivery_pipeline/transformations/*.py`` are executed unchanged, with two shims injected:

* ``pyspark.pipelines`` (``dp``): records tables, views, materialized views, expectations,
  streaming tables and AUTO CDC flows instead of handing them to the Databricks runtime
* ``spark``: a real local SparkSession, except that ``readStream`` / ``read`` / ``conf`` resolve
  pipeline datasets, and that Auto Loader (``cloudFiles``) becomes a batch JSON read with the same
  schema and ``_metadata`` columns

Datasets are then materialised in dependency order. Expectations are enforced with the same
semantics (warn / drop / fail), and AUTO CDC flows are applied with :mod:`fooddelivery.local.scd`.
This exercises the real pipeline wiring end to end, without a workspace. The parts that are
Databricks-only (incremental processing, streaming checkpoints, Enzyme refresh) are what the
deployed bundle covers.
"""

from __future__ import annotations

import logging
import os
import runpy
import sys
import types
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from fooddelivery.local.scd import scd1, scd2

log = logging.getLogger(__name__)

PIPELINE_DIR = Path(__file__).resolve().parents[2] / "fooddelivery_pipeline" / "transformations"


def local_spark(warehouse: Path, app_name: str = "fooddelivery-local") -> SparkSession:
    """Local Spark with Delta. Delta jars come from Maven via delta-spark, unless ``FD_SPARK_JARS``
    (comma-separated local jar paths) is set, for offline or locked-down networks."""
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("FD_SPARK_MASTER", "local[4]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", os.environ.get("FD_SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.ansi.enabled", "true")
    )
    jars = os.environ.get("FD_SPARK_JARS", "").strip()
    if jars:
        return builder.config("spark.jars", jars).getOrCreate()
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ---------------------------------------------------------------------------- dataset registry
@dataclass
class Expectations:
    warn: dict[str, str] = field(default_factory=dict)
    drop: dict[str, str] = field(default_factory=dict)
    fail: dict[str, str] = field(default_factory=dict)


@dataclass
class Dataset:
    name: str
    kind: str  # table | materialized_view | temporary_view | streaming_table
    fn: Callable[[], DataFrame] | None
    expectations: Expectations
    comment: str = ""


@dataclass
class CdcFlow:
    name: str
    target: str
    source: str
    keys: list[str]
    sequence_by: Column
    scd_type: int
    track_history: list[str] | None
    apply_as_deletes: Column | None
    except_columns: list[str]


class Registry:
    def __init__(self) -> None:
        self.datasets: dict[str, Dataset] = {}
        self.flows: dict[str, list[CdcFlow]] = {}

    def add(self, ds: Dataset) -> None:
        if ds.name in self.datasets:
            raise ValueError(f"dataset {ds.name!r} defined twice")
        self.datasets[ds.name] = ds


def _expectations_of(fn) -> Expectations:
    return getattr(fn, "__fd_expectations__", None) or Expectations()


def build_dp_shim(registry: Registry) -> types.ModuleType:
    """A stand-in for ``pyspark.pipelines`` that records definitions into ``registry``."""
    dp = types.ModuleType("pyspark.pipelines")

    def _expect(kind: str, rules: dict[str, str]):
        def deco(fn):
            exp = _expectations_of(fn)
            getattr(exp, kind).update(rules)
            fn.__fd_expectations__ = exp
            return fn

        return deco

    dp.expect = lambda name, cond: _expect("warn", {name: cond})
    dp.expect_or_drop = lambda name, cond: _expect("drop", {name: cond})
    dp.expect_or_fail = lambda name, cond: _expect("fail", {name: cond})
    dp.expect_all = lambda rules: _expect("warn", rules)
    dp.expect_all_or_drop = lambda rules: _expect("drop", rules)
    dp.expect_all_or_fail = lambda rules: _expect("fail", rules)

    def _dataset_decorator(kind: str):
        def decorator(fn=None, *, name=None, comment="", **_ignored):
            def register(f):
                registry.add(Dataset(name or f.__name__, kind, f, _expectations_of(f), comment))
                return f

            return register(fn) if callable(fn) else register

        return decorator

    dp.table = _dataset_decorator("table")
    dp.materialized_view = _dataset_decorator("materialized_view")
    dp.temporary_view = _dataset_decorator("temporary_view")

    def create_streaming_table(
        name, comment="", expect_all=None, expect_all_or_drop=None, expect_all_or_fail=None, **_ignored
    ):
        registry.add(
            Dataset(
                name,
                "streaming_table",
                None,
                Expectations(dict(expect_all or {}), dict(expect_all_or_drop or {}), dict(expect_all_or_fail or {})),
                comment,
            )
        )

    def create_auto_cdc_flow(
        target,
        source,
        keys,
        sequence_by,
        stored_as_scd_type=1,
        track_history_column_list=None,
        apply_as_deletes=None,
        except_column_list=None,
        name=None,
        **_ignored,
    ):
        seq = F.col(sequence_by) if isinstance(sequence_by, str) else sequence_by
        flow = CdcFlow(
            name or target,
            target,
            source,
            list(keys),
            seq,
            int(stored_as_scd_type),
            list(track_history_column_list) if track_history_column_list else None,
            apply_as_deletes,
            list(except_column_list or []),
        )
        registry.flows.setdefault(target, []).append(flow)

    dp.create_streaming_table = create_streaming_table
    dp.create_auto_cdc_flow = create_auto_cdc_flow
    return dp


# ---------------------------------------------------------------------------- spark shim
class _Reader:
    def __init__(self, emulator: PipelineEmulator):
        self._emu = emulator
        self._format: str | None = None
        self._options: dict[str, str] = {}
        self._schema: str | None = None

    def format(self, fmt: str) -> _Reader:
        self._format = fmt
        return self

    def option(self, key: str, value) -> _Reader:
        self._options[key] = str(value)
        return self

    def options(self, **kwargs) -> _Reader:
        for k, v in kwargs.items():
            self.option(k, v)
        return self

    def schema(self, schema) -> _Reader:
        self._schema = schema
        return self

    def table(self, name: str) -> DataFrame:
        return self._emu.resolve(name)

    def load(self, path: str) -> DataFrame:
        if self._format != "cloudFiles" or self._options.get("cloudFiles.format") != "json":
            raise NotImplementedError(f"emulator only supports Auto Loader JSON, got {self._format}")
        rescued = self._options.get("rescuedDataColumn")
        ddl = f"{self._schema}, {rescued} STRING" if rescued else self._schema
        spark = self._emu.spark
        local = Path(path)
        if not local.exists():  # an empty feed (no files landed yet)
            return spark.createDataFrame([], ddl).withColumn(
                "_metadata",
                F.struct(
                    F.lit(None).cast("string").alias("file_path"),
                    F.lit(None).cast("timestamp").alias("file_modification_time"),
                ),
            )
        reader = spark.read.schema(ddl).option("recursiveFileLookup", "true")
        if rescued:
            reader = reader.option("columnNameOfCorruptRecord", rescued)
        return reader.json(str(local))


class _Conf:
    def __init__(self, values: dict[str, str]):
        self._values = values

    def get(self, key: str, default: str | None = None) -> str:
        if key in self._values:
            return self._values[key]
        if default is not None:
            return default
        raise KeyError(f"pipeline configuration {key!r} not set")


class SparkShim:
    def __init__(self, emulator: PipelineEmulator, conf: dict[str, str]):
        self._emu = emulator
        self.conf = _Conf(conf)

    @property
    def readStream(self) -> _Reader:
        return _Reader(self._emu)

    @property
    def read(self) -> _Reader:
        return _Reader(self._emu)

    def __getattr__(self, item):
        return getattr(self._emu.spark, item)


# ---------------------------------------------------------------------------- emulator
@dataclass
class RunReport:
    rows: dict[str, int] = field(default_factory=dict)
    dropped: dict[str, int] = field(default_factory=dict)
    warnings: dict[str, int] = field(default_factory=dict)

    def by_prefix(self, prefix: str) -> dict[str, int]:
        return {k.removeprefix(prefix): v for k, v in self.rows.items() if k.startswith(prefix)}


class ExpectationFailed(RuntimeError):
    pass


def _violations(df: DataFrame, rules: dict[str, str]) -> dict[str, int]:
    if not rules:
        return {}
    aggs = [
        F.sum(F.when(~F.coalesce(F.expr(f"({e})"), F.lit(False)), 1).otherwise(0)).alias(n) for n, e in rules.items()
    ]
    row = df.agg(*aggs).collect()[0]
    return {n: int(row[n] or 0) for n in rules}


class PipelineEmulator:
    def __init__(
        self,
        spark: SparkSession,
        landing: Path,
        *,
        database: str = "fooddelivery_local",
        persist: bool = False,
        pipeline_dir: Path = PIPELINE_DIR,
    ):
        self.spark = spark
        self.database = database
        self.persist = persist
        self.registry = Registry()
        self.report = RunReport()
        self._materialised: dict[str, DataFrame] = {}
        self._resolving: list[str] = []
        self._load(pipeline_dir, {"fooddelivery.landing_path": str(landing)})
        if persist:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    def _load(self, pipeline_dir: Path, conf: dict[str, str]) -> None:
        import pyspark

        dp = build_dp_shim(self.registry)
        saved_module, saved_attr = sys.modules.get("pyspark.pipelines"), getattr(pyspark, "pipelines", None)
        sys.modules["pyspark.pipelines"] = dp
        pyspark.pipelines = dp
        try:
            for source in sorted(pipeline_dir.glob("*.py")):
                runpy.run_path(str(source), init_globals={"spark": SparkShim(self, conf)})
        finally:
            if saved_module is None:
                sys.modules.pop("pyspark.pipelines", None)
            else:
                sys.modules["pyspark.pipelines"] = saved_module
            if saved_attr is None:
                delattr(pyspark, "pipelines")
            else:
                pyspark.pipelines = saved_attr

    def table_name(self, name: str) -> str:
        """Name to use in SQL: a Delta table when persisting, else a session temp view."""
        return f"spark_catalog.{self.database}.{name}" if self.persist else name

    # ------------------------------------------------------------------ resolution
    def resolve(self, name: str) -> DataFrame:
        if name in self._materialised:
            return self._materialised[name]
        if name not in self.registry.datasets:
            raise KeyError(f"dataset {name!r} is read but never defined by the pipeline")
        if name in self._resolving:
            raise RuntimeError(f"dependency cycle: {' -> '.join([*self._resolving, name])}")
        self._resolving.append(name)
        try:
            ds = self.registry.datasets[name]
            df = self._compute(ds)
            df = self._enforce(ds, df)
            df = self._store(ds, df)
        finally:
            self._resolving.pop()
        self._materialised[name] = df
        return df

    def _compute(self, ds: Dataset) -> DataFrame:
        if ds.kind != "streaming_table":
            return ds.fn()
        flows = self.registry.flows.get(ds.name)
        if not flows:
            raise RuntimeError(f"streaming table {ds.name!r} has no flow")
        if len(flows) != 1:
            raise NotImplementedError("the emulator supports one AUTO CDC flow per target")
        f = flows[0]
        source = self.resolve(f.source)
        if f.scd_type == 2:
            return scd2(
                source,
                f.keys,
                f.sequence_by,
                track_history=f.track_history,
                apply_as_deletes=f.apply_as_deletes,
                except_columns=f.except_columns,
            )
        return scd1(source, f.keys, f.sequence_by, apply_as_deletes=f.apply_as_deletes, except_columns=f.except_columns)

    def _enforce(self, ds: Dataset, df: DataFrame) -> DataFrame:
        exp = ds.expectations
        counts = _violations(df, {**exp.fail, **exp.drop, **exp.warn})
        for rule in exp.fail:
            if counts[rule]:
                raise ExpectationFailed(f"{ds.name}: expect_or_fail {rule!r} violated by {counts[rule]} rows")
        for rule in exp.warn:
            self.report.warnings[f"{ds.name}.{rule}"] = counts[rule]
        if exp.drop:
            cond = F.lit(True)
            for expr in exp.drop.values():
                cond = cond & F.coalesce(F.expr(f"({expr})"), F.lit(False))
            for rule in exp.drop:
                self.report.dropped[f"{ds.name}.{rule}"] = counts[rule]
            df = df.where(cond)
        return df

    def _store(self, ds: Dataset, df: DataFrame) -> DataFrame:
        if ds.kind == "temporary_view":
            df = df.localCheckpoint(eager=True)
        elif self.persist:
            target = self.table_name(ds.name)
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
            df = self.spark.read.table(target)
        else:
            df = df.localCheckpoint(eager=True)
            df.createOrReplaceTempView(ds.name)
        if ds.kind != "temporary_view":
            self.report.rows[ds.name] = df.count()
        return df

    def run(self) -> RunReport:
        for name, ds in self.registry.datasets.items():
            if ds.kind != "temporary_view":
                self.resolve(name)
        log.info("pipeline emulation complete", extra={"datasets": len(self.report.rows)})
        return self.report
