"""Silver: typed, validated, de-duplicated tables built with AUTO CDC.

For each spec in ``fooddelivery.transforms.silver.SPECS``:

* ``v_<entity>_conformed``: temporary view with the expectations (warn / drop / fail)
* ``quarantine_<entity>``: rows that broke a drop rule, with the failed rule names (feeds with ``quarantine=True``)
* ``silver_<entity>``: streaming table maintained by ``create_auto_cdc_flow``, SCD1 (dedupe and
  upsert by key) or SCD2 (history on tracked columns, for restaurants and menu prices)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from fooddelivery.transforms.silver import SPECS, SilverSpec, quarantine


def define_silver(spec: SilverSpec) -> None:
    view_name = f"v_{spec.entity}_conformed"

    def conformed():
        return spec.typed(spark.readStream.table(spec.bronze_table))

    view = conformed
    if spec.fail:
        view = dp.expect_all_or_fail(spec.fail)(view)
    if spec.drop:
        view = dp.expect_all_or_drop(spec.drop)(view)
    if spec.warn:
        view = dp.expect_all(spec.warn)(view)
    dp.temporary_view(name=view_name, comment=f"Typed, validated {spec.entity} feed.")(view)

    if spec.drop and spec.quarantine:

        @dp.table(
            name=f"quarantine_{spec.entity}",
            comment=f"{spec.entity} rows rejected by a drop expectation, with `_failed_rules`.",
            table_properties={"quality": "quarantine"},
        )
        def quarantined():
            return quarantine(spec.typed(spark.readStream.table(spec.bronze_table)), spec.drop)

    dp.create_streaming_table(
        name=f"silver_{spec.entity}",
        comment=spec.comment,
        table_properties={"quality": "silver"},
    )
    dp.create_auto_cdc_flow(
        target=f"silver_{spec.entity}",
        source=view_name,
        keys=list(spec.keys),
        sequence_by=F.col(spec.sequence_by),
        stored_as_scd_type=spec.scd_type,
        track_history_column_list=list(spec.track_history) if spec.scd_type == 2 and spec.track_history else None,
        apply_as_deletes=F.expr("op = 'DELETE'") if spec.is_cdc_feed else None,
        except_column_list=list(spec.except_columns) or None,
        name=f"auto_cdc_{spec.entity}",
    )


for _spec in SPECS:
    define_silver(_spec)
