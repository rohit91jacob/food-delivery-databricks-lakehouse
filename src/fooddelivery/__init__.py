"""Food-delivery lakehouse.

Python package shared by three runtimes:

* the Airflow side (Kaggle seeding, synthetic data generation, landing-zone upload),
* the Databricks Lakeflow Declarative Pipeline (``fooddelivery.transforms``),
* the serverless data-quality job task (``fooddelivery.quality``).
"""

__version__ = "0.1.0"

# Bump whenever the generator's output for an already-generated business date changes.
# It is stamped on every record (``record_version``) and in landing file names, so a rerun
# with new semantics lands as new files that win over the old ones in silver (see ADR 0002).
GENERATOR_VERSION = 1
