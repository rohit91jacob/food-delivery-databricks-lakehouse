"""Genie agent over the gold marts, traced, evaluated and monitored with MLflow.

* ``space``     - declares the Genie space (tables, instructions, example SQL) and creates/updates it.
* ``genie_agent`` - an MLflow ``ResponsesAgent`` that answers through the Genie Conversation API.
  It is self-contained (mlflow + databricks-sdk only) so it can be logged as models-from-code.
* ``evalset``   - evaluation questions whose expected answers come from the generator manifest.
* ``scorers``   - deterministic scorers shared by offline evaluation and production monitoring.
* ``ops``       - tracing setup, evaluation, monitoring registration and deployment.
"""
