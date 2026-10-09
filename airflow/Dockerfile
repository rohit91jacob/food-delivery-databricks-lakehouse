# Airflow image for the food-delivery DAG: official image + Databricks provider + this repo's package.
# Build context is the repository root (see airflow/docker-compose.yaml).
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

ARG AIRFLOW_VERSION
ARG DATABRICKS_PROVIDER_VERSION=7.20.0
ENV CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt

# Provider pinned against the official constraints for this Airflow/Python pair.
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "apache-airflow-providers-databricks==${DATABRICKS_PROVIDER_VERSION}" \
      --constraint "${CONSTRAINTS_URL}"

# The fooddelivery package (generator, seeding, landing sync, reconciliation) with Kaggle + SDK
# extras, resolved under the same constraints so it cannot drift Airflow's own dependencies.
COPY --chown=airflow:root pyproject.toml README.md LICENSE /opt/fooddelivery/
COPY --chown=airflow:root src /opt/fooddelivery/src
RUN pip install --no-cache-dir "/opt/fooddelivery[seed,databricks]" --constraint "${CONSTRAINTS_URL}"

# The committed CC0 restaurant seed, so the default FD_SEED_SOURCE=committed needs no Kaggle token.
COPY --chown=airflow:root seed /opt/fooddelivery/seed
ENV FD_COMMITTED_SEED=/opt/fooddelivery/seed/restaurants.jsonl.gz

COPY --chown=airflow:root airflow/dags /opt/airflow/dags
