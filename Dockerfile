FROM ghcr.io/astral-sh/uv:0.8.17 AS uv

FROM python:3.12-slim-bookworm AS base
COPY --from=uv /uv /usr/local/bin/uv
ENV AIRFLOW_HOME=/opt/airflow \
    AIRFLOW__CORE__DAGS_FOLDER=/workspace/dags \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1
RUN useradd --create-home --uid 1000 airflow \
    && mkdir -p /opt/venv /opt/airflow/shared /opt/airflow/catalogs /opt/airflow/logs /workspace \
    && chown -R airflow:airflow /opt/venv /opt/airflow /workspace
WORKDIR /workspace
USER airflow
COPY --chown=airflow:airflow pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --no-dev --no-cache
COPY --chown=airflow:airflow dags ./dags
COPY --chown=airflow:airflow docker/init-airflow.sh /opt/airflow/init.sh
COPY --chown=airflow:airflow docker/init-analytics.sql /opt/airflow/init-analytics.sql

FROM base AS development
ENV PRICING_DEV_CONTAINER=1
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git make \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN uv sync --locked --no-cache

FROM base AS runtime
