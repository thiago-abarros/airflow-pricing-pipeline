"""Eventos estruturados que herdam o contexto da tarefa do Airflow."""

from datetime import UTC, datetime

import structlog

log = structlog.get_logger("pricing_pipeline")


def _duracao_ms(context) -> int | None:
    inicio = context["ti"].start_date
    if inicio is None:
        return None
    return round((datetime.now(UTC) - inicio).total_seconds() * 1000)


def registrar_inicio(_context):
    log.info("etapa_iniciada")


def registrar_sucesso(context):
    log.info("etapa_concluida", duracao_ms=_duracao_ms(context))


def avisar_retry(context):
    log.warning("etapa_retentativa", duracao_ms=_duracao_ms(context))


def alertar_falha(context):
    log.error("etapa_falhou", alerta="falha_definitiva", duracao_ms=_duracao_ms(context))
