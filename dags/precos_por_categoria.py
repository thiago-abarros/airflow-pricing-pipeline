import logging
import os
from contextlib import closing
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import TaskGroup, dag, get_current_context, task

from pricing_pipeline.catalogo import buscar_catalogo, calcular_metricas, listar_categorias
from pricing_pipeline.operadores import ValidarProdutosOperator
from pricing_pipeline.persistencia import salvar_precos

FUSO = "America/Sao_Paulo"
log = logging.getLogger(__name__)


def avisar_retry(context):
    log.warning("Nova tentativa de ingestão agendada para %s", context["run_id"])


def registrar_sucesso(context):
    log.info("Catálogo recebido para %s", context["run_id"])


def alertar_falha(context):
    log.error("ALERTA: falha definitiva em %s, execução %s", context["ti"].task_id, context["run_id"])


@dag(
    dag_id="precos_por_categoria",
    description="Quantidade e preços dos produtos por categoria da FakeStore API.",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz=FUSO),
    catchup=False,
    default_args={"on_failure_callback": alertar_falha},
    max_active_runs=1,
    is_paused_upon_creation=False,
)
def pipeline():
    with TaskGroup("ingestao"):
        @task(
            retries=3,
            retry_delay=timedelta(seconds=30),
            retry_exponential_backoff=True,
            on_retry_callback=avisar_retry,
            on_success_callback=registrar_sucesso,
        )
        def buscar_produtos():
            return buscar_catalogo(
                os.environ.get("FAKESTORE_PRODUCTS_URL", "https://fakestoreapi.com/products"),
                Path("/opt/airflow/catalogs"),
                get_current_context()["run_id"],
            )

        validado = ValidarProdutosOperator(task_id="validar_produtos", caminho=buscar_produtos()).output

    with TaskGroup("analise"):
        @task
        def descobrir_categorias(caminho):
            return listar_categorias(caminho)

        @task(pool="ecommerce_pool", multiple_outputs=False)
        def resumir_categoria(caminho, categoria):
            return calcular_metricas(caminho, categoria)

        @task
        def persistir(resumos):
            run = get_current_context()["dag_run"]
            referencia = pendulum.instance(run.logical_date or run.run_after).in_timezone(FUSO)
            linhas = list(resumos)
            hook = PostgresHook(postgres_conn_id="pricing_postgres")
            with closing(hook.get_conn()) as conexao:
                salvar_precos(conexao, linhas, referencia.date(), run.run_id, run.run_after)

        categorias = descobrir_categorias(validado)
        resumos = resumir_categoria.partial(caminho=validado).expand(categoria=categorias)
        persistir(resumos)


pipeline()
