import os
from collections.abc import Sequence
from contextlib import closing
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import TaskGroup, dag, get_current_context, task
from pricing_pipeline.catalogo import (
    buscar_catalogo,
    particionar_catalogo,
    precos_da_categoria,
)
from pricing_pipeline.contratos import ResumoXCom, consolidar
from pricing_pipeline.metricas import calcular_metricas
from pricing_pipeline.observabilidade import (
    alertar_falha,
    avisar_retry,
    log,
    registrar_inicio,
    registrar_sucesso,
)
from pricing_pipeline.operadores import ValidarProdutosOperator
from pricing_pipeline.persistencia import salvar_precos

FUSO = "America/Sao_Paulo"


@dag(
    dag_id="precos_por_categoria",
    description="Quantidade e preços dos produtos por categoria da FakeStore API.",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz=FUSO),
    catchup=False,
    default_args={
        "on_execute_callback": registrar_inicio,
        "on_success_callback": registrar_sucesso,
        "on_failure_callback": alertar_falha,
    },
    max_active_runs=1,
    is_paused_upon_creation=False,
)
def pipeline():
    with TaskGroup("ingestao"):
        @task(
            retries=3,
            retry_delay=timedelta(seconds=30),
            retry_exponential_backoff=True,
            execution_timeout=timedelta(seconds=90),
            on_retry_callback=avisar_retry,
        )
        def buscar_produtos() -> str:
            return buscar_catalogo(
                os.environ.get("FAKESTORE_PRODUCTS_URL", "https://fakestoreapi.com/products"),
                Path("/opt/airflow/catalogs"),
                get_current_context()["run_id"],
            )

        validado = ValidarProdutosOperator(task_id="validar_produtos", caminho=buscar_produtos()).output

    with TaskGroup("analise"):
        @task
        def descobrir_categorias(caminho: str) -> list[str]:
            categorias = particionar_catalogo(caminho)
            log.info("catalogo_particionado", quantidade_categorias=len(categorias))
            return categorias

        @task(
            pool="ecommerce_pool",
            multiple_outputs=False,
            map_index_template="{{ task.op_kwargs['categoria'] }}",
        )
        def resumir_categoria(caminho: str, categoria: str) -> ResumoXCom:
            log.info("categoria_iniciada", categoria=categoria)
            resumo = calcular_metricas(categoria, precos_da_categoria(caminho, categoria))
            log.info("categoria_calculada", categoria=categoria, quantidade_produtos=resumo.quantidade)
            return resumo.para_xcom()

        @task(
            retries=2,
            retry_delay=timedelta(seconds=30),
            retry_exponential_backoff=True,
            execution_timeout=timedelta(seconds=90),
            on_retry_callback=avisar_retry,
        )
        def persistir(categorias: list[str], resumos: Sequence[ResumoXCom]) -> None:
            run = get_current_context()["dag_run"]
            referencia = pendulum.instance(run.logical_date or run.run_after).in_timezone(FUSO).date()
            # O Airflow entrega uma sequência lazy. A leitura indexada evita uma
            # consulta XCom fora dos limites ao encerrar a iteração.
            linhas = consolidar(categorias, (resumos[i] for i in range(len(resumos))))
            hook = PostgresHook(postgres_conn_id="pricing_postgres")
            with closing(hook.get_conn()) as conexao:
                snapshot_atualizado = salvar_precos(conexao, linhas, referencia, run.run_id, run.run_after)
            log.info(
                "publicacao_confirmada",
                data_referencia=referencia.isoformat(),
                fuso_referencia=FUSO,
                quantidade_categorias=len(linhas),
                quantidade_produtos=sum(linha.quantidade for linha in linhas),
                snapshot_atualizado=snapshot_atualizado,
            )

        categorias = descobrir_categorias(validado)
        resumos = resumir_categoria.partial(caminho=validado).expand(categoria=categorias)
        persistir(categorias, resumos)


pipeline()
