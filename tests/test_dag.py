from pathlib import Path

import pendulum
from airflow.dag_processing.dagbag import DagBag
from airflow.serialization.encoders import coerce_to_core_timetable
from airflow.timetables.base import TimeRestriction


def test_contrato_da_dag():
    pasta = Path(__file__).resolve().parents[1] / "dags"
    bag = DagBag(dag_folder=pasta)
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags["precos_por_categoria"]
    assert not dag.catchup and not dag.is_paused_upon_creation
    assert dag.max_active_runs == 1
    busca = dag.get_task("ingestao.buscar_produtos")
    assert busca.retries == 3 and busca.retry_exponential_backoff
    assert all((busca.on_retry_callback, busca.on_success_callback, busca.on_failure_callback))
    assert busca.downstream_task_ids == {"ingestao.validar_produtos"}
    resumo = dag.get_task("analise.resumir_categoria")
    assert resumo.pool == "ecommerce_pool" and resumo.pool_slots == 1
    assert "analise.descobrir_categorias" in resumo.upstream_task_ids
    assert resumo.downstream_task_ids == {"analise.persistir"}
    proxima = coerce_to_core_timetable(dag.timetable).next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(earliest=pendulum.datetime(2026, 1, 1, tz="UTC"), latest=None, catchup=True),
    )
    assert proxima.run_after.in_timezone("America/Sao_Paulo").hour == 6
    assert proxima.run_after.minute == 0
