#!/bin/sh
set -eu

export PRICING_IMAGE="airflow-pricing-smoke:local"
# Uma porta dinâmica evita conflito com a pilha principal e com make demo-up.
export AIRFLOW_DEMO_PORT=0
projeto="pricing-smoke-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
dag=precos_por_categoria
data_logica=2025-01-02T09:00:00+00:00

pilha() {
    sh scripts/compose.sh -p "$projeto" -f compose.yaml -f tests/compose.falhas.yaml "$@"
}

limpar() {
    codigo=$?
    if [ "$codigo" -ne 0 ]; then
        pilha logs --tail=25 scheduler dag-processor >&2
    fi
    pilha down -v >/dev/null 2>&1 || true
}
trap limpar EXIT
trap 'exit 130' INT TERM

sql() {
    pilha exec -T postgres psql -At -v ON_ERROR_STOP=1 -U "$1" -d "$1" -c "$2"
}

airflow() {
    pilha exec -T scheduler airflow "$@"
}

limpar_analise() {
    pilha exec -T api python - "$dag" "$1" <<'PY'
import json
import os
import sys
from pathlib import Path

import requests

dag_id, run_id = sys.argv[1:]
api = "http://localhost:8080"
senha = json.loads(Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"]).read_text())["admin"]
with requests.Session() as sessao:
    token = sessao.post(f"{api}/auth/token", json={"username": "admin", "password": senha}, timeout=10)
    token.raise_for_status()
    sessao.headers["Authorization"] = f"Bearer {token.json()['access_token']}"
    resposta = sessao.post(
        f"{api}/api/v2/dags/{dag_id}/clearTaskInstances",
        json={"dag_run_id": run_id, "task_group_id": "analise", "dry_run": False, "only_failed": False},
        timeout=30,
    )
    resposta.raise_for_status()
    tarefas = {tarefa["task_id"] for tarefa in resposta.json()["task_instances"]}
    assert tarefas == {"analise.descobrir_categorias", "analise.resumir_categoria", "analise.persistir"}, tarefas
PY
}

igual() {
    if [ "$1" != "$2" ]; then
        printf 'Esperado: %s\nRecebido: %s\n' "$2" "$1" >&2
        exit 1
    fi
}

aguardar() {
    run_id=$1
    esperado=${2:-success}
    for tentativa in $(seq 1 "${3:-60}"); do
        estado=$(sql airflow "SELECT state FROM dag_run WHERE run_id = '$run_id'")
        case "$estado" in
            success|failed) igual "$estado" "$esperado"; return ;;
        esac
        sleep 2
    done
    printf 'Tempo excedido esperando %s\n' "$run_id" >&2
    return 1
}

aguardar_retry() {
    for tentativa in $(seq 1 60); do
        estado=$(sql airflow "SELECT state FROM task_instance
            WHERE run_id = '$1' AND task_id = '$2' ORDER BY try_number DESC LIMIT 1")
        if [ "$estado" = up_for_retry ]; then return; fi
        if [ "$(sql airflow "SELECT state FROM dag_run WHERE run_id = '$1'")" = failed ]; then break; fi
        sleep 1
    done
    printf 'Retentativa não observada em %s / %s. Estado: %s\n' "$1" "$2" "$estado" >&2
    return 1
}

confirmar_log() {
    pilha exec -T scheduler python - "$dag" "$@" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

dag_id, run_id, task_id, evento, *campos = sys.argv[1:]
pasta = Path("/opt/airflow/logs") / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}"
esperado = dict(campo.split("=", 1) for campo in campos)
for arquivo in pasta.rglob("attempt=*.log"):
    for linha in arquivo.read_text().splitlines():
        registro = json.loads(linha)
        if registro.get("event") == evento and all(str(registro.get(k)) == v for k, v in esperado.items()):
            assert registro["timestamp"].endswith("Z")
            assert datetime.fromisoformat(registro["timestamp"]).utcoffset() == timezone.utc.utcoffset(None)
            assert (registro["dag_id"], registro["run_id"], registro["task_id"]) == (dag_id, run_id, task_id)
            assert registro["try_number"] >= 1
            if evento == "etapa_concluida":
                assert registro["duracao_ms"] >= 0
            if task_id == "analise.resumir_categoria":
                assert registro["map_index"] >= 0
            sys.exit(0)
sys.exit(f"Evento não encontrado: {run_id} / {task_id} / {evento} / {esperado}")
PY
}

publicacao() {
    sql pricing "SELECT json_build_array(
        (SELECT json_agg(d ORDER BY categoria) FROM precos_diarios d WHERE data_referencia = '2025-01-02'),
        (SELECT json_agg(h ORDER BY categoria) FROM precos_historico h WHERE run_id = 'smoke-t06-t07')
    )"
}

pilha build init
pilha up -d --wait api dag-processor catalogo-teste
pilha exec -T catalogo-teste cp /amostras/valido.json /tmp/products
pilha up -d --wait scheduler

# A API pode ficar saudável antes da primeira leitura da DAG pelo processador.
for tentativa in $(seq 1 30); do
    if [ "$(sql airflow "SELECT count(*) FROM dag WHERE dag_id = '$dag'")" = 1 ]; then break; fi
    sleep 2
done

airflow dags trigger "$dag" --run-id smoke-t06-t07 --logical-date "$data_logica"
aguardar smoke-t06-t07
confirmar_log smoke-t06-t07 ingestao.buscar_produtos catalogo_gravado status_http=200
confirmar_log smoke-t06-t07 ingestao.validar_produtos catalogo_validado quantidade_produtos=4
confirmar_log smoke-t06-t07 analise.descobrir_categorias catalogo_particionado quantidade_categorias=3
confirmar_log smoke-t06-t07 analise.resumir_categoria categoria_calculada categoria=livros quantidade_produtos=2
confirmar_log smoke-t06-t07 analise.persistir publicacao_confirmada data_referencia=2025-01-02 snapshot_atualizado=True
for tarefa in ingestao.buscar_produtos ingestao.validar_produtos analise.descobrir_categorias analise.resumir_categoria analise.persistir; do
    confirmar_log smoke-t06-t07 "$tarefa" etapa_iniciada
    confirmar_log smoke-t06-t07 "$tarefa" etapa_concluida
done
igual "$(sql airflow "SELECT slots FROM slot_pool WHERE pool = 'ecommerce_pool'")" 2
igual "$(sql airflow "SELECT count(DISTINCT region_index) FROM task_instance
    WHERE run_id = 'smoke-t06-t07' AND task_id = 'analise.resumir_categoria'
    AND pool = 'ecommerce_pool' AND state = 'success' AND region_index >= 0")" 3
pico=$(sql airflow "WITH m AS (
    SELECT start_date, end_date FROM task_instance WHERE run_id = 'smoke-t06-t07'
    AND task_id = 'analise.resumir_categoria' AND state = 'success'
)
SELECT max((SELECT count(*) FROM m b WHERE b.start_date <= a.start_date AND b.end_date > a.start_date)) FROM m a")
case "$pico" in
    1|2) printf 'Pico de concorrência observado: %s\n' "$pico" ;;
    *) printf 'Pico fora do pool de dois slots: %s\n' "$pico" >&2; exit 1 ;;
esac
igual "$(sql pricing "SELECT quantidade, preco_medio, preco_minimo, preco_maximo
    FROM precos_diarios WHERE data_referencia = '2025-01-02' AND categoria = 'livros'")" "2|15.00|10.00|20.00"

original=$(publicacao)
for repeticao in 1 2; do
    limpar_analise smoke-t06-t07
    aguardar smoke-t06-t07
    igual "$(sql airflow "SELECT clear_number FROM dag_run WHERE run_id = 'smoke-t06-t07'")" "$repeticao"
    igual "$(publicacao)" "$original"
done

pilha exec -T catalogo-teste cp /amostras/quarta_categoria.json /tmp/products
airflow dags trigger "$dag" --run-id smoke-nova-categoria --logical-date 2025-01-03T09:00:00+00:00
aguardar smoke-nova-categoria
igual "$(sql airflow "SELECT count(DISTINCT region_index) FROM task_instance
    WHERE run_id = 'smoke-nova-categoria' AND task_id = 'analise.resumir_categoria'
    AND state = 'success' AND region_index >= 0")" 4
igual "$(sql pricing "SELECT count(*) FROM precos_diarios WHERE data_referencia = '2025-01-03'")" 4
igual "$(sql pricing "SELECT preco_medio FROM precos_diarios WHERE data_referencia = '2025-01-03' AND categoria = 'casa'")" 4.50
printf 'T06 e T07 passaram. Três e quatro categorias mapeadas, pico de %s e dois reprocessamentos idênticos.\n' "$pico"

if [ "${1:-}" != "--falhas" ]; then
    exit 0
fi

consulta_dia() {
    sql pricing "SELECT json_build_array(
        (SELECT json_agg(d ORDER BY categoria) FROM precos_diarios d WHERE data_referencia = '2025-01-04'),
        (SELECT json_agg(h ORDER BY run_id, categoria) FROM precos_historico h WHERE data_referencia = '2025-01-04')
    )"
}

pilha stop catalogo-teste
airflow dags trigger "$dag" --run-id smoke-api-transitoria --logical-date 2025-01-04T09:00:00+00:00
aguardar_retry smoke-api-transitoria ingestao.buscar_produtos
pilha start catalogo-teste
aguardar smoke-api-transitoria
igual "$(sql airflow "SELECT max(try_number) FROM task_instance
    WHERE run_id = 'smoke-api-transitoria' AND task_id = 'ingestao.buscar_produtos' AND state = 'success'")" 2
confirmar_log smoke-api-transitoria ingestao.buscar_produtos etapa_retentativa
confirmar_log smoke-api-transitoria ingestao.buscar_produtos etapa_concluida try_number=2
printf 'API recuperada automaticamente na segunda tentativa.\n'

publicado=$(consulta_dia)
pilha exec -T catalogo-teste cp /amostras/invalido.json /tmp/products
airflow dags trigger "$dag" --run-id smoke-catalogo-invalido --logical-date 2025-01-04T10:00:00+00:00
aguardar smoke-catalogo-invalido failed
confirmar_log smoke-catalogo-invalido ingestao.validar_produtos etapa_falhou alerta=falha_definitiva
igual "$(consulta_dia)" "$publicado"
printf 'Catálogo inválido rejeitado, com alerta e dados preservados.\n'

# Bloqueia a última escrita para provocar rollback depois de atualizar o snapshot.
pilha exec -T catalogo-teste cp /amostras/valido.json /tmp/products
sql pricing "ALTER TABLE precos_historico ADD CONSTRAINT smoke_bloquear_historico
    CHECK (run_id <> 'smoke-db-transitorio')"
airflow dags trigger "$dag" --run-id smoke-db-transitorio --logical-date 2025-01-04T11:00:00+00:00
aguardar_retry smoke-db-transitorio analise.persistir
igual "$(consulta_dia)" "$publicado"
sql pricing "ALTER TABLE precos_historico DROP CONSTRAINT smoke_bloquear_historico"
aguardar smoke-db-transitorio
confirmar_log smoke-db-transitorio analise.persistir etapa_retentativa
igual "$(sql pricing "SELECT count(*) FROM precos_diarios WHERE data_referencia = '2025-01-04'")" 3
igual "$(sql pricing "SELECT count(*) FROM precos_historico WHERE run_id = 'smoke-db-transitorio'")" 3
printf 'Falha na escrita do histórico desfez o snapshot, e o Airflow publicou na retentativa.\n'

publicado=$(consulta_dia)
pilha stop catalogo-teste
airflow dags trigger "$dag" --run-id smoke-api-fora --logical-date 2025-01-04T12:00:00+00:00
aguardar smoke-api-fora failed 270
igual "$(sql airflow "SELECT count(*) FROM task_instance
    WHERE run_id = 'smoke-api-fora' AND task_id = 'ingestao.buscar_produtos' AND state = 'failed'")" 4
confirmar_log smoke-api-fora ingestao.buscar_produtos etapa_falhou alerta=falha_definitiva try_number=4
igual "$(consulta_dia)" "$publicado"
printf 'API indisponível nas quatro tentativas. Alerta emitido e publicação anterior preservada.\n'
