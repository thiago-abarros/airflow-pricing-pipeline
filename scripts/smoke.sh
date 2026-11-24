#!/bin/sh
set -eu

export PRICING_IMAGE="airflow-pricing-smoke:local"
# Uma porta dinâmica evita conflito com a pilha principal e com make demo-up.
export AIRFLOW_DEMO_PORT=0
projeto="pricing-smoke-$$"
dag=precos_por_categoria
data_logica=2025-01-02T09:00:00+00:00

pilha() {
    docker compose -p "$projeto" -f compose.yaml -f tests/compose.falhas.yaml "$@"
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

igual() {
    if [ "$1" != "$2" ]; then
        printf 'Esperado: %s\nRecebido: %s\n' "$2" "$1" >&2
        exit 1
    fi
}

aguardar() {
    for tentativa in $(seq 1 60); do
        estado=$(sql airflow "SELECT state FROM dag_run WHERE run_id = '$1'")
        case "$estado" in
            success) return ;;
            failed) printf 'Execução falhou: %s\n' "$1" >&2; return 1 ;;
        esac
        sleep 2
    done
    printf 'Tempo excedido esperando %s\n' "$1" >&2
    return 1
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
    airflow tasks clear "$dag" -s "$data_logica" -e "$data_logica" \
        -t '^analise\.descobrir_categorias$' -d -y
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
