"""Publicação transacional dos agregados no PostgreSQL."""

from collections.abc import Sequence
from datetime import date, datetime

from psycopg import Connection

from pricing_pipeline.contratos import ResumoCategoria


def salvar_precos(
    conexao: Connection,
    resumos: Sequence[ResumoCategoria],
    dia: date,
    run_id: str,
    instante: datetime,
) -> None:
    """Atualiza o histórico da execução e publica o snapshot se ela for a mais recente do dia."""
    if not resumos:
        raise ValueError("Nenhuma categoria para persistir")

    linhas = [dict(resumo.para_sql(), dia=dia, run_id=run_id, instante=instante) for resumo in resumos]
    with conexao.transaction(), conexao.cursor() as cursor:
        # Serializa publicações da mesma data, inclusive se houver dois clientes.
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", (dia.toordinal(),))
        cursor.execute(
            """SELECT data_execucao, run_id FROM precos_historico
               WHERE data_referencia = %s
               ORDER BY data_execucao DESC, run_id COLLATE "C" DESC
               LIMIT 1""",
            (dia,),
        )
        mais_recente = cursor.fetchone()
        if mais_recente is None or (instante, run_id) >= mais_recente:
            cursor.execute("DELETE FROM precos_diarios WHERE data_referencia = %s", (dia,))
            cursor.executemany(
                """INSERT INTO precos_diarios
                   (data_referencia, categoria, quantidade, preco_medio, preco_minimo, preco_maximo)
                   VALUES (%(dia)s, %(categoria)s, %(quantidade)s, %(preco_medio)s,
                           %(preco_minimo)s, %(preco_maximo)s)""",
                linhas,
            )

        cursor.execute("DELETE FROM precos_historico WHERE run_id = %s", (run_id,))
        cursor.executemany(
            """INSERT INTO precos_historico
               (run_id, data_execucao, data_referencia, categoria, quantidade,
                preco_medio, preco_minimo, preco_maximo)
               VALUES (%(run_id)s, %(instante)s, %(dia)s, %(categoria)s, %(quantidade)s,
                       %(preco_medio)s, %(preco_minimo)s, %(preco_maximo)s)""",
            linhas,
        )
