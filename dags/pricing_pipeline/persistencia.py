from datetime import date, datetime


def salvar_precos(conexao, resumos: list[dict], dia: date, run_id: str, instante: datetime) -> None:
    """Substitui o dia e a contribuição desta execução em uma só transação."""
    if not resumos:
        raise ValueError("Nenhuma categoria para persistir")

    with conexao.transaction(), conexao.cursor() as cursor:
        cursor.execute("DELETE FROM precos_diarios WHERE data_referencia = %s", (dia,))
        cursor.execute("DELETE FROM precos_historico WHERE run_id = %s", (run_id,))
        linhas = [dict(resumo, dia=dia, run_id=run_id, instante=instante) for resumo in resumos]
        cursor.executemany(
            """INSERT INTO precos_diarios
               (data_referencia, categoria, quantidade, preco_medio, preco_minimo, preco_maximo)
               VALUES (%(dia)s, %(categoria)s, %(quantidade)s, %(preco_medio)s,
                       %(preco_minimo)s, %(preco_maximo)s)""",
            linhas,
        )
        cursor.executemany(
            """INSERT INTO precos_historico
               (run_id, data_execucao, data_referencia, categoria, quantidade,
                preco_medio, preco_minimo, preco_maximo)
               VALUES (%(run_id)s, %(instante)s, %(dia)s, %(categoria)s, %(quantidade)s,
                       %(preco_medio)s, %(preco_minimo)s, %(preco_maximo)s)""",
            linhas,
        )
