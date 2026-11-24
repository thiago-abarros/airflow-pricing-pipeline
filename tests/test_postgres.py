import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest
from pricing_pipeline.contratos import ResumoCategoria
from pricing_pipeline.persistencia import salvar_precos


def test_snapshot_historico_replay_fora_de_ordem_e_rollback():
    dsn = os.getenv("AIRFLOW_CONN_PRICING_POSTGRES")
    if not dsn:
        pytest.skip("Execute make test no host ou pytest no Dev Container")

    instante = datetime(2026, 1, 1, 9, tzinfo=UTC)
    dia = instante.date()
    livro = ResumoCategoria("livros", 2, Decimal("15.00"), Decimal("10"), Decimal("20"))
    roupa = replace(livro, categoria="roupas")
    with psycopg.connect(dsn, autocommit=True) as conn:
        for tabela in ("precos_diarios", "precos_historico"):
            conn.execute(f"CREATE TEMP TABLE {tabela} (LIKE public.{tabela} INCLUDING ALL)")
        consulta = "SELECT categoria, quantidade, preco_medio FROM precos_diarios ORDER BY categoria"
        for _ in range(3):
            salvar_precos(conn, [livro, roupa], dia, "primeira", instante)
            assert conn.execute(consulta).fetchall() == [
                ("livros", 2, Decimal("15.00")), ("roupas", 2, Decimal("15.00")),
            ]
            assert conn.execute("SELECT count(*) FROM precos_historico").fetchone() == (2,)

        novo_instante = instante + timedelta(hours=1)
        salvar_precos(conn, [livro], dia, "segunda", novo_instante)
        salvar_precos(conn, [roupa], dia, "primeira", instante)
        esperado = [("livros", 2, Decimal("15.00"))]
        assert conn.execute(consulta).fetchall() == esperado
        select_historico = "SELECT * FROM precos_historico ORDER BY run_id, categoria"
        historico = conn.execute(select_historico).fetchall()
        assert [(linha[0], linha[3]) for linha in historico] == [
            ("primeira", "roupas"), ("segunda", "livros"),
        ]

        # A falha ocorre após a substituição do snapshot, dentro da mesma transação.
        conn.execute("ALTER TABLE precos_historico ADD CHECK (quantidade < 99)")
        with pytest.raises(psycopg.errors.CheckViolation):
            salvar_precos(conn, [replace(roupa, quantidade=99)], dia, "segunda", novo_instante)
        assert conn.execute(consulta).fetchall() == esperado
        assert conn.execute(select_historico).fetchall() == historico


def test_papel_analytics_so_le_metricas():
    dsn = os.getenv("PRICING_ANALYTICS_DSN")
    if not dsn:
        pytest.skip("Execute make test no host ou pytest no Dev Container")

    with psycopg.connect(dsn, autocommit=True) as conn:
        for tabela in ("precos_diarios", "precos_historico"):
            conn.execute(f"SELECT * FROM {tabela} LIMIT 1")
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(f"DELETE FROM {tabela} WHERE false")
