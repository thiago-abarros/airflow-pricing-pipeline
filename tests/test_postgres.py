import os
from datetime import UTC, datetime
from decimal import Decimal

import psycopg
import pytest

from pricing_pipeline.persistencia import salvar_precos


def test_snapshot_historico_e_rollback():
    dsn = os.getenv("AIRFLOW_CONN_PRICING_POSTGRES")
    if not dsn:
        pytest.skip("Execute no serviço dev para testar com PostgreSQL")

    agora = datetime.now(UTC)
    dia = agora.date()
    livro = dict(categoria="livros", quantidade=2, preco_medio="15.00", preco_minimo="10", preco_maximo="20")
    roupa = dict(livro, categoria="roupas")
    with psycopg.connect(dsn, autocommit=True) as conn:
        for tabela in ("precos_diarios", "precos_historico"):
            conn.execute(f"CREATE TEMP TABLE {tabela} (LIKE public.{tabela} INCLUDING ALL)")
        for _ in range(3):
            salvar_precos(conn, [livro, roupa], dia, "primeira", agora)
        assert conn.execute("SELECT count(*) FROM precos_historico").fetchone() == (2,)
        salvar_precos(conn, [livro], dia, "segunda", agora)
        assert conn.execute("SELECT count(*) FROM precos_historico").fetchone() == (3,)
        snapshot = "SELECT categoria, quantidade, preco_medio FROM precos_diarios"
        assert conn.execute(snapshot).fetchall() == [("livros", 2, Decimal("15.00"))]
        # A falha ocorre depois de o snapshot ter sido substituído.
        conn.execute("ALTER TABLE precos_historico ADD CHECK (quantidade < 99)")
        with pytest.raises(psycopg.errors.CheckViolation):
            salvar_precos(conn, [dict(roupa, quantidade=99)], dia, "segunda", agora)
        assert conn.execute(snapshot).fetchall() == [("livros", 2, Decimal("15.00"))]
        assert conn.execute("SELECT count(*) FROM precos_historico").fetchone() == (3,)
