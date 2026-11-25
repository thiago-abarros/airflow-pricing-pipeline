#!/bin/sh
set -eu
umask 077
for key in jwt web; do
    path="/opt/airflow/shared/$key"
    if [ ! -s "$path" ]; then
        python -c 'import secrets; print(secrets.token_hex(32), end="")' > "$path.tmp"
        mv "$path.tmp" "$path"
    fi
done
airflow db migrate
airflow pools set ecommerce_pool 2 "Categorias processadas em paralelo"
# Aplica as permissões também a bancos criados antes do notebook.
python - <<'PYTHON'
import os
from pathlib import Path
import psycopg

metadata_dsn = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
with psycopg.connect(metadata_dsn.replace("postgresql+psycopg://", "postgresql://"), dbname="pricing") as conn:
    conn.execute(Path("/opt/airflow/init-analytics.sql").read_text())
PYTHON
