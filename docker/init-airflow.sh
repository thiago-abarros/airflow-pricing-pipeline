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
