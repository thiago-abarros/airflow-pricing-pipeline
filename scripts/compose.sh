#!/bin/sh
set -eu

if [ "${PRICING_DEV_CONTAINER:-}" = 1 ]; then
    if ! command -v docker >/dev/null || [ -z "${PRICING_BIND_ROOT:-}" ]; then
        printf '%s\n' 'Use Dev Containers: Rebuild Container no VS Code para habilitar o Docker no terminal integrado.' >&2
        exit 1
    fi
    # Os ensaios informam seus próprios projetos. Os demais comandos usam o
    # projeto do contêiner aberto, mesmo quando /workspace tem outro nome.
    if [ "${1:-}" != "-p" ]; then
        projeto=$(docker inspect --type container --format '{{ index .Config.Labels "com.docker.compose.project" }}' "$(hostname)")
        if [ -z "$projeto" ]; then
            printf '%s\n' 'O contêiner atual precisa fazer parte do projeto Compose.' >&2
            exit 1
        fi
        set -- -p "$projeto" "$@"
    fi
fi

exec docker compose "$@"
