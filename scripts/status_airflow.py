"""Consulta os componentes usados pelo pipeline pela API interna do Airflow."""

import json
import os
import sys
from urllib.parse import urljoin
from urllib.request import urlopen

COMPONENTES = {
    "metadatabase": "PostgreSQL (metadados)",
    "scheduler": "Scheduler",
    "dag_processor": "Processador de DAGs",
}


def main() -> int:
    url_execucao = os.environ["AIRFLOW__CORE__EXECUTION_API_SERVER_URL"]
    url_status = urljoin(url_execucao, "/api/v2/monitor/health")
    try:
        with urlopen(url_status, timeout=5) as resposta:
            saude = json.load(resposta)
    except (OSError, ValueError) as erro:
        print(f"API do Airflow indisponível ({erro})", file=sys.stderr)
        return 1

    print("API do Airflow          healthy")
    estados = {chave: saude.get(chave, {}).get("status", "desconhecido") for chave in COMPONENTES}
    for chave, nome in COMPONENTES.items():
        print(f"{nome:24} {estados[chave]}")
    return int(any(estado != "healthy" for estado in estados.values()))


if __name__ == "__main__":
    sys.exit(main())
