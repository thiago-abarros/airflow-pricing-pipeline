"""Mostra o acesso ao Airflow a partir da configuração da API local."""

import json
import os
import sys
from pathlib import Path


def main() -> None:
    endereco, dag_id = sys.argv[1:]
    porta = endereco.rsplit(":", maxsplit=1)[1]
    url = f"http://localhost:{porta}"
    arquivo = os.getenv("AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE") or (
        "/opt/airflow/simple_auth_manager_passwords.json.generated"
    )
    senha = json.loads(Path(arquivo).read_text(encoding="utf-8"))["admin"]
    print(f"Airflow  {url}")
    print(f"DAG      {url}/dags/{dag_id}")
    print("Usuário  admin")
    print(f"Senha    {senha}")


if __name__ == "__main__":
    main()
