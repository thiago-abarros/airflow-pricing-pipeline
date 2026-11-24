"""Captura do catálogo e cálculo de indicadores, independentes do Airflow."""

import json
import logging
import os
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile

import requests


def buscar_catalogo(url: str, diretorio: Path, run_id: str) -> str:
    destino = diretorio / (sha256(run_id.encode()).hexdigest() + ".json")
    temporario = None
    try:
        resposta = requests.get(url, timeout=(5, 30))
        resposta.raise_for_status()
        # Uma resposta truncada ou HTML com status 200 também aciona o retry.
        json.loads(resposta.content, parse_float=Decimal)
        diretorio.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(dir=diretorio, delete=False) as arquivo:
            temporario = Path(arquivo.name)
            arquivo.write(resposta.content)
        os.replace(temporario, destino)
    except (requests.RequestException, ValueError, OSError):
        logging.exception("Falha ao buscar ou gravar o catálogo da execução %s", run_id)
        raise
    finally:
        if temporario:
            temporario.unlink(missing_ok=True)
    return str(destino)


def ler_catalogo(caminho: str) -> list:
    with open(caminho, encoding="utf-8") as arquivo:
        return json.load(arquivo, parse_float=Decimal)


def validar_produtos(produtos: list) -> None:
    if not isinstance(produtos, list) or not produtos:
        raise ValueError("O catálogo precisa conter uma lista não vazia de produtos")
    ids = set()
    for indice, produto in enumerate(produtos):
        if not isinstance(produto, dict):
            raise ValueError(f"Produto inválido na posição {indice}")
        identificador = produto.get("id")
        if type(identificador) is not int or identificador <= 0 or identificador in ids:
            raise ValueError(f"ID inválido ou repetido na posição {indice}")
        ids.add(identificador)
        categoria = produto.get("category")
        if not isinstance(categoria, str) or not categoria.strip():
            raise ValueError(f"Categoria inválida no produto {identificador}")
        preco = produto.get("price")
        if type(preco) not in (int, Decimal) or not Decimal(preco).is_finite() or preco < 0:
            raise ValueError(f"Preço inválido no produto {identificador}")


def listar_categorias(caminho: str) -> list[str]:
    return sorted({produto["category"].strip() for produto in ler_catalogo(caminho)})


def calcular_metricas(caminho: str, categoria: str) -> dict:
    precos = [
        Decimal(produto["price"])
        for produto in ler_catalogo(caminho)
        if produto["category"].strip() == categoria
    ]
    media = (sum(precos) / len(precos)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    # Strings decimais preservam a precisão no transporte JSON via XCom.
    return {
        "categoria": categoria,
        "quantidade": len(precos),
        "preco_medio": str(media),
        "preco_minimo": str(min(precos)),
        "preco_maximo": str(max(precos)),
    }
