"""Coleta, armazenamento e particionamento do catálogo."""

import json
import os
from collections import defaultdict
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import perf_counter

import requests

from pricing_pipeline.contratos import Produto
from pricing_pipeline.observabilidade import log


def gravar_atomico(destino: Path, conteudo: bytes) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = None
    try:
        with NamedTemporaryFile(dir=destino.parent, delete=False) as arquivo:
            temporario = Path(arquivo.name)
            arquivo.write(conteudo)
        os.replace(temporario, destino)
    finally:
        if temporario is not None:
            temporario.unlink(missing_ok=True)


def buscar_catalogo(url: str, diretorio: Path, run_id: str) -> str:
    inicio = perf_counter()
    try:
        resposta = requests.get(url, timeout=(5, 30))
        resposta.raise_for_status()
        # HTML ou JSON truncado com status 200 também aciona o retry.
        json.loads(resposta.content)
        subdiretorio = diretorio / sha256(run_id.encode()).hexdigest()
        destino = subdiretorio / sha256(resposta.content).hexdigest() / "catalogo.json"
        gravar_atomico(destino, resposta.content)
    except (requests.RequestException, ValueError, OSError) as erro:
        resposta_com_erro = getattr(erro, "response", None)
        log.error(
            "catalogo_falhou",
            tipo_erro=type(erro).__name__,
            status_http=resposta_com_erro.status_code if resposta_com_erro is not None else None,
        )
        raise
    log.info(
        "catalogo_gravado",
        status_http=resposta.status_code,
        tamanho_bytes=len(resposta.content),
        duracao_ms=round((perf_counter() - inicio) * 1000),
    )
    return str(destino)


def ler_catalogo(caminho: str | Path) -> object:
    with open(caminho, encoding="utf-8") as arquivo:
        return json.load(arquivo, parse_float=Decimal)


def validar_produtos(dados: object) -> list[Produto]:
    if not isinstance(dados, list) or not dados:
        raise ValueError("O catálogo precisa conter uma lista não vazia de produtos")
    produtos = [Produto.da_api(item) for item in dados]
    if len({produto.id for produto in produtos}) != len(produtos):
        raise ValueError("O catálogo contém IDs duplicados")
    return produtos


def caminho_particao(catalogo: Path, categoria: str) -> Path:
    chave = sha256(categoria.encode()).hexdigest()
    return catalogo.parent / f"{catalogo.stem}.particoes" / f"{chave}.json"


def particionar_catalogo(caminho: str | Path) -> list[str]:
    catalogo = Path(caminho)
    precos_por_categoria: dict[str, list[str]] = defaultdict(list)
    for produto in validar_produtos(ler_catalogo(catalogo)):
        precos_por_categoria[produto.categoria].append(str(produto.preco))
    for categoria, precos in precos_por_categoria.items():
        gravar_atomico(caminho_particao(catalogo, categoria), json.dumps(precos).encode())
    return sorted(precos_por_categoria)


def precos_da_categoria(caminho: str | Path, categoria: str) -> list[Decimal]:
    catalogo = Path(caminho)
    particao = caminho_particao(catalogo, categoria)
    # Execuções anteriores guardavam um JSON plano e podem ser reprocessadas pelo XCom antigo.
    if catalogo.name != "catalogo.json" and not particao.exists():
        return [p.preco for p in validar_produtos(ler_catalogo(catalogo)) if p.categoria == categoria]
    return [Decimal(preco) for preco in json.loads(particao.read_bytes())]
