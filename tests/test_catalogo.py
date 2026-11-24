import json
from decimal import Decimal
from pathlib import Path

import pytest
import requests

from pricing_pipeline.catalogo import buscar_catalogo, calcular_metricas, listar_categorias, validar_produtos


def test_categorias_e_precos(tmp_path):
    produtos = [
        {"id": 1, "category": "livros", "price": 10},
        {"id": 2, "category": "livros", "price": 20},
        {"id": 3, "category": "roupas", "price": 0},
        {"id": 4, "category": "nova", "price": 1.005},
    ]
    arquivo = tmp_path / "catalogo.json"
    arquivo.write_text(json.dumps(produtos))
    assert listar_categorias(arquivo) == ["livros", "nova", "roupas"]
    assert calcular_metricas(arquivo, "livros") == {
        "categoria": "livros", "quantidade": 2, "preco_medio": "15.00",
        "preco_minimo": "10", "preco_maximo": "20",
    }
    assert calcular_metricas(arquivo, "nova")["preco_medio"] == "1.01"


@pytest.mark.parametrize("campo,valor", [
    ("id", None), ("id", True), ("id", 0), ("category", "  "),
    ("price", -1), ("price", "10"), ("price", False), ("price", Decimal("NaN")),
])
def test_rejeita_produto_invalido(campo, valor):
    produto = {"id": 1, "category": "livros", "price": Decimal("10.00")}
    validar_produtos([produto])
    with pytest.raises(ValueError):
        validar_produtos([dict(produto, **{campo: valor})])


def test_rejeita_catalogo_vazio_e_id_repetido():
    produto = {"id": 1, "category": "livros", "price": 10}
    for catalogo in [[], [produto, produto]]:
        with pytest.raises(ValueError):
            validar_produtos(catalogo)


def test_ingestao_preserva_arquivo_em_falhas(tmp_path, monkeypatch):
    def resposta(status, conteudo):
        resultado = requests.Response()
        resultado.status_code = status
        resultado._content = conteudo
        return resultado

    respostas = iter([resposta(503, b""), resposta(200, b"[1]"), resposta(200, b"html")])
    monkeypatch.setattr(requests, "get", lambda url, timeout: next(respostas))
    with pytest.raises(requests.HTTPError):
        buscar_catalogo("http://catalogo/products", tmp_path, "run")
    assert not list(tmp_path.iterdir())
    arquivo = buscar_catalogo("http://catalogo/products", tmp_path, "run")
    with pytest.raises(ValueError):
        buscar_catalogo("http://catalogo/products", tmp_path, "run")
    assert list(tmp_path.iterdir()) == [Path(arquivo)]
    assert Path(arquivo).read_bytes() == b"[1]"
