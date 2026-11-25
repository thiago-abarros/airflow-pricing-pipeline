import json
from decimal import Decimal
from pathlib import Path

import pytest
import requests
from pricing_pipeline.catalogo import (
    buscar_catalogo,
    particionar_catalogo,
    precos_da_categoria,
    validar_produtos,
)
from pricing_pipeline.contratos import consolidar
from pricing_pipeline.metricas import calcular_metricas


def test_mapeamento_le_apenas_a_particao_da_categoria(tmp_path):
    produtos = [
        {"id": 1, "category": " livros ", "price": 10},
        {"id": 2, "category": "livros", "price": 20},
        {"id": 3, "category": "roupas", "price": 0},
        {"id": 4, "category": "../nova", "price": 1.005},
    ]
    arquivo = tmp_path / "catalogo.json"
    arquivo.write_text(json.dumps(produtos))
    categorias = particionar_catalogo(arquivo)
    arquivo.unlink()
    assert categorias == ["../nova", "livros", "roupas"]
    resumos = [
        calcular_metricas(categoria, precos_da_categoria(arquivo, categoria)).para_xcom()
        for categoria in categorias
    ]
    nova, livros, roupas = consolidar(categorias, resumos)
    assert nova.preco_medio == Decimal("1.01")
    assert roupas.preco_minimo == Decimal(0)
    assert livros.para_xcom() == {
        "categoria": "livros", "quantidade": 2, "preco_medio": "15.00",
        "preco_minimo": "10", "preco_maximo": "20",
    }
    with pytest.raises(ValueError, match="Resumos esperados"):
        consolidar(categorias, resumos[:-1])
    with pytest.raises(ValueError, match="Resumos esperados"):
        consolidar(categorias, resumos + resumos[-1:])


def test_le_catalogo_de_execucao_anterior(tmp_path):
    arquivo = tmp_path / "run-anterior.json"
    arquivo.write_text('[{"id": 1, "category": "livros", "price": 10.51}]')
    assert precos_da_categoria(arquivo, "livros") == [Decimal("10.51")]


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


def test_busca_preserva_versoes_do_catalogo(tmp_path, monkeypatch):
    def resposta(status, conteudo):
        resultado = requests.Response()
        resultado.status_code = status
        resultado._content = conteudo
        return resultado

    respostas = iter([
        resposta(503, b""), resposta(200, b"[1]"),
        resposta(200, b"html"), resposta(200, b"[2]"),
    ])
    monkeypatch.setattr(requests, "get", lambda url, timeout: next(respostas))
    with pytest.raises(requests.HTTPError):
        buscar_catalogo("http://catalogo/products", tmp_path, "run")
    assert not list(tmp_path.iterdir())
    primeira = Path(buscar_catalogo("http://catalogo/products", tmp_path, "run"))
    with pytest.raises(ValueError):
        buscar_catalogo("http://catalogo/products", tmp_path, "run")
    segunda = Path(buscar_catalogo("http://catalogo/products", tmp_path, "run"))
    assert list(sorted(tmp_path.rglob("*.json"))) == sorted([primeira, segunda])
    assert primeira.read_bytes() == b"[1]"
    assert segunda.read_bytes() == b"[2]"
