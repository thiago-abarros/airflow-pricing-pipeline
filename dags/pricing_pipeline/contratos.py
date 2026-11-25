"""Contratos entre a API, as tarefas do Airflow e o PostgreSQL."""

from collections.abc import Iterable
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import TypedDict


@dataclass(frozen=True, slots=True)
class Produto:
    id: int
    categoria: str
    preco: Decimal

    @classmethod
    def da_api(cls, valor: object) -> "Produto":
        if not isinstance(valor, dict):
            raise ValueError("Produto precisa ser um objeto JSON")
        id_produto = valor.get("id")
        if type(id_produto) is not int or id_produto <= 0:
            raise ValueError("ID precisa ser um inteiro positivo")
        categoria = valor.get("category")
        if not isinstance(categoria, str) or not categoria.strip():
            raise ValueError(f"Categoria inválida no produto {id_produto}")
        preco = valor.get("price")
        if type(preco) not in (int, Decimal) or not Decimal(preco).is_finite() or preco < 0:
            raise ValueError(f"Preço inválido no produto {id_produto}")
        return cls(id_produto, categoria.strip(), Decimal(preco))


class ResumoXCom(TypedDict):
    categoria: str
    quantidade: int
    preco_medio: str
    preco_minimo: str
    preco_maximo: str


@dataclass(frozen=True, slots=True)
class ResumoCategoria:
    categoria: str
    quantidade: int
    preco_medio: Decimal
    preco_minimo: Decimal
    preco_maximo: Decimal

    def para_xcom(self) -> ResumoXCom:
        return {
            "categoria": self.categoria,
            "quantidade": self.quantidade,
            "preco_medio": str(self.preco_medio),
            "preco_minimo": str(self.preco_minimo),
            "preco_maximo": str(self.preco_maximo),
        }

    @classmethod
    def de_xcom(cls, valor: ResumoXCom) -> "ResumoCategoria":
        return cls(
            valor["categoria"],
            valor["quantidade"],
            Decimal(valor["preco_medio"]),
            Decimal(valor["preco_minimo"]),
            Decimal(valor["preco_maximo"]),
        )

    def para_sql(self) -> dict[str, str | int | Decimal]:
        return asdict(self)


def consolidar(categorias: list[str], resultados: Iterable[ResumoXCom]) -> list[ResumoCategoria]:
    resumos = [ResumoCategoria.de_xcom(resultado) for resultado in resultados]
    recebidas = sorted(resumo.categoria for resumo in resumos)
    if not categorias or recebidas != sorted(set(categorias)):
        raise ValueError(f"Resumos esperados para {sorted(categorias)}, recebidos para {recebidas}")
    return resumos
