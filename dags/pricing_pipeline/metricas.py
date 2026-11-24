"""Agregação de preços de uma categoria."""

from collections.abc import Sequence
from decimal import ROUND_HALF_UP, Decimal

from pricing_pipeline.contratos import ResumoCategoria


def calcular_metricas(categoria: str, precos: Sequence[Decimal]) -> ResumoCategoria:
    quantidade = len(precos)
    if quantidade == 0:
        raise ValueError(f"Categoria sem produtos: {categoria}")
    media = sum(precos, Decimal(0)) / quantidade
    return ResumoCategoria(
        categoria=categoria,
        quantidade=quantidade,
        preco_medio=media.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        preco_minimo=min(precos),
        preco_maximo=max(precos),
    )
