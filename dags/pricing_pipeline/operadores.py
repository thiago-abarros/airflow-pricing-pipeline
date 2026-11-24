from airflow.sdk import BaseOperator

from pricing_pipeline.catalogo import ler_catalogo, validar_produtos
from pricing_pipeline.observabilidade import log


class ValidarProdutosOperator(BaseOperator):
    """Valida o arquivo da execução e devolve sua referência para a análise."""

    template_fields = ("caminho",)

    def __init__(self, *, caminho, **kwargs):
        super().__init__(**kwargs)
        self.caminho = caminho

    def execute(self, context):
        produtos = validar_produtos(ler_catalogo(self.caminho))
        log.info("catalogo_validado", quantidade_produtos=len(produtos))
        return self.caminho
