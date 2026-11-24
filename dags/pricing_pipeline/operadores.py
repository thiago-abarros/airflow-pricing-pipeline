from airflow.sdk import BaseOperator

from pricing_pipeline.catalogo import ler_catalogo, validar_produtos


class ValidarProdutosOperator(BaseOperator):
    """Valida o arquivo da execução e devolve sua referência para a análise."""

    template_fields = ("caminho",)

    def __init__(self, *, caminho, **kwargs):
        super().__init__(**kwargs)
        self.caminho = caminho

    def execute(self, context):
        validar_produtos(ler_catalogo(self.caminho))
        return self.caminho
