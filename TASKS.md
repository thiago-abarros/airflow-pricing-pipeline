# Roteiro da atividade

Siga a ordem abaixo para reconstruir ou revisar o projeto. As caixas marcadas indicam etapas verificadas na implementação. Os comandos e o ensaio de falhas estão no [guia de execução](README.md).

## Etapa 1 · Ambiente reproduzível

- [x] **T01 · Fixar o ambiente Python.** Compartilhe Python 3.12, Airflow 3.4.0 e dependências entre desenvolvimento e execução. Verifique com `uv lock --check` e `uv sync --locked`. Concluído quando ambos passam com o lockfile do repositório.
- [x] **T02 · Iniciar os serviços.** Suba PostgreSQL, API, scheduler e processador de DAGs com Docker. Execute `make up` e `make status`. Concluído quando `init` termina com código zero, a UI aceita o login e os resultados continuam disponíveis após reiniciar.
- [x] **T03 · Abrir o Dev Container.** Edite e teste no mesmo ambiente do pipeline. Use **Reopen in Container**, rode `uv run python --version` e edite a descrição da DAG. Concluído quando o Python é 3.12 e o Airflow exibe a edição.

## Etapa 2 · Pipeline obrigatório

- [x] **T04 · Modelar e agendar.** Organize ingestão e análise e inicie a coleta às 06h de Brasília. Na UI, abra o grafo e confira a próxima execução no fuso de São Paulo. Concluído quando os dois grupos aparecem, a DAG está ativa e as execuções diárias seguem o cron.
- [x] **T05 · Tornar a ingestão resiliente.** Colete o catálogo com timeout, retry exponencial e callbacks. Execute `make smoke-falhas` ou os cenários manuais abaixo. Concluído quando uma falha temporária se recupera e uma persistente registra o alerta depois da quarta tentativa.
- [x] **T06 · Mapear as categorias.** Calcule quantidade, média, mínimo e máximo para cada categoria descoberta. Execute `make smoke`. Concluído quando as três categorias da amostra são mapeadas, o pico de concorrência fica em até dois e uma quarta categoria aparece automaticamente. Livros mantém quantidade 2, média 15,00, mínimo 10,00 e máximo 20,00.
- [x] **T07 · Consolidar e gravar.** Publique o snapshot diário de forma atômica usando a conexão `pricing_postgres`. O `make smoke` repete o cálculo e a gravação duas vezes, comparando snapshot e histórico completos. O `make smoke-falhas` rejeita uma escrita no histórico para verificar o rollback do snapshot e a recuperação na retentativa. O `make test` cobre também reprocessamento de execução antiga.
- [x] **T08 · Demonstrar os obrigatórios.** Comprove o caminho da API até o PostgreSQL. Execute `make test` e acompanhe uma execução da API real pela UI. Concluído quando a suíte passa, o grafo mostra o fan-out e o fan-in e a soma das quantidades corresponde ao catálogo da execução.

## Etapa 3 · Extras e entrega

- [x] **T09 · Validar produtos antes da análise.** Rejeite dados inválidos com `ValidarProdutosOperator`. Dispare a amostra de preço negativo depois de publicar a amostra válida. Concluído quando a validação falha, a persistência fica em `upstream_failed` e o snapshot anterior se mantém.
- [x] **T10 · Manter o histórico.** Preserve uma observação por execução e categoria. Execute `uv run pytest -q tests/test_postgres.py` no Dev Container. Concluído quando uma nova execução acrescenta observações, o reprocessamento preserva a contagem e uma falha na segunda tabela desfaz também a alteração na primeira.
- [ ] **T11 · Entregar à disciplina.** Permita que o avaliador reproduza a atividade a partir do repositório.
  - [x] Documentar como iniciar, acessar, testar e reprocessar o pipeline.
  - [ ] Publicar os commits desta revisão e conferir o acesso de leitura. A [versão publicada da atividade](https://github.com/thiago-abarros/airflow-pricing-pipeline/tree/feature/pipeline-precos) segue acessível sem login.
  - [ ] Postar o link no canal da disciplina e conferir a confirmação de envio.

## Consumo analítico

- [x] Execute `make notebook` sobre o snapshot publicado e explore os preços por categoria. Concluído quando o notebook gera a tabela e o gráfico com a conta de leitura e consulta o histórico por categoria.

## Ensaio de ingestão resiliente

`make demo-up` inicia outro Airflow em `http://localhost:18080`, com banco separado. O servidor de demonstração responde 404 até que um catálogo seja disponibilizado. Use `make demo-trigger` para criar uma execução.

1. Quando a busca entrar em `up_for_retry`, execute `make demo-ready`. A próxima tentativa deve coletar a amostra válida e a execução deve concluir.
2. Rode `make demo-invalid` e `make demo-trigger`. A validação deve rejeitar o preço negativo antes da persistência.
3. Rode `make demo-offline` e `make demo-trigger`. Após quatro tentativas, confira o alerta de falha definitiva nos logs da tarefa `ingestao.buscar_produtos`.
4. Encerre o ensaio com `make demo-down`.
