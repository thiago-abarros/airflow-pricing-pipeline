# Roteiro da atividade

Siga a ordem abaixo para reconstruir ou revisar o projeto. As caixas marcadas indicam etapas verificadas na implementação. Os comandos e o ensaio de falhas estão no [guia de execução](README.md).

## Etapa 1 · Ambiente reproduzível

- [x] **T01 · Fixar o ambiente Python.** Compartilhe Python 3.12, Airflow 3.4.0 e dependências entre desenvolvimento e execução. Verifique com `uv lock --check` e `uv sync --locked`. Concluído quando ambos passam com o lockfile do repositório.
- [x] **T02 · Iniciar os serviços.** Suba PostgreSQL, API, scheduler e processador de DAGs com Docker. Execute `docker compose up --build -d` e `docker compose ps -a`. Concluído quando `init` termina com código zero, a UI aceita o login e os resultados continuam disponíveis após reiniciar.
- [x] **T03 · Abrir o Dev Container.** Edite e teste no mesmo ambiente do pipeline. Use **Reopen in Container**, rode `uv run python --version` e edite a descrição da DAG. Concluído quando o Python é 3.12 e o Airflow exibe a edição.

## Etapa 2 · Pipeline obrigatório

- [x] **T04 · Modelar e agendar.** Organize ingestão e análise e inicie a coleta às 06h de Brasília. Na UI, abra o grafo e confira a próxima execução no fuso de São Paulo. Concluído quando os dois grupos aparecem, a DAG está ativa e as execuções diárias seguem o cron.
- [x] **T05 · Tornar a ingestão resiliente.** Colete o catálogo com timeout, retry exponencial e callbacks. Execute os cenários de recuperação e indisponibilidade do README. Concluído quando uma falha temporária se recupera e uma persistente registra o alerta depois da quarta tentativa.
- [x] **T06 · Mapear as categorias.** Calcule quantidade, média, mínimo e máximo para cada categoria descoberta. Use o catálogo de três categorias do ensaio e confira `airflow pools get ecommerce_pool` no scheduler. Concluído quando aparecem três instâncias mapeadas com um slot cada, em um pool de dois slots, e a média de livros é 15,00.
- [x] **T07 · Consolidar e gravar.** Publique o snapshot diário de forma atômica usando a conexão `pricing_postgres`. Limpe a tarefa `analise.persistir` duas vezes na mesma execução, esperando o sucesso entre elas. Concluído quando a consulta SQL devolve as mesmas linhas e valores.
- [x] **T08 · Demonstrar os obrigatórios.** Comprove o caminho da API até o PostgreSQL. Execute `uv run pytest -q` no Dev Container e acompanhe uma execução da API real pela UI. Concluído quando a suíte passa, o grafo mostra o fan-out e o fan-in e a soma das quantidades corresponde ao catálogo da execução.

## Etapa 3 · Extras e entrega

- [x] **T09 · Validar produtos antes da análise.** Rejeite dados inválidos com `ValidarProdutosOperator`. Dispare a amostra de preço negativo depois de publicar a amostra válida. Concluído quando a validação falha, a persistência fica em `upstream_failed` e o snapshot anterior se mantém.
- [x] **T10 · Manter o histórico.** Preserve uma observação por execução e categoria. Execute `uv run pytest -q tests/test_postgres.py` no Dev Container. Concluído quando uma nova execução acrescenta observações, o reprocessamento preserva a contagem e uma falha na segunda tabela desfaz também a alteração na primeira.
- [ ] **T11 · Entregar à disciplina.** Permita que o avaliador reproduza a atividade a partir do repositório.
  - [x] Documentar como iniciar, acessar, testar e reprocessar o pipeline.
  - [x] Publicar a versão final no remoto e conferir o acesso de leitura. O [código da atividade](https://github.com/thiago-abarros/airflow-pricing-pipeline/tree/feature/pipeline-precos) está acessível sem login.
  - [ ] Postar o link no canal da disciplina e conferir a confirmação de envio.
