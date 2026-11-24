# Pipeline de preços com Apache Airflow

Todos os dias às **06h de Brasília**, a DAG `precos_por_categoria` coleta os produtos da FakeStore API e publica no PostgreSQL a quantidade e os preços médio, mínimo e máximo de cada categoria. Para acompanhar a atividade passo a passo, use o [checklist de tarefas](TASKS.md).

## Primeira execução

Na raiz do clone, com o Docker e o Docker Compose v2 instalados, execute os comandos abaixo no terminal da máquina.

```sh
docker compose up --build -d
docker compose ps -a
docker compose exec api cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Abra [o Airflow local](http://localhost:8080), entre como `admin` com a senha do último comando e selecione **precos_por_categoria**. O serviço `init` termina com código zero depois de migrar o banco e configurar o pool. A DAG começa ativa. Para coletar imediatamente, use **Trigger** na interface ou o comando abaixo. Aguarde o estado **success** e abra a visualização **Graph** da execução.

```sh
docker compose exec scheduler airflow dags trigger precos_por_categoria
```

Para encerrar e retomar preservando preços, histórico e catálogos, use `docker compose down` e depois `docker compose up -d`. As credenciais do Compose são de desenvolvimento local. O PostgreSQL é acessível pela rede interna e a UI pela interface de loopback da máquina.

## Desenvolver com VS Code e uv

1. Abra a pasta do clone no VS Code com a extensão **Dev Containers** instalada. Pela paleta de comandos, escolha **Dev Containers: Reopen in Container**.
2. No terminal integrado, execute os comandos abaixo. O workspace fica em `/workspace` e o interpretador em `/opt/venv/bin/python`.

   ```sh
   uv run python --version
   uv lock --check
   uv run pytest -q
   ```

3. Edite a descrição ou uma tarefa em `dags/precos_por_categoria.py` e salve. O processador relê a pasta montada do clone. Confira a mudança na UI e verifique erros de importação com `docker compose exec dag-processor airflow dags list-import-errors` no terminal da máquina.
4. Para acrescentar uma dependência, use `uv add pacote` ou `uv add --dev pacote`. Versione `pyproject.toml` e `uv.lock` juntos, reconstrua os serviços com `docker compose up --build --force-recreate -d` e use **Dev Containers: Rebuild Container** para atualizar o editor.

O Dockerfile parte da imagem do Python 3.12, copia o binário de uv 0.8.17 e usa o mesmo lockfile nas imagens. O estágio `runtime` usa `uv sync --locked --no-dev`. O estágio `development` inclui Git e pytest. Para testar sem abrir o editor, use o terminal da máquina.

```sh
docker compose --profile dev build dev
docker compose --profile dev run --rm dev uv run --locked pytest -q
```

O teste de persistência usa tabelas temporárias na conexão com o PostgreSQL. Ao rodar a suíte diretamente em um host sem `AIRFLOW_CONN_PRICING_POSTGRES`, o pytest indica esse teste como pulado.

## Como os dados percorrem a DAG

| Grupo | Tarefa | Entrada e saída |
|---|---|---|
| Ingestão | `buscar_produtos` | Consulta `GET /products` com timeout. Grava o JSON por execução no volume `catalogs` e retorna o caminho via XCom. Em falha de coleta, o Airflow faz até três retentativas com espera exponencial. |
| Ingestão | `validar_produtos` | O operador customizado exige catálogo não vazio, ID inteiro positivo e único, categoria preenchida e preço numérico finito e não negativo. Retorna a referência ao arquivo validado. |
| Análise | `descobrir_categorias` | Extrai do arquivo a lista de categorias que alimenta o mapeamento dinâmico. |
| Análise | `resumir_categoria` | `.expand()` cria uma instância por categoria. O pool `ecommerce_pool` permite duas em paralelo. Cada instância devolve apenas o resumo. A média usa `Decimal` e é arredondada para duas casas decimais, com meio centavo arredondado para cima. |
| Análise | `persistir` | Espera todas as categorias e usa `PostgresHook` com a conexão `pricing_postgres` para publicar snapshot e histórico na mesma transação. |

O PostgreSQL mantém os metadados do Airflow no banco `airflow` e as métricas no banco `pricing`. Cada dia tem um snapshot da última publicação para aquela data. Uma nova execução acrescenta um registro histórico por categoria. Ao repetir a mesma execução, a gravação substitui apenas o snapshot do dia e as linhas de histórico daquele `run_id`. A DAG processa uma execução de cada vez.

A data de referência é a data lógica convertida para `America/Sao_Paulo`. Para disparos manuais sem data lógica, usa-se o instante do disparo. A UI pode exibir horários em UTC. **09h UTC corresponde a 06h de Brasília**. A FakeStore API disponibiliza o catálogo atual. Repetir apenas a análise usa o arquivo da execução. Repetir a busca captura novamente o conteúdo disponível na API.

## Consultar e reprocessar

No terminal da máquina, abra `docker compose exec postgres psql -U pricing -d pricing` e execute as consultas abaixo.

```sql
SELECT * FROM precos_diarios ORDER BY data_referencia DESC, categoria;
SELECT run_id, data_execucao, categoria, quantidade, preco_medio
FROM precos_historico ORDER BY data_execucao DESC, categoria;
```

Para demonstrar idempotência, anote o `run_id` e os resultados. Na UI, abra **essa execução**, selecione `analise.persistir` e use **Clear** apenas nessa tarefa. Aguarde o sucesso e repita as consultas. O número de linhas e os valores devem coincidir nas duas tabelas. Para repetir a análise a partir do mesmo arquivo, selecione `ingestao.validar_produtos` e inclua as tarefas downstream.

## Demonstrar retries, validação e alerta

O ensaio usa outro projeto Compose, outro banco e um servidor HTTP controlado. Requer Compose 2.24.4 ou superior pelo uso de `!override`. Depois do primeiro build, execute na raiz do repositório, em um terminal Bash ou Zsh da máquina.

```sh
ensaio() { docker compose -p pricing-falhas -f compose.yaml -f tests/compose.falhas.yaml "$@"; }
ensaio up -d
ensaio exec api cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Abra [o Airflow do ensaio](http://localhost:18080) e acompanhe a DAG. Se ainda não houver uma execução, use **Trigger**.

1. **Recuperação.** O servidor começa respondendo 404. Quando a busca ficar `up_for_retry`, execute `ensaio exec catalogo-teste cp /amostras/valido.json /tmp/products`. O retry deve recuperar a execução. Consulte `ensaio exec postgres psql -U pricing -d pricing -c 'TABLE precos_diarios'`. A amostra tem três categorias. Para livros, aguarde quantidade 2, média 15,00, mínimo 10,00 e máximo 20,00.
2. **Validação.** Execute `ensaio exec catalogo-teste cp /amostras/invalido.json /tmp/products` e dispare outra execução. O operador deve rejeitar o preço negativo. A tarefa de persistência ficará em `upstream_failed` e as métricas anteriores devem continuar disponíveis.
3. **Falha definitiva.** Execute `ensaio exec catalogo-teste mv /tmp/products /tmp/products-indisponivel` e dispare uma nova execução. Mantenha a API indisponível por quatro tentativas. Nos logs de `ingestao.buscar_produtos`, confira a mensagem de nova tentativa e, na última, o alerta de falha definitiva.
4. **Encerrar.** Use `ensaio down` para parar apenas o ensaio. Para recomeçá-lo com um banco novo, `ensaio down -v` remove seus volumes e dados de teste.

Referências · [FakeStore API](https://fakestoreapi.com/docs) · [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/3.4.0/) · [uv em Docker](https://docs.astral.sh/uv/guides/integration/docker/).
