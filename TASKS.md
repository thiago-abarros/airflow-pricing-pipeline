# Roteiro de validação da Atividade 01

Use este roteiro na ordem indicada, a partir da raiz do clone. Os comandos `make` podem ser executados no terminal da máquina ou no terminal integrado do Dev Container. As tabelas de demonstração e os ensaios usam projetos Compose separados da pilha principal.

## T01–T03 · Preparar o ambiente e abrir o Dev Container

1. No VS Code, instale a extensão **Dev Containers** e use **Dev Containers: Reopen in Container**. Se o projeto já estiver aberto em uma imagem anterior, use **Dev Containers: Rebuild Container**. O VS Code prepara a imagem de execução, sobe os serviços e sincroniza as dependências fixadas em `uv.lock`.
2. No terminal integrado, confira as ferramentas e a pilha.

   ```sh
   make
   uv run --locked python --version
   docker compose version
   make up
   make status
   make check
   make test
   make airflow
   ```

3. Use o link, o usuário e a senha informados por `make airflow`. A interface deverá abrir a DAG `precos_por_categoria`. No terminal da máquina, `make status` também mostra que o serviço `init` terminou com código zero.
4. O ambiente está pronto quando o Python reportar a versão 3.12, os componentes estiverem `healthy`, o Ruff e o lockfile passarem e a suíte reportar **15 testes aprovados**.

Para iniciar pelo terminal da máquina, use `make up` e `make airflow`. Veja os pré-requisitos e a diferença de comportamento de `make down` nos dois terminais. [README](README.md#executar)

## T04 · Conferir o agendamento e o desenho da DAG

1. Abra o link da DAG mostrado por `make airflow`. Em **Graph**, expanda os grupos `ingestao` e `analise`. O trecho linear vai de `buscar_produtos` a `descobrir_categorias`, passando por `validar_produtos`. O **fan-out** cria uma instância de `resumir_categoria` por categoria. O **fan-in** reúne essas saídas em `persistir`.
2. Confira no código o uso de `@dag`, `@task` e das chamadas das funções para encadear a execução. A DAG começa ativa, com `start_date` ancorado por Pendulum em `America/Sao_Paulo`, cron `0 6 * * *` e `catchup=False`. [Definição da DAG](dags/precos_por_categoria.py)
3. A próxima execução deve aparecer às **06h de Brasília** com o seletor da interface em **Local**, quando o navegador estiver no fuso de Brasília ou Recife. Em UTC, o horário é **09h**. Quem estiver em outro fuso pode conferir diretamente nos metadados.

   ```sh
   sh scripts/compose.sh exec -T postgres psql -U airflow -d airflow -c "
   SELECT dag_id, is_paused,
          next_dagrun_create_after AT TIME ZONE 'America/Sao_Paulo' AS proxima_em_brasilia
   FROM dag WHERE dag_id = 'precos_por_categoria';"
   ```

   Espere `is_paused = f` e o horário `06:00:00`. O teste de contrato em `make test` também verifica o agendamento.

## T05 e T08 · Coletar a API real e rastrear a execução

1. Rode `make trigger` e anote o `run_id` apresentado. `queued` significa que o Airflow aceitou o disparo.
2. Em **Grid**, abra essa execução e aguarde `success`. Nos logs de `ingestao.buscar_produtos`, procure `catalogo_gravado` com `status_http: 200` e `etapa_concluida`. Em `analise.persistir`, procure `publicacao_confirmada` e anote as contagens e a data de referência. Os horários dos logs estão em UTC. Na coleta, o `try/except` registra e relança falhas para o Airflow aplicar as retentativas com backoff. [Coleta do catálogo](dags/pricing_pipeline/catalogo.py)
3. Confira os XComs da última execução manual bem-sucedida com a consulta abaixo. Espere caminhos de arquivo nas tarefas de ingestão, nomes de categorias em `descobrir_categorias` e um resumo pequeno por instância de `resumir_categoria`. O catálogo bruto fica no volume `catalogs`.

   ```sh
   sh scripts/compose.sh exec -T postgres psql -U airflow -d airflow -c "
   WITH ultima AS (
       SELECT run_id FROM dag_run
       WHERE dag_id = 'precos_por_categoria' AND run_type = 'manual' AND state = 'success'
       ORDER BY run_after DESC LIMIT 1
   )
   SELECT t.task_id, count(*) AS xcoms, max(octet_length(x.value::text)) AS maior_json_bytes,
          min(x.value::text) AS exemplo
   FROM xcom_v2 x JOIN task_instance t ON t.id = x.task_instance_id
   WHERE t.dag_id = 'precos_por_categoria' AND t.run_id = (SELECT run_id FROM ultima)
   GROUP BY t.task_id ORDER BY t.task_id;"
   ```

4. Rode `make sql` e consulte o `run_id` anotado em `precos_historico`. Confira a quantidade por categoria e a soma do catálogo registrada em `publicacao_confirmada`.

   ```sql
   SELECT categoria, quantidade, preco_medio, preco_minimo, preco_maximo,
          sum(quantidade) OVER () AS total_produtos
   FROM precos_historico
   WHERE run_id = '<run_id>'
   ORDER BY categoria;
   ```

   A contagem da API real pode variar. Para comparar números fixos, use a amostra de T06.

## T06 · Conferir as métricas e o pool

1. Execute `make smoke`. A pilha temporária começa com três categorias. Em `livros`, a amostra de dois produtos a 10,00 e 20,00 deve produzir quantidade **2**, média **15,00**, mínimo **10,00** e máximo **20,00**.
2. O ensaio confirma três instâncias mapeadas, o pool `ecommerce_pool` com **2 slots** e o pico observado de até duas tarefas simultâneas.
3. O mesmo ensaio alimenta outro catálogo com uma quarta categoria, `casa`. Espere quatro instâncias mapeadas e uma nova linha com média **4,50**. A descoberta de categorias divide os dados em arquivos próprios. Cada instância de `.expand()` lê seu arquivo e calcula em `Decimal`. [Amostras](tests/amostras/) · [Cálculo](dags/pricing_pipeline/metricas.py)

## T07 e T10 · Conferir idempotência, atomicidade e histórico

1. `make smoke` limpa o grupo `analise` **duas vezes na mesma execução**, espera o sucesso e compara todas as colunas de `precos_diarios` e `precos_historico`. Espere a mensagem `dois reprocessamentos idênticos`.
2. Para repetir pela interface, abra a execução original, selecione `analise.descobrir_categorias` e use **Clear**, incluindo as tarefas downstream. Aguarde `success` antes de repetir. Compare as linhas do dia no snapshot e as linhas do mesmo `run_id` no histórico. A tarefa final grava as duas tabelas em uma transação com `PostgresHook` e a conexão `pricing_postgres`, definida em `AIRFLOW_CONN_PRICING_POSTGRES` no Compose. [Persistência](dags/pricing_pipeline/persistencia.py)
3. Em `make sql`, execute as consultas abaixo. A primeira mostra os indicadores do dia mais recente. A segunda mostra as observações por execução. Um novo `make trigger` bem-sucedido acrescenta observações ao histórico e mantém uma linha por data e categoria no snapshot.

   ```sql
   SELECT data_referencia, categoria, quantidade, preco_medio, preco_minimo, preco_maximo
   FROM precos_diarios
   WHERE data_referencia = (SELECT max(data_referencia) FROM precos_diarios)
   ORDER BY categoria;

   SELECT run_id, data_execucao, data_referencia, count(*) AS categorias
   FROM precos_historico
   GROUP BY run_id, data_execucao, data_referencia
   ORDER BY data_execucao DESC;
   ```

4. No ensaio de T05 e T09 abaixo, a falha injetada na escrita do histórico deve desfazer também a alteração do snapshot. O `make test` cobre ainda o reprocessamento de uma execução antiga após uma mais recente.

## T05 e T09 · Reproduzir falhas e confirmar alertas

### Ensaio automatizado

Execute `make smoke-falhas` e aguarde a conclusão. O comando roda os cenários de `make smoke` antes das falhas abaixo, usa os atrasos reais do Airflow e limpa seus próprios contêineres e volumes ao sair.

| Cenário | Resultado esperado |
|---|---|
| API volta após falhar | `ingestao.buscar_produtos` passa por `up_for_retry` e conclui na segunda tentativa. Os logs registram `etapa_retentativa` e `etapa_concluida`. |
| Preço negativo na amostra | `ValidarProdutosOperator` rejeita o catálogo, as tarefas downstream ficam bloqueadas e o snapshot anterior continua igual. |
| Falha na escrita do histórico | A transação desfaz a alteração do snapshot. A retentativa publica as duas tabelas após a remoção da restrição de teste. |
| API permanece indisponível | Uma tentativa inicial e três retentativas com backoff. A execução fica `failed`, o log contém `etapa_falhou` com `alerta=falha_definitiva` e os dados anteriores continuam disponíveis. |

### Demonstração manual na interface

1. Execute `make demo-up` e `make demo-airflow`. Abra a URL da demonstração com as credenciais exibidas. Essa pilha agenda apenas os disparos manuais do ensaio.
2. Execute `make demo-offline` e `make demo-trigger`. Assim que `ingestao.buscar_produtos` ficar `up_for_retry`, execute `make demo-ready`. Aguarde `success`. Consulte o snapshot com `make demo-sql` usando a primeira consulta de T07.
3. Execute `make demo-invalid` e `make demo-trigger`. Abra os logs de `ingestao.validar_produtos` e confira a rejeição. `analise.persistir` deve ficar em `upstream_failed`. Repita a consulta do snapshot para conferir os valores preservados.
4. Execute `make demo-offline` e `make demo-trigger`. Aguarde as quatro tentativas e procure o alerta final nos logs de `ingestao.buscar_produtos`.
5. Encerre com `make demo-down`. Os volumes da demonstração permanecem disponíveis para comparar os resultados em outra sessão. O ensaio automatizado sempre parte de volumes novos.

## Consumo analítico e encerramento

1. Com dados publicados na pilha principal, execute `make notebook`. Abra `.artifacts/precos.ipynb` e confira a tabela e o gráfico do último snapshot. O notebook registra o histórico disponível e exibe a série quando há pelo menos duas datas. A conexão usa o papel `pricing_reader`, com permissão de leitura.
2. Para conferir a persistência do ambiente, execute `make down` e `make up`. Rode a consulta de T07 novamente e confirme que os dados continuam disponíveis. Dentro do Dev Container, esses comandos mantêm o terminal aberto.

## T11 · Entregar à disciplina

- [x] Repositório público com a implementação e o roteiro. [Branch da atividade](https://github.com/thiago-abarros/airflow-pricing-pipeline/tree/feature/pipeline-precos)
- [ ] Após a integração do PR, confira que o link do repositório abre o README da atividade sem login. Enquanto o PR estiver aberto, use o link direto da branch.
- [ ] Poste o link no canal da disciplina e confira a confirmação de envio.
