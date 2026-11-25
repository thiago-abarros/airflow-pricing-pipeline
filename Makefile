COMPOSE ?= sh scripts/compose.sh
DAG := precos_por_categoria
DEV := $(COMPOSE) --profile dev
DEMO := $(COMPOSE) -p pricing-falhas -f compose.yaml -f tests/compose.falhas.yaml
AIRFLOW_PASSWORD_FILE := /opt/airflow/shared/simple_auth_manager_passwords.json

.DEFAULT_GOAL := help
.PHONY: help airflow trigger status password sql check test notebook up down logs smoke smoke-falhas demo-up demo-down demo-ready demo-invalid demo-offline demo-trigger

help:
	@printf '%s\n' 'make up        Sobe Airflow e PostgreSQL' 'make down      Para a pilha e mantém os volumes' \
	  'make airflow   Mostra os links e o login local do Airflow' \
	  'make status    Mostra a saúde do Airflow ou os serviços do Compose' 'make logs      Acompanha os logs do Airflow' \
	  'make password  Mostra a senha local do admin' 'make trigger   Dispara a DAG' \
	  'make sql       Abre o PostgreSQL analítico' 'make check     Verifica lockfile e lint' \
	  'make test      Roda pytest com PostgreSQL' 'make smoke     Valida T06 e T07 em uma pilha isolada' \
	  'make smoke-falhas Exercita recuperação, rollback e alertas no Airflow' \
	  'make notebook  Executa o notebook e grava o resultado em .artifacts' \
	  'make demo-up   Sobe a pilha isolada para simular falhas' 'make demo-down Encerra a pilha de falhas' \
	  'make demo-ready / demo-invalid / demo-offline Configura a resposta do catálogo de teste' \
	  'make demo-trigger Dispara a DAG na pilha de demonstração'

ifeq ($(PRICING_DEV_CONTAINER),1)
NOTEBOOK_PREFIX :=

trigger:
	uv run --locked airflow dags trigger $(DAG)

status:
	@python scripts/status_airflow.py

password:
	@if [ -s "$(AIRFLOW_PASSWORD_FILE)" ]; then \
	  cat "$(AIRFLOW_PASSWORD_FILE)"; \
	else \
	  printf '%s\n' 'Aguarde a API do Airflow iniciar ou execute make up no terminal da máquina.' >&2; \
	  exit 1; \
	fi

sql:
	psql "$$AIRFLOW_CONN_PRICING_POSTGRES"

check:
	uv lock --check
	uv run --locked ruff check .

test:
	uv run --locked pytest -q

down:
	$(COMPOSE) stop scheduler dag-processor api postgres

else
# A execução do notebook usa o serviço de desenvolvimento em ambas as modalidades.
NOTEBOOK_PREFIX := $(DEV) run --rm -T dev

down:
	$(DEV) down

status:
	$(DEV) ps -a

password:
	@$(COMPOSE) exec -T api sh -ec 'cat "$${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:-/opt/airflow/simple_auth_manager_passwords.json.generated}"'

trigger:
	$(COMPOSE) exec -T scheduler airflow dags trigger $(DAG)

sql:
	$(COMPOSE) exec postgres psql -U pricing -d pricing

check:
	$(DEV) build dev
	$(DEV) run --no-deps --rm -T dev sh -ec 'uv lock --check && uv run --locked ruff check .'

test: build-dev-stack
	$(DEV) run --rm -T dev uv run --locked pytest -q

.PHONY: build-dev-stack
build-dev-stack:
	$(DEV) build init dev

notebook: build-dev-stack
endif

up:
	$(COMPOSE) build init
	$(COMPOSE) up -d --wait api scheduler dag-processor

airflow:
	@endereco=$$($(COMPOSE) port api 8080) && \
	  $(COMPOSE) exec -T api python - "$$endereco" "$(DAG)" < scripts/acesso_airflow.py

logs:
	$(COMPOSE) logs -f api scheduler dag-processor

smoke:
	sh scripts/smoke.sh

smoke-falhas:
	sh scripts/smoke.sh --falhas

demo-up:
	$(DEMO) build init
	$(DEMO) up -d --wait

demo-down:
	$(DEMO) down

demo-ready:
	$(DEMO) exec -T catalogo-teste cp /amostras/valido.json /tmp/products

demo-invalid:
	$(DEMO) exec -T catalogo-teste cp /amostras/invalido.json /tmp/products

demo-offline:
	$(DEMO) exec -T catalogo-teste rm -f /tmp/products

demo-trigger:
	$(DEMO) exec -T scheduler airflow dags trigger $(DAG)

notebook:
	mkdir -p .artifacts
	$(NOTEBOOK_PREFIX) uv run --locked --group analytics jupyter nbconvert \
	  --to notebook --execute --ExecutePreprocessor.timeout=90 \
	  --output-dir=/workspace/.artifacts analytics/precos.ipynb
