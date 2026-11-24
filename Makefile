COMPOSE ?= docker compose
DAG := precos_por_categoria
DEV := $(COMPOSE) --profile dev
DEMO := $(COMPOSE) -p pricing-falhas -f compose.yaml -f tests/compose.falhas.yaml

.PHONY: help up down status logs password trigger sql check test notebook smoke smoke-falhas demo-up demo-down demo-ready demo-invalid demo-offline demo-trigger
help:
	@printf '%s\n' 'make up        Sobe Airflow e PostgreSQL' 'make down      Para os serviços e mantém os volumes' \
	  'make status    Mostra o estado dos serviços' 'make logs      Acompanha os logs do Airflow' \
	  'make password  Mostra a senha local do admin' 'make trigger   Dispara a DAG' \
	  'make sql       Abre o PostgreSQL analítico' 'make check     Verifica lockfile e lint' \
	  'make test      Roda pytest com PostgreSQL' 'make smoke     Valida T06 e T07 em uma pilha isolada' \
	  'make smoke-falhas Exercita recuperação, rollback e alertas no Airflow' \
	  'make notebook  Executa o notebook e grava o resultado em .artifacts' \
	  'make demo-up   Sobe a pilha isolada para simular falhas' 'make demo-down Encerra a pilha de falhas'

up:
	$(COMPOSE) build init
	$(COMPOSE) up -d --wait

down:
	$(DEV) down

status:
	$(DEV) ps -a

logs:
	$(COMPOSE) logs -f api scheduler dag-processor

password:
	$(COMPOSE) exec -T api cat /opt/airflow/simple_auth_manager_passwords.json.generated

trigger:
	$(COMPOSE) exec -T scheduler airflow dags trigger $(DAG)

sql:
	$(COMPOSE) exec postgres psql -U pricing -d pricing

check:
	$(DEV) build dev
	$(DEV) run --no-deps --rm -T dev sh -ec 'uv lock --check && uv run --locked ruff check .'

test:
	$(DEV) build init dev
	$(DEV) run --rm -T dev uv run --locked pytest -q

notebook:
	$(DEV) build init dev
	mkdir -p .artifacts
	$(DEV) run --rm -T dev uv run --locked --group analytics jupyter nbconvert \
	  --to notebook --execute --ExecutePreprocessor.timeout=90 \
	  --output-dir=/workspace/.artifacts analytics/precos.ipynb

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
