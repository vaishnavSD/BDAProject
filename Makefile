# =============================================================================
# Smart Inventory Management System — Makefile
# All commands run from the project root.
#
# Usage:
#   make up           — Start the full Docker stack
#   make down         — Stop and remove containers (keep volumes)
#   make destroy      — Stop and remove containers AND volumes
#   make logs         — Tail logs for all services
#   make ps           — Show running containers
#   make kafka-topics — List all Kafka topics
#   make produce      — Run all three CSV producers (requires .env)
#   make verify       — Run the Kafka consumer verification tool
#   make install      — Install Python dependencies locally
# =============================================================================

COMPOSE = docker compose -f docker/docker-compose.yml
PYTHON  = python

.PHONY: up down destroy logs ps kafka-topics produce verify install

up:
	$(COMPOSE) up -d --build
	@echo ""
	@echo "Services:"
	@echo "  Kafka UI       : http://localhost:8080"
	@echo "  Spark Master   : http://localhost:8081"
	@echo "  Spark Worker   : http://localhost:8082"
	@echo "  Airflow        : http://localhost:8085  (admin/admin)"
	@echo "  PostgreSQL     : localhost:5432"

down:
	$(COMPOSE) down

destroy:
	$(COMPOSE) down -v
	@echo "All containers and volumes removed."

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

kafka-topics:
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --list

produce:
	$(PYTHON) data-ingestion/producers/run_all_producers.py

verify:
	$(PYTHON) data-ingestion/producers/consumer_verify.py --max 10

install:
	pip install -r data-ingestion/requirements.txt
	@echo "Dependencies installed."
