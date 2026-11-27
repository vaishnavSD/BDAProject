# =============================================================================
# Smart Inventory Management System — Makefile
# All commands run from the project root.
#
# Usage:
#   make up              — Start the full Docker stack
#   make down            — Stop and remove containers (keep volumes)
#   make destroy         — Stop and remove containers AND volumes
#   make logs            — Tail logs for all services
#   make ps              — Show running containers
#   make kafka-topics    — List all Kafka topics
#   make produce         — Run all three CSV producers
#   make verify          — Run the Kafka consumer verification tool
#   make stream          — Run all three Spark streaming jobs locally
#   make stream-submit   — Submit all streaming jobs via spark-submit (Docker)
#   make install         — Install all Python dependencies locally
# =============================================================================

COMPOSE = docker compose -f docker/docker-compose.yml
PYTHON  = python

.PHONY: up down destroy logs ps kafka-topics produce verify stream stream-submit install

up:
	$(COMPOSE) up -d --build
	@echo ""
	@echo "Services:"
	@echo "  Kafka UI       : http://localhost:8080"
	@echo "  Spark Master   : http://localhost:8081"
	@echo "  Spark Worker   : http://localhost:8082"
	@echo "  Airflow        : http://localhost:8085  (admin/admin)"
	@echo "  FastAPI        : http://localhost:8000"
	@echo "  API Docs       : http://localhost:8000/docs"
	@echo "  Dashboard      : http://localhost:3000"
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
	pip install -r streaming/requirements.txt
	@echo "Dependencies installed."

stream:
	$(PYTHON) streaming/run_streaming.py

stream-submit:
	docker exec -it inventory_spark_master bash /opt/spark-apps/streaming/submit_jobs.sh all

stream-stock:
	docker exec -it inventory_spark_master bash /opt/spark-apps/streaming/submit_jobs.sh stock_aggregator

stream-alerts:
	docker exec -it inventory_spark_master bash /opt/spark-apps/streaming/submit_jobs.sh threshold_detector

stream-shipments:
	docker exec -it inventory_spark_master bash /opt/spark-apps/streaming/submit_jobs.sh shipment_monitor

# ── Batch ─────────────────────────────────────────────────────────────────────
batch-load:
	docker exec -it inventory_spark_master bash /opt/spark-apps/batch/submit_batch_jobs.sh load_reference --mode full

batch-snapshot:
	docker exec -it inventory_spark_master bash /opt/spark-apps/batch/submit_batch_jobs.sh daily_snapshot

batch-overstock:
	docker exec -it inventory_spark_master bash /opt/spark-apps/batch/submit_batch_jobs.sh overstock

batch-supplier:
	docker exec -it inventory_spark_master bash /opt/spark-apps/batch/submit_batch_jobs.sh supplier_perf

batch-turnover:
	docker exec -it inventory_spark_master bash /opt/spark-apps/batch/submit_batch_jobs.sh turnover

batch-all-daily:
	docker exec -it inventory_spark_master bash /opt/spark-apps/batch/submit_batch_jobs.sh all_daily

# ── API ───────────────────────────────────────────────────────────────────────
api-dev:
	cd api && uvicorn api.app.main:app --host 0.0.0.0 --port 8000 --reload

api-test:
	pytest api/tests/ -v

api-logs:
	$(COMPOSE) logs -f api

# ── Dashboard ─────────────────────────────────────────────────────────────────
dashboard-dev:
	cd dashboard && npm start

dashboard-build:
	cd dashboard && npm run build

dashboard-install:
	cd dashboard && npm install --legacy-peer-deps
