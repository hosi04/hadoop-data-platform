# ============================================================
# Makefile — Hasaki DWH local dev commands
# Requires: make (Linux/macOS native, Windows via chocolatey or Git Bash)
# ============================================================

.PHONY: help up down down-clean ps logs build hdfs-setup hdfs-ls ingest-orders ingest-order-status-events ingest-customers ingest-products clean-orders clean-customers clean-products spark-shell

help:
	@echo ""
	@echo "  Hasaki DWH — Available commands"
	@echo ""
	@echo "  Docker"
	@echo "    make up                        Start all containers"
	@echo "    make build                     Build custom images (hive, spark)"
	@echo "    make down                      Stop containers (keep volumes)"
	@echo "    make down-clean                Stop and remove all volumes"
	@echo "    make ps                        Show container status"
	@echo "    make logs service=<name>       Tail logs for a container"
	@echo ""
	@echo "  HDFS"
	@echo "    make hdfs-setup                Create /data/bronze,silver,gold structure"
	@echo "    make hdfs-ls path=<hdfs-path>  List HDFS directory"
	@echo ""
	@echo "  Spark Jobs (run inside spark container — avoids WSL2/Docker network issues)"
	@echo "    make ingest-orders DATE=YYYY-MM-DD               Run Bronze orders ingestion"
	@echo "    make ingest-order-status-events DATE=YYYY-MM-DD  Run Bronze order status events ingestion"
	@echo "    make ingest-customers                            Run Bronze customers ingestion (full snapshot)"
	@echo "    make ingest-products                             Run Bronze products ingestion (full snapshot)"
	@echo "    make clean-orders DATE=YYYY-MM-DD                Run Silver orders cleaning"
	@echo "    make clean-customers                             Run Silver customers cleaning (full snapshot)"
	@echo "    make clean-products                              Run Silver products cleaning (full snapshot)"
	@echo "    make spark-shell                                 Open interactive PySpark shell"
	@echo ""

# ── Docker ────────────────────────────────────────────────────────────────────

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

down-clean:
	docker-compose down -v

ps:
	docker-compose ps

logs:
	docker-compose logs -f $(service)

# ── HDFS ──────────────────────────────────────────────────────────────────────

hdfs-setup:
	bash scripts/setup_hdfs.sh

hdfs-ls:
	docker exec namenode hdfs dfs -ls $(path)

# ── Spark Jobs ────────────────────────────────────────────────────────────────
# Jobs run inside the spark container (same Docker network as HDFS datanodes).
# This avoids WSL2 → Docker NAT issues where DataNode internal IPs are unreachable.

ingest-orders:
	@test -n "$(DATE)" || (echo "ERROR: DATE is required, e.g. make ingest-orders DATE=2026-07-01" && exit 1)
	docker exec -e PYTHONPATH=/opt/spark-apps spark \
		/opt/spark/bin/spark-submit \
		--master local[*] \
		/opt/spark-apps/jobs/bronze/ingest_orders.py \
		--source-path file:///opt/spark-apps/data/sample/orders.csv \
		--partition-date $(DATE)


ingest-order-status-events:
	@test -n "$(DATE)" || (echo "ERROR: DATE is required, e.g. make ingest-order-status-events DATE=2026-07-01" && exit 1)
	docker exec -e PYTHONPATH=/opt/spark-apps spark \
		/opt/spark/bin/spark-submit \
		--master local[*] \
		/opt/spark-apps/jobs/bronze/ingest_order_status_events.py \
		--source-path file:///opt/spark-apps/data/sample/order_status_events.csv \
		--partition-date $(DATE)


ingest-customers:
	docker exec -e PYTHONPATH=/opt/spark-apps spark \
		/opt/spark/bin/spark-submit \
		--master local[*] \
		/opt/spark-apps/jobs/bronze/ingest_customers.py \
		--source-path file:///opt/spark-apps/data/sample/customers.csv


ingest-products:
	docker exec -e PYTHONPATH=/opt/spark-apps spark \
		/opt/spark/bin/spark-submit \
		--master local[*] \
		/opt/spark-apps/jobs/bronze/ingest_products.py \
		--source-path file:///opt/spark-apps/data/sample/products.csv


clean-orders:
	@test -n "$(DATE)" || (echo "ERROR: DATE is required, e.g. make clean-orders DATE=2026-07-01" && exit 1)
	docker exec -e PYTHONPATH=/opt/spark-apps spark \
		/opt/spark/bin/spark-submit \
		--master local[*] \
		/opt/spark-apps/jobs/silver/clean_orders.py \
		--partition-date $(DATE)


clean-customers:
	docker exec -e PYTHONPATH=/opt/spark-apps spark \
		/opt/spark/bin/spark-submit \
		--master local[*] \
		/opt/spark-apps/jobs/silver/clean_customers.py


clean-products:
	docker exec -e PYTHONPATH=/opt/spark-apps spark \
		/opt/spark/bin/spark-submit \
		--master local[*] \
		/opt/spark-apps/jobs/silver/clean_products.py


spark-shell:
	docker exec -it -e PYTHONPATH=/opt/spark-apps spark \
		/opt/spark/bin/pyspark --master local[*]
