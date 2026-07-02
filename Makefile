# ============================================================
# Makefile — Hasaki DWH local dev commands
# Requires: make (Linux/macOS native, Windows via chocolatey or Git Bash)
# ============================================================

.PHONY: help up down down-clean ps logs build hdfs-setup hdfs-ls ingest-orders spark-shell

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
	@echo "    make ingest-orders DATE=YYYY-MM-DD   Run Bronze orders ingestion"
	@echo "    make spark-shell                     Open interactive PySpark shell"
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

DATE ?= $(shell date +%Y-%m-%d)

ingest-orders:
	docker exec -e PYTHONPATH=/opt/spark-apps spark \
		/opt/spark/bin/spark-submit \
		--master local[*] \
		/opt/spark-apps/jobs/bronze/ingest_orders.py \
		--source-path file:///opt/spark-apps/data/sample/orders.csv \
		--partition-date $(DATE)

spark-shell:
	docker exec -it -e PYTHONPATH=/opt/spark-apps spark \
		/opt/spark/bin/pyspark --master local[*]
