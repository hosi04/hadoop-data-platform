#!/usr/bin/env bash
# setup_hdfs.sh — Initialize HDFS folder structure for Hasaki DWH
# Run once after `docker-compose up -d` and NameNode is healthy.
#
# Usage:
#   bash scripts/setup_hdfs.sh
#   make hdfs-setup

set -euo pipefail

NAMENODE_CONTAINER="namenode"
HDFS="docker exec ${NAMENODE_CONTAINER} hdfs dfs"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# ── Wait for NameNode to be ready ─────────────────────────────────────────────
log "Waiting for NameNode to be ready..."
until docker exec "${NAMENODE_CONTAINER}" hdfs dfsadmin -report &>/dev/null; do
    sleep 3
    echo -n "."
done
echo ""
log "NameNode is ready."

# ── Medallion layer directories ───────────────────────────────────────────────
log "Creating medallion layer directories..."

for layer in bronze silver gold; do
    for entity in orders customers products stores; do
        $HDFS -mkdir -p "/data/${layer}/${entity}"
        log "  Created /data/${layer}/${entity}"
    done
done

# ── Hive Metastore warehouse directory ────────────────────────────────────────
log "Creating Hive warehouse directory..."
$HDFS -mkdir -p /user/hive/warehouse

# ── Permissions ───────────────────────────────────────────────────────────────
# Open permissions for local dev — restrict in production via Apache Ranger
log "Setting permissions..."
$HDFS -chmod -R 777 /data
$HDFS -chmod -R 777 /user/hive/warehouse

# ── Verify ────────────────────────────────────────────────────────────────────
log "Final structure:"
$HDFS -ls -R /data
echo ""
$HDFS -ls /user/hive/

log "HDFS setup complete."
