# Hasaki.vn — Data Platform Problem Statement

> Business context: Hasaki.vn is a beauty & cosmetics retail chain operating
> both online (website/app) and offline (nationwide stores).

---

## Business Goal

> "Leadership needs a dashboard showing revenue by day/week/month, by sales channel
> (online/offline), by product category, by region — and the ability to forecast inventory."

---

## Source Systems

| System | Technology | Data |
|---|---|---|
| ERP | Oracle | Orders, inventory, purchase orders |
| Website | MySQL | Sessions, cart events, checkout |
| POS | POS System | In-store transactions |
| CRM | Internal | Customer profiles, loyalty points |
| Analytics | Google Analytics | Web traffic, conversion funnel |

---

## KPIs to Answer

**Revenue & Sales**
- Revenue today vs yesterday vs same period last month
- Top 20 best-selling products this week by channel
- Return rate by product category

**Customer**
- Customer retention rate by monthly cohort
- Purchase behavior of VIP customers (top 10% GMV)
- Average LTV (Lifetime Value) by acquisition channel

**Inventory**
- Products approaching stockout (< 7 days of stock remaining)
- Stockout rate by store

---

## Data Model — Star Schema (Gold Layer)

```
                    dim_date
                       │
dim_customer ──── fact_orders ──── dim_product
                       │                │
                  dim_channel      dim_category
                       │
                  dim_store
```

### fact_orders

| Column | Type | Description |
|---|---|---|
| order_id | BIGINT | Primary key |
| order_date_key | INT | FK → dim_date |
| customer_key | INT | FK → dim_customer |
| product_key | INT | FK → dim_product |
| store_key | INT | FK → dim_store |
| channel_key | INT | FK → dim_channel (online / offline) |
| quantity | INT | |
| unit_price | DECIMAL | |
| discount_amount | DECIMAL | |
| gmv | DECIMAL | quantity × unit_price |
| revenue | DECIMAL | gmv − discount |
| cost | DECIMAL | |
| gross_profit | DECIMAL | revenue − cost |
| is_returned | BOOLEAN | |

### dim_product

| Column | Type | Description |
|---|---|---|
| product_key | INT | Surrogate key |
| product_id | STRING | Natural key from source |
| product_name | STRING | |
| brand | STRING | |
| category_l1 | STRING | e.g. "Skincare" |
| category_l2 | STRING | e.g. "Moisturizer" |
| category_l3 | STRING | e.g. "Gel cream" |
| supplier | STRING | |
| cost_price | DECIMAL | |

### dim_customer

| Column | Type | Description |
|---|---|---|
| customer_key | INT | Surrogate key |
| customer_id | STRING | Natural key from CRM |
| tier | STRING | Bronze / Silver / Gold / VIP |
| gender | STRING | |
| age_group | STRING | 18-24, 25-34, 35-44, 45+ |
| province | STRING | |
| acquisition_channel | STRING | |
| first_order_date | DATE | |

---

## Architecture — 2 Parallel Streams

```
Oracle / POS / Website
        │
        ▼
      Kafka                        ← ingestion hub, all events pass through here
        │
        ├──────────────────────────────────────┐
        │                                      │
        ▼                                      ▼
   Spark Structured                      ClickHouse
   Streaming                             Kafka Engine
        │                                      │
        ▼                                      │
  HDFS Bronze (raw)                      realtime insert
        │                                 (latency < 1s)
        ▼                                      │
  HDFS Silver (cleaned)                        │
        │                                      │
        ▼                                      │
  HDFS Gold (star schema) ── Spark batch ──────┘
  source of truth            sync every hour/day
        │
        ▼
  Metabase / Superset        ← BI dashboards for business
```

### Stream 1 — Batch (accurate, complete)

| Step | Component | Description |
|---|---|---|
| Ingest | Kafka → Spark Streaming | Consume events, write to Bronze |
| Bronze | HDFS `/data/bronze/` | Raw data, no transformation, JSON/CSV |
| Silver | HDFS `/data/silver/` | Cleaned, deduplicated, Parquet, partitioned |
| Gold | HDFS `/data/gold/` | Star schema, aggregated, business-ready |
| Sync | Spark batch → ClickHouse | Hourly/daily export to serving layer |
| Serve | ClickHouse → Metabase | Interactive BI queries |

### Stream 2 — Realtime (fast, for live dashboards)

| Step | Component | Description |
|---|---|---|
| Ingest | Kafka | Events published by source systems |
| Consume | ClickHouse Kafka Engine | Directly consumes topic, no Spark needed |
| Store | ClickHouse staging table | Raw realtime inserts |
| Serve | ClickHouse → Metabase | Live dashboard (< 1s latency) |

### Trade-offs

| | Batch (via HDFS) | Realtime (direct to ClickHouse) |
|---|---|---|
| Latency | Hours | < 1 second |
| Accuracy | High (dedup, clean) | Lower (raw events) |
| Use case | End-of-day reports | Live order monitoring |
| Hasaki example | Yesterday's revenue report | Orders being processed right now |

---

## Medallion Architecture — Layer Responsibilities

| Layer | Path | Format | Description |
|---|---|---|---|
| Bronze | `/data/bronze/` | JSON / CSV | Raw from source, no transformation |
| Silver | `/data/silver/` | Parquet | Cleaned, deduped, schema standardized |
| Gold | `/data/gold/` | Parquet + partitioned | Star schema, aggregated, BI-ready |

---

## Full Tech Stack

| Component | Technology | Role |
|---|---|---|
| Source | Oracle, MySQL, POS | OLTP, source of truth |
| Streaming | Apache Kafka | Event ingestion, pub/sub decoupling |
| Compute | Apache Spark | Batch ETL + Structured Streaming |
| Storage | HDFS | Distributed file storage |
| Catalog | Hive Metastore + PostgreSQL | Table schema, partition metadata |
| Table format | Apache Iceberg | ACID, time travel, schema evolution |
| Orchestration | Apache Airflow | Pipeline scheduling (2AM daily) |
| Serving | ClickHouse | OLAP, sub-second queries for BI |
| BI | Metabase / Superset | Dashboards for business users |
| Governance | Apache Ranger / Atlas | Access control, data lineage |

---

## Build Roadmap

### Phase 1 — Foundations *(current)*
- [x] Setup HDFS + Hive Metastore (docker-compose)
- [ ] Create HDFS folder structure (`/data/bronze`, `/data/silver`, `/data/gold`)
- [ ] Write first Spark job: load fake orders → Bronze

### Phase 2 — Bronze → Silver Pipeline
- [ ] Generate fake data: 100k orders, 10k customers, 5k products (Hasaki-like)
- [ ] Bronze job: ingest raw CSV → HDFS `/data/bronze/orders/`
- [ ] Silver job: clean + deduplicate + standardize → Parquet, partitioned by date

### Phase 3 — Silver → Gold (Star Schema)
- [ ] Build `dim_date` (static, generate 5 years)
- [ ] Build `dim_product` from product master
- [ ] Build `dim_customer` with SCD Type 2 (track tier changes over time)
- [ ] Build `fact_orders`: join all dimensions

### Phase 4 — Orchestration + Serving
- [ ] Airflow DAG: run pipeline daily at 2AM
- [ ] Add Apache Iceberg for ACID + time travel on Gold tables
- [ ] Export Gold → ClickHouse via Spark JDBC
- [ ] Connect Metabase to ClickHouse

### Phase 5 — Streaming *(advanced)*
- [ ] Add Kafka: source systems publish order events to topics
- [ ] Spark Structured Streaming: Kafka → Bronze (realtime)
- [ ] ClickHouse Kafka Engine: consume topic directly for live dashboard
