# DSS150P Weeks 2–3 Laboratory Package: Production-Grade Ingestion & Profiling Engine

**Author:** David Memorando  
**Course:** DSS150P Data Engineering  
**Academic Term:** 2026  

---

## 📌 Executive Summary

This repository implements a production-grade, verifiable, and idempotent data engineering ingestion and profiling engine. The project acquires data across five heterogeneous source formats—**CSV**, **JSON**, **Parquet**, **REST API**, and **PostgreSQL OLTP**—and lands them into an immutable raw zone with strict guarantees of idempotency, cryptographic deduplication, atomic writes, watermark synchronization, and runtime auditing.

All source code, configuration files, and documentation reside in the [`dss150p-lab2-starter-main/`](dss150p-lab2-starter-main/) project directory.

---

## 🏛️ Ingestion Architecture & Data Flow

```
+-----------------------------------------------------------------------------------+
|                                 SOURCE SYSTEMS                                    |
|   +--------------------+     +---------------------+     +--------------------+   |
|   | data/customers.csv |     |   data/orders.json  |     | products.parquet   |   |
|   +---------+----------+     +----------+----------+     +---------+----------+   |
|             |                           |                          |              |
|             +--------------------+      |     +--------------------+              |
|                                  |      |     |                                   |
|   +--------------------+         v      v     v                                   |
|   | /api/events (REST) | ----> [ Ingestion Pipeline ]                             |
|   |   (Paginated)      |          src/ingest_pipeline.py                          |
|   +--------------------+         /            \                                   |
|                                 /              \                                  |
|   +--------------------+       /                \                                 |
|   | PostgreSQL DB      |      /                  \                                |
|   |  (support_tickets) | ----+                    \                               |
+---+--------------------+---------------------------\------------------------------+
                                                      \
                                                       \
    +------------------------------------+  +-----------\---------------------------+
    |           RAW LANDING AREA         |  |             STATE STORAGE             |
    |  raw/files/ (immutable copies)     |  |  state/api_watermark.json             |
    |  raw/manifest.json (file SHA-256)  |  |  (advances ONLY after atomic landing) |
    |  raw/api/events.jsonl (atomic tmp) |  |                                       |
    +------------------------------------+  |  pipeline_run_log.csv                 |
                                            |  (persistent observability log)       |
                                            +---------------------------------------+
```

### Key Architectural Guarantees:
1. **Raw Immutability:** Data is ingested without destructive transformations to preserve auditability.
2. **SHA-256 Manifest Tracking:** File ingestion calculates 256-bit cryptographic hashes, preventing duplicate file landing.
3. **High-Watermark Ingestion:** The REST API is polled incrementally (`updated_after=<watermark>`). State updates occur **strictly after** durable disk commits.
4. **Deterministic Deduplication:** In-memory tie-breaking prioritizes the greatest `updated_at` timestamp per `event_id`.
5. **Atomic Disk Writes:** Events are written to `.tmp` files before an atomic filesystem swap (`replace`), preventing torn writes.
6. **Persistent Run Log:** Every execution records start/end timestamps, record counts, and status to `pipeline_run_log.csv`.

---

## 📂 Key Documentation Links

- **[Detailed Ingestion Design](dss150p-lab2-starter-main/docs/INGESTION_DESIGN.md):** Deep dive into watermarking semantics, atomic writes, timestamp collisions, and crash resilience.
- **[Comprehensive Source Profiling Report](dss150p-lab2-starter-main/docs/PROFILING_REPORT.md):** Detailed analysis across CSV, JSON, Parquet, REST API, and PostgreSQL.
- **[Verifiable Execution Evidence & Transcripts](dss150p-lab2-starter-main/docs/EVIDENCE.md):** Verbatim execution logs of clean-room runs, idempotency checks, and failure recovery.
- **[Data Contract: Customers](dss150p-lab2-starter-main/config/data_contract_customers.yml):** Schema, SLAs, quality rules, and change policy for customer data.
- **[Data Contract: REST API Events](dss150p-lab2-starter-main/config/data_contract_api_events.yml):** Schema, envelope, watermark, and SLA specifications for API events.

---

## 🚀 Quick Execution Commands

From `dss150p-lab2-starter-main/`:

```bash
# 1. Profile all 5 sources
python src/profile_sources.py

# 2. Run ingestion pipeline
python src/ingest_pipeline.py

# 3. Run automated raw data validation (8 assertions)
python src/validate_raw.py

# 4. Run automated clean-room, idempotency, and failure-recovery test suite
python src/test_rerun_safety.py
```
