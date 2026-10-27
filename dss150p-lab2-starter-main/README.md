# DSS150P Weeks 2–3 Laboratory Package: Production-Grade Ingestion & Profiling Engine

**Author:** David Memorando  
**Course:** DSS150P Data Engineering  
**Academic Term:** 2026  

---

## 📌 Executive Summary

This repository implements a production-grade, verifiable, and idempotent data engineering ingestion and profiling engine. The project acquires data across five heterogeneous source formats—**CSV**, **JSON**, **Parquet**, **REST API**, and **PostgreSQL OLTP**—and lands them into an immutable raw zone with strict guarantees of idempotency, cryptographic deduplication, atomic writes, watermark synchronization, and runtime auditing.

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

## 📂 Repository Directory Layout

```
.
├── .gitignore                      # Excludes raw/, state/, logs, venv, and .env
├── docker-compose.yml              # Local PostgreSQL 16 service
├── requirements.txt                # Python dependencies (pandas, pyarrow, requests, psycopg, PyYAML)
├── config/                         # Data contracts and metadata catalogs
│   ├── data_contract_customers.yml # Formal contract for customer CSV data
│   ├── data_contract_api_events.yml# Formal contract for REST API events
│   ├── source_metadata_template.yml# Master multi-source inventory catalog
│   ├── source_metadata_customers.yml
│   ├── source_metadata_orders.yml
│   ├── source_metadata_products.yml
│   ├── source_metadata_api.yml
│   └── source_metadata_postgres.yml
├── data/                           # Source test data files
│   ├── customers.csv
│   ├── orders.json
│   ├── products.parquet
│   ├── products_optional_compare.csv
│   ├── products_optional_compare.json
│   └── api_events.json
├── docs/                           # Comprehensive documentation & verification evidence
│   ├── INGESTION_DESIGN.md         # Detailed watermarking, deduplication & failure design
│   ├── PROFILING_REPORT.md         # Full profiling report for all 5 sources
│   └── EVIDENCE.md                 # Verbatim CLI execution transcripts & test logs
├── sql/
│   └── seed_support_tickets.sql    # DDL & 250 records for PostgreSQL support tickets
├── src/                            # Production Python code
│   ├── ingest_pipeline.py          # Rerunnable ingestion pipeline with run logging
│   ├── local_api_server.py         # Mock paginated REST API server daemon
│   ├── profile_api.py              # Multi-page REST API profiler
│   ├── profile_csv.py              # Comprehensive CSV profiler
│   ├── profile_json.py             # Comprehensive JSON profiler
│   ├── profile_parquet.py          # Parquet schema and storage efficiency profiler
│   ├── profile_postgres.py         # PostgreSQL support tickets inspection profiler
│   ├── profile_sources.py          # Master profiler orchestrating all sources
│   ├── test_rerun_safety.py        # Automated clean-room, second-run & failure test suite
│   └── validate_raw.py             # 8-point automated raw validation suite
└── templates/
    ├── pipeline_run_log_template.csv
    └── profiling_report_template.md
```

---

## 🚀 Quick Start Guide

### Step 1: Environment Setup
```bash
python -m venv .venv
# On Windows PowerShell:
.venv\Scripts\Activate.ps1
# On Linux/macOS:
source .venv/bin/activate

pip install -r requirements.txt
```

### Step 2: Start PostgreSQL Database (Optional)
```bash
docker compose up -d
docker exec -i dss150p-lab2-postgres psql -U dss150p -d dss150p < sql/seed_support_tickets.sql
```
*(Note: If Docker is unavailable, `src/profile_postgres.py` automatically utilizes an embedded deterministic in-memory SQL engine to analyze the seed SQL).*

### Step 3: Run the Source Profilers
```bash
# Terminal A: Start the local API server
python src/local_api_server.py

# Terminal B: Run the master profiler across all 5 sources
python src/profile_sources.py
```

### Step 4: Execute the Ingestion Pipeline
```bash
python src/ingest_pipeline.py
```

### Step 5: Run Automated Validation Suite
```bash
python src/validate_raw.py
```

### Step 6: Execute Automated Rerun Safety & Failure Recovery Suite
```bash
python src/test_rerun_safety.py
```

---

## 📊 Source Profiling Summary Matrix

| Source | Format | Rows/Records | Primary Key | Update Pattern | Quality Findings & Ingestion Strategy |
|---|---|---:|---|---|---|
| **Customers** | CSV | 250 rows | `customer_id` | Full Snapshot | 3 missing emails, 2 missing cities, 2 exact duplicate rows. Ingest raw as-is; deduplicate downstream. |
| **Orders** | JSON | 500 records | `order_id` | Append | Root is array of objects; 0 missing keys; 100% unique `order_id`. Land as immutable file copy. |
| **Products** | Parquet | 120 rows | `product_id` | Full Snapshot | Native Arrow schema; 0 nulls; 64% compression reduction vs JSON (14.6KB vs 40.7KB). |
| **Events API** | REST API | 100 events / 5 pages | `event_id` | Incremental Watermark | Same `event_id` updates over time; deduplicate keeping `MAX(updated_at)`. |
| **Support Tickets**| PostgreSQL | 250 rows | `ticket_id` | Incremental / CDC | 4 unassigned tickets (`assigned_agent IS NULL`), 125 unresolved tickets (`resolved_at IS NULL`). |

---

## 🛡️ Rerun Safety, Watermarking & Failure Recovery

Detailed technical specifications are documented in [`docs/INGESTION_DESIGN.md`](docs/INGESTION_DESIGN.md).

### 1. SHA-256 Manifest Deduplication
Before writing to `raw/files/`, the pipeline calculates the file's SHA-256 checksum:
- If filename and hash match `raw/manifest.json`: **Skip copy** (No-op rerun).
- If hash matches an existing payload under a different filename: **Skip duplicate ingestion**.
- If hash is new/modified: **Copy file and update manifest**.

### 2. Incremental Watermark Semantics
- Active state stored in `state/api_watermark.json`.
- State advancement rule: **Watermark advances ONLY after atomic disk write succeeds**.
- If an API request times out or the process is interrupted, the watermark remains at its prior timestamp, guaranteeing **at-least-once delivery** upon restart.

### 3. Event Deduplication Logic
The API contains state mutations where the same `event_id` appears with newer `updated_at` timestamps across pages. The pipeline resolves duplicates deterministically:
$$\text{Preserved Record} = \arg\max_{r \in \text{Records}(e)} (r.\text{updated\_at})$$

### 4. Persistent Run Log (`pipeline_run_log.csv`)
Audited execution metrics are appended on every execution conforming to the required schema:
```csv
run_id,started_at,finished_at,status,source,records_read,records_written,duplicates_removed,watermark_before,watermark_after,error_message
```

---

## ✅ Automated Validation Suite (`src/validate_raw.py`)

The automated validation suite executes 8 strict assertions:
1. **File System Audit:** Verifies `raw/manifest.json`, `raw/api/events.jsonl`, and `state/api_watermark.json` exist.
2. **Persistent Run Log Audit:** Asserts `pipeline_run_log.csv` is present, non-empty, and schema-compliant.
3. **Cryptographic Integrity:** Recalculates SHA-256 for all landed files and asserts exact match against manifest.
4. **Primary Key Uniqueness:** Asserts 0 duplicate `event_id`s in `events.jsonl`.
5. **Lineage Metadata:** Verifies `_ingested_at` and `_source` on every event record.
6. **Timestamp Conformance:** Asserts all datetime fields conform to ISO-8601.
7. **Watermark Synchronization:** Asserts `state/api_watermark.json` strictly equals $\max(\text{updated\_at})$.
8. **Count Monotonicity:** Protects against historical data loss by asserting raw count never regresses between runs.

---

## 📜 Verifiable Execution Evidence

Full verbatim transcripts for clean-room runs, idempotency reruns, and failure simulations are in [`docs/EVIDENCE.md`](docs/EVIDENCE.md).

### Summary of Automated Test Suite Results (`src/test_rerun_safety.py`):
```
>>> TEST 1: Clean-Room Initial Run (Starting from Scratch)...
 [SETUP] Wiped raw/, state/, and pipeline_run_log.csv.
 [PASS] Clean-room pipeline run completed successfully.
 [METRICS] Run 1 landed 6 files, 120 events, watermark: 2026-08-21T10:00:00
 [PASS] Automated validation checks passed (8/8).

>>> TEST 2: Second-Run Idempotency & Duplicate Prevention...
 [PASS] Rerun produced 0 new events, 0 duplicate rows, and identical watermark.
 [PASS] Automated validation checks passed (8/8).

>>> TEST 3: Failure Recovery Simulation...
 [SIMULATION] Intentionally sending requests to invalid port 9999...
 [PASS] Watermark was NOT advanced or corrupted during API failure.
 [PASS] Existing raw data remained 100% intact and uncorrupted.
 [RECOVERY] Restoring API endpoint and verifying clean recovery...
 [PASS] Pipeline recovered cleanly from failure state.
 [PASS] Automated validation checks passed (8/8).

ALL RERUN SAFETY & FAILURE RECOVERY TESTS PASSED (3/3)!
```

---

## 🔒 Git Policy Compliance
Per lab instructions, all runtime-generated directories (`raw/`, `state/`), persistent logs (`pipeline_run_log.csv`), virtual environments (`.venv/`), and secrets (`.env`) are strictly excluded via `.gitignore` and untracked from Git version control.
