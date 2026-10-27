# DSS150P Lab 2 – Comprehensive Source Profiling Report

**Author:** David Memorando  
**Course:** DSS150P Data Engineering  
**Scope:** Profiling and Acquisition Design for CSV, JSON, Parquet, REST API, and PostgreSQL sources.

---

## 1. Source Inventory

| Source Name | Storage Type | Volume / Records | Primary / Candidate Key | Update Pattern | Critical Quality Findings |
|---|---|---:|---|---|---|
| **Customers** | CSV (`data/customers.csv`) | 250 rows (18,183 bytes) | `customer_id` | Full Snapshot (Batch Drop) | 3 missing emails, 2 missing cities, 2 exact duplicate rows (`customer_id` not unique before dedup) |
| **Orders** | JSON (`data/orders.json`) | 500 records (77,938 bytes) | `order_id` | Append / Incremental | Root structure is a JSON array of objects; 0 nulls across all keys; `order_id` 100% unique |
| **Products** | Parquet (`data/products.parquet`) | 120 rows (14,652 bytes) | `product_id` | Full Snapshot | Embedded Arrow schema; 0 nulls, 0 duplicate rows; 36.0% size of JSON equivalent |
| **Events API** | REST API (`/api/events`) | 100 events across 5 pages | `event_id` | Incremental (State-based) | Same `event_id` appears with multiple timestamps (mutations); requires `MAX(updated_at)` tie-breaker |
| **Support Tickets** | PostgreSQL (`support_tickets`) | 250 rows (24,664 bytes seed) | `ticket_id` | Incremental / CDC | 4 unassigned tickets (`assigned_agent IS NULL`), 125 unresolved tickets (`resolved_at IS NULL`) |

---

## 2. Schema & Structural Findings

### 2.1 Customers CSV (`customers.csv`)
* **Physical vs. Logical Types:**
  - `customer_id`: Physical string (`object`), logical identifier (`C0001` - `C0250`).
  - `first_name`, `last_name`: Text strings.
  - `email`: String formatted as RFC-5322 email. 3 rows contain `NaN`.
  - `city`: Text strings. 2 rows contain `NaN`.
  - `signup_date`: Inferred as string, validated as ISO-8601 (`YYYY-MM-DD`). Span: `2025-01-02` to `2026-06-28`.
  - `customer_segment`: Categorical (`Standard`: 148, `Premium`: 72, `Enterprise`: 30).
* **Key Observations:** The CSV has no embedded type headers; string-to-type parsing is required downstream.

### 2.2 Orders JSON (`orders.json`)
* **Structural Shape:** The root is a well-formed JSON array (`list` of dictionaries).
* **Schema Fields:**
  - `order_id` (String): Unique transaction identifier (`ORD-XXXXX`).
  - `customer_id` (String): Relational reference linking to `customers.csv`.
  - `order_date` (ISO-8601 string): Dates range from `2026-01-01T08:12:00` to `2026-06-30T19:45:00`.
  - `total_amount` (Float): Order monetary value in USD ($12.50 to $1,489.00).
  - `status` (String): Categorical lifecycle state (`Completed`: 340, `Pending`: 98, `Cancelled`: 62).
  - `items_count` (Integer): Count of line items (1 to 12).
* **Key Observations:** Strict key consistency; no missing keys across all 500 JSON objects.

### 2.3 Products Parquet (`products.parquet`)
* **Storage Footprint Analysis:**
  - `products.parquet`: **14,652 bytes** (Baseline 1.0x).
  - `products_optional_compare.csv`: **11,560 bytes** (0.79x for small text, but loses types).
  - `products_optional_compare.json`: **40,686 bytes** (2.78x larger due to repetitive key serialization).
* **Columnar Schema:**
  - `product_id` (int64 / string): Unique product identifier.
  - `product_name` (string): UTF-8 encoded text.
  - `category` (string): Low-cardinality category dictionary.
  - `price` (float64): Floating-point decimal.
  - `cost` (float64): Production cost.
  - `stock_level` (int64): Integer inventory count.
* **Key Observations:** Parquet enforces exact physical types natively at the file level and supports dictionary encoding and snappy compression.

### 2.4 REST API (`/api/events`)
* **Endpoint Envelope:**
  ```json
  {
    "page": 1,
    "per_page": 20,
    "total": 100,
    "has_more": true,
    "next_page": 2,
    "items": [...]
  }
  ```
* **Payload Fields:**
  - `event_id` (string UUID/integer identifier).
  - `customer_id` (string reference).
  - `event_type` (string: `login`, `page_view`, `checkout_started`, `payment_processed`).
  - `updated_at` (ISO-8601 UTC timestamp).
* **Pagination Parameters:** `page` (integer >= 1), `per_page` (1-50), `updated_after` (ISO-8601 cutoff string).

### 2.5 PostgreSQL Support Tickets (`support_tickets`)
* **Table DDL:**
  ```sql
  CREATE TABLE support_tickets (
      ticket_id INTEGER PRIMARY KEY,
      customer_id VARCHAR(10) NOT NULL,
      category VARCHAR(40) NOT NULL,
      priority VARCHAR(10) NOT NULL,
      assigned_agent VARCHAR(80),
      opened_at TIMESTAMP NOT NULL,
      resolved_at TIMESTAMP,
      status VARCHAR(20) NOT NULL
  );
  ```
* **Metrics:**
  - Total records: 250.
  - Unassigned tickets (`assigned_agent IS NULL`): 4 (1.6%).
  - Unresolved tickets (`resolved_at IS NULL`): 125 (50.0%).
  - Status breakdown: `In Progress`: 69, `Resolved`: 64, `Closed`: 61, `Open`: 56.
  - Priority breakdown: `Low`: 92, `High`: 84, `Medium`: 74.
  - Categories: `Account`: 60, `Product`: 51, `Delivery`: 49, `Billing`: 47, `Technical`: 43.

---

## 3. Data Quality Findings

1. **Customers CSV Duplication:**
   - 2 rows are 100% exact duplicates.
   - Consequently, `df['customer_id'].is_unique` evaluates to `False`.
   - Raw zone preservation rule: Ingest raw as-is (do not alter source). Deduplicate downstream in bronze/silver using deterministic hashing.
2. **Missing Contact Data in Customers:**
   - 3 null emails, 2 null cities.
   - Data contract must mark `email` and `city` as nullable (`nullable: true`) in raw, with a quality rule alerting if null count exceeds 5%.
3. **REST API Event Mutations:**
   - Multiple records share the same `event_id` across pages or runs, representing state mutations over time.
   - Strict deduplication logic is required: `GROUP BY event_id ORDER BY updated_at DESC LIMIT 1`.
4. **PostgreSQL Nullable Agents:**
   - 4 tickets have no assigned agent; this is a valid business state (triage stage).

---

## 4. Recommended Acquisition Methods

| Source | Acquisition Method | Frequency | Idempotency / Deduplication Mechanism |
|---|---|---|---|
| **Customers** | Batch File Ingestion | Daily | SHA-256 hash comparison against `raw/manifest.json`. Skip copy if hash matches. |
| **Orders** | Batch File Ingestion | Hourly / Daily | SHA-256 hash manifest comparison. |
| **Products** | Batch File Ingestion | Daily | SHA-256 hash manifest comparison. |
| **Events API** | Paginated REST API with Watermark | Every 5-15 min | Incremental watermark via `updated_after=<watermark>`. Deduplicate in-memory by `event_id` keeping `MAX(updated_at)`. |
| **PostgreSQL** | CDC (Change Data Capture) or Read-Replica Batch | Continuous / Hourly | Log-based replication (Debezium/WAL) or incremental query `WHERE opened_at > :watermark OR resolved_at > :watermark`. |

---

## 5. Risks and Assumptions

1. **OLTP Load Hazard:** Running unindexed profiling queries or full table extractions against the primary PostgreSQL instance can saturate CPU and memory, lock transactional tables, and degrade live user operations. Always extract via read replicas or CDC.
2. **Clock Skew and Timestamp Collisions:** When filtering with `updated_at > watermark`, multiple events generated in the same microsecond or across pagination boundaries can be skipped if a strict `>` filter is used without a safety overlap window or secondary tie-breaker.
3. **API Rate Limiting:** High-frequency polling without throttling risks triggering HTTP 429 (Too Many Requests). A production pipeline requires exponential backoff, retry limits, and rate throttling (e.g., token bucket).
