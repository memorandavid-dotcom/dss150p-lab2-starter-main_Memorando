# DSS150P Lab 2 – Ingestion Design Document: Rerun Safety, Watermarking & Idempotency

**Author:** David Memorando  
**Component:** Data Acquisition & Landing Layer (Raw Zone)  
**System Target:** Enterprise Lakehouse Raw Storage

---

## 1. Architectural Overview & Raw Zone Philosophy

The objective of the DSS150P ingestion layer is to land data from disparate source systems (batch files, REST APIs, and relational databases) into a raw landing zone with strict guarantees of:
1. **Immutability:** Source records are stored in their native fidelity without destructive business transformations.
2. **Traceability:** Every landed record or file is augmented with ingestion lineage metadata (`_ingested_at`, `_source`, SHA-256 hashes).
3. **Idempotency & Rerun Safety:** The pipeline can be executed repeatedly without generating duplicate records, corrupting state, or regressing historical counts.

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
+---------------------------------/--------------\----------------------------------+
                                 /                \
                                v                  v
    +------------------------------------+  +---------------------------------------+
    |           RAW LANDING AREA         |  |             STATE STORAGE             |
    |  raw/files/ (immutable copies)     |  |  state/api_watermark.json             |
    |  raw/manifest.json (file SHA-256)  |  |  (advances ONLY after atomic landing) |
    |  raw/api/events.jsonl (atomic tmp) |  |                                       |
    +------------------------------------+  |  pipeline_run_log.csv                 |
                                            |  (persistent observability log)       |
                                            +---------------------------------------+
```

---

## 2. File Ingestion Design (SHA-256 Fingerprinting & Manifest)

### 2.1 The Challenge
Static files in `data/` (`customers.csv`, `orders.json`, `products.parquet`) arrive periodically. Blindly copying files on every pipeline run causes:
- Excessive disk I/O and churn.
- Overwritten timestamps that obscure actual file arrival times.
- Risk of duplicate ingestion if upstream systems re-upload identical files under different filenames.

### 2.2 SHA-256 Manifest Strategy
1. **Content Fingerprint Calculation:** Before moving or recording any file, the pipeline computes a 256-bit SHA-256 checksum over the binary stream in 1MB chunks:
   ```python
   def sha256_file(path: Path) -> str:
       h = hashlib.sha256()
       with path.open('rb') as f:
           for chunk in iter(lambda: f.read(1024 * 1024), b''):
               h.update(chunk)
       return h.hexdigest()
   ```
2. **Duplicate & Modification Detection:**
   - The pipeline checks `raw/manifest.json` for:
     * Does `source_file` already exist with an identical `sha256`? -> **SKIP** (No-op rerun).
     * Does the `sha256` match any previously ingested file? -> **SKIP duplicate content**.
     * Has the hash changed? -> **COPY & RECORD NEW ENTRY** (file was updated upstream).
3. **Manifest Schema (`raw/manifest.json`):**
   ```json
   {
     "source_file": "customers.csv",
     "ingested_at": "2026-10-27T10:55:00.000000+00:00",
     "sha256": "4229a20fde8733740f6d9a4c4bc49029dec0a36c96fce4efc2b50e14e0c2c80b",
     "bytes": 18183
   }
   ```

---

## 3. REST API Incremental Ingestion & Watermark Semantics

### 3.1 Watermark Definition
The watermark represents the maximum high-water timestamp observed and successfully committed to raw storage during prior runs.
- **State File:** `state/api_watermark.json`
- **Initial State:** `{"updated_at": null}` (causes full ingestion from the beginning of history).
- **Subsequent Runs:** `{"updated_at": "2026-08-21T10:00:00"}`.
- **API Request:** `GET /api/events?updated_after=<watermark>&page=1&per_page=50`.

### 3.2 State Advancement Rules (The Gold Rule of Watermarking)
> **Crucial Rule:** The watermark state MUST NEVER advance until the newly fetched records are durably and atomically written to disk in `raw/api/events.jsonl`.
- If the API crashes midway through pagination -> **Watermark is NOT advanced**.
- If the disk runs out of space during file writing -> **Watermark is NOT advanced**.
- On subsequent retry, the pipeline re-requests data from the previous valid watermark, guaranteeing **at-least-once delivery** without data loss.

### 3.3 Timestamp Edge Cases & Defenses

| Edge Case | Failure Mode Without Defense | Ingestion Pipeline Defense |
|---|---|---|
| **Tied Timestamps** | Multiple events occurring at the exact same microsecond (e.g. `10:00:00.000000`). If query uses strict `updated_after > 10:00:00`, subsequent records at `10:00:00` are permanently lost. | **Defense:** Use inclusive lookback window or secondary key ordering. When filtering strictly, retain full state in memory and tie-break across `event_id`. |
| **Out-of-Order / Late Events** | Event mutations in source database committed out of order across concurrent worker threads. | **Defense:** In production, subtract a safety lookback delta (e.g. `watermark - 5 minutes`) and rely on downstream deduplication. |
| **Pagination Race Condition** | New events inserted while pagination loop is traversing page 3 of 5. | **Defense:** Upstream API sorts deterministically by `(updated_at ASC, event_id ASC)` ensuring stable page offsets. |

---

## 4. Deduplication Logic (Deterministic Tie-Breaking)

### 4.1 The Mutation Problem
In event-driven architectures, an entity (e.g. an e-commerce order or user session) updates multiple times:
- `event_id: "EVT-101", updated_at: "2026-08-10T09:00:00", event_type: "cart_update"`
- `event_id: "EVT-101", updated_at: "2026-08-10T14:30:00", event_type: "checkout_started"`

Merely checking `if event_id in seen` would discard the newer version if the older version was processed first!

### 4.2 Deterministic Resolution
The pipeline accumulates existing historical records and newly polled records into a hash map indexed by `event_id`, keeping the record with the strictly greatest `updated_at`:
```python
dedup_dict = {}
for item in all_items:
    eid = item['event_id']
    if eid not in dedup_dict or item['updated_at'] > dedup_dict[eid]['updated_at']:
        dedup_dict[eid] = item

unique_items = sorted(dedup_dict.values(), key=lambda it: (it['updated_at'], it['event_id']))
```
This guarantees:
1. Every `event_id` in `raw/api/events.jsonl` is unique.
2. The preserved record is always the latest canonical state.
3. Reruns produce bit-for-bit identical outputs (pure idempotency).

---

## 5. Atomicity & Crash Consistency

To prevent corrupted or half-written files if the process is terminated (SIGINT/SIGKILL/power failure), writes use the **Atomic Staging Pattern**:
1. Serialize data to a temporary file: `raw/api/events.jsonl.tmp`.
2. Flush and close the file descriptor.
3. Perform an OS-level atomic rename:
   ```python
   temp_file.replace(target_file)
   ```
4. Only upon successful replace, save `state/api_watermark.json`.

---

## 6. Pipeline Observability: Persistent Run Logging

To satisfy data auditing and governance standards, every execution logs its runtime metrics to `pipeline_run_log.csv` matching the schema in `templates/pipeline_run_log_template.csv`:

| Column | Data Type | Description |
|---|---|---|
| `run_id` | UUID | Unique execution run identifier |
| `started_at` | ISO-8601 Timestamp | Execution start time |
| `finished_at` | ISO-8601 Timestamp | Execution completion time |
| `status` | String | `SUCCESS` or `FAILED` |
| `source` | String | `files`, `api`, or `unified` |
| `records_read` | Integer | Total records fetched from source |
| `records_written` | Integer | Net records persisted to raw zone |
| `duplicates_removed`| Integer | Duplicate records/files filtered out |
| `watermark_before` | Timestamp / None | Watermark at start of run |
| `watermark_after` | Timestamp / None | Watermark at conclusion of run |
| `error_message` | String | Empty on success; stack trace summary on failure |

---

## 7. Rerun & Recovery Scenarios

1. **Clean-Room Initial Run:**
   - Starting state: `raw/` and `state/` empty.
   - Outcome: All files ingested; manifest created; API paginated from beginning; watermark initialized to `MAX(updated_at)`.
2. **Second-Run Idempotency Verification:**
   - Starting state: `raw/` and `state/` populated from Run 1.
   - Outcome: File hashes match manifest -> 0 files copied. API polls `updated_after=<watermark>` -> 0 new records. Output files remain bit-for-bit identical; 0 duplicates added.
3. **Failure Recovery Simulation:**
   - Scenario: API server unreachable or network severed midway through ingestion.
   - Outcome: Pipeline logs `FAILED` with descriptive error; watermark is NOT modified; raw file is untouched; running again after API recovery cleanly resumes and advances watermark.
