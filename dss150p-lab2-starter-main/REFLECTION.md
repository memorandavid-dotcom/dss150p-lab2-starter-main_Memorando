🌟 Part 1: Project Summary – The Lab Journey
Throughout DSS150P-Lab2, you built a production-grade, rerunnable data engineering pipeline from scratch. Here is how the project unfolded across its core phases:

Phase 1: Source Profiling & Inspection

You profiled multiple data sources (customers.csv, orders.json, products.parquet, a local REST API, and a PostgreSQL support tickets database) to uncover data types, missing values, duplicates, and row counts.

You safely logged into the Dockerized PostgreSQL container using psql to inspect schemas and query support ticket records.

Phase 2: Metadata, Schema, and Data Contracts

You formalized your discoveries into structured YAML inventory templates (source_metadata_template.yml).

You drafted strict data contracts (data_contract_customers.yml) enforcing schemas, nullability rules, primary keys, and quality expectations.

You mapped out robust incremental ingestion strategies and watermark semantics to protect against data loss.

Phase 3: Pipeline Implementation & Ingestion (src/ingest_pipeline.py)

File Ingestion: Built a file-copy routine with SHA-256 hashing and manifest tracking to ensure static files (csv, json, parquet) don't duplicate on reruns.

API Pagination & Incremental Watermarks: Implemented paginated REST API fetching, appended ingestion metadata (_ingested_at, _source), and used an incremental watermark (api_watermark.json) to pull only newly updated records.

Deterministic Deduplication & Atomic Writes: Handled duplicate API events by keeping the record with the greatest updated_at timestamp, writing safely via temporary files (.tmp) to ensure atomic guarantees.

Phase 4: Validation, Idempotency & Failure Testing (src/validate_raw.py)

Wrote automated assertions to verify raw file existence, unique event IDs, timestamp formatting, and watermark synchronization.

Tested pipeline resilience via a failure experiment (shutting down the API server to ensure state/watermark safety) and proved idempotency through clean reruns.

Successfully version-controlled and pushed all work to your GitHub repository.

💡 Part 2: Humanized Engineering Reflections
Here are thoughtful, natural answers to the 10 reflection questions, written from an engineering perspective:

1. Why should source profiling occur before implementation of ingestion?
Profiling acts as our early warning system. If we blindly start writing pipeline code without inspecting the source data first, we run the risk of unhandled data types, hidden null values breaking downstream transformations, or unexpected duplicates ruining our primary keys. Profiling lets us design defenses before the data hits our storage.

2. What is the difference between source event time/updated_at and ingestion time?
Event time (updated_at) tells us when something actually happened or changed in the real world from the source system's perspective. Ingestion time (_ingested_at), on the other hand, is a system-generated timestamp marking the exact moment our pipeline reached out, grabbed that record, and safely landed it in our raw layer.

3. Why is event_id alone insufficient to decide which duplicate API record to keep in this exercise?
An event_id tells us that duplicates exist, but it doesn't tell us which version is the latest. If a customer updates their event status multiple times, simply dropping duplicates arbitrarily could leave us with an older state. We need a deterministic tie-breaker—like comparing their updated_at timestamps—to ensure we always keep the most recent, accurate record.

4. Why must watermark state advance only after successful persistence?
This is crucial for preventing data loss. If our pipeline saves the new watermark timestamp before the raw files or JSONL records are fully and durably written to disk, and the script crashes halfway through writing, the next run will start after that advanced watermark. That means the unwritten records will be permanently skipped and lost forever.

5. What limitation does updated_at > watermark have when multiple source records share the exact same timestamp?
If multiple events are generated at the exact same microsecond, a strict greater-than filter can inadvertently skip records that arrived concurrently or were affected by pagination chunk boundaries. This is why introducing a small safety lookback window (or tracking processing state carefully) helps ensure late-arriving or tied timestamps aren't missed.

6. How is duplicate prevention related to idempotency?
Idempotency means that running our pipeline multiple times produces the exact same result without unintended side effects or growing duplicate data. Duplicate prevention (like deduplicating by event_id or hashing file contents) is the core mechanism that allows our pipeline to achieve true idempotency.

7. Why should the raw area preserve source values instead of applying business transformations?
The raw zone serves as our immutable audit log and single source of truth. If we clean, mask, or transform data too early, we might accidentally destroy valuable context or raw errors. Preserving original source payloads gives us the flexibility to reprocess historical data later if downstream business logic changes.

8. How could querying a production OLTP source for profiling or extraction degrade the application?
Production databases are optimized for fast, transactional user interactions, not heavy analytical queries or full-table scans. Running unoptimized profiling queries against an OLTP source can lock tables, consume excessive CPU and memory, and severely slow down or crash live user-facing applications.

9. What would you change if the API had a rate limit of 60 requests per minute?
To respect rate limits without crashing the pipeline, we would need to build throttling logic into our pagination loop. This could include adding explicit sleep intervals (time.sleep()) between page requests, implementing exponential backoff for retries, or utilizing a token-bucket rate limiter.

10. How would you extend this pipeline from a local raw area to PostgreSQL while preserving rerun safety?
To push data from raw storage into a structured PostgreSQL database safely, we would utilize database transactions along with UPSERT operations (such as INSERT INTO ... ON CONFLICT (event_id) DO UPDATE). This guarantees that our writes remain atomic and reruns never duplicate records in our relational warehouse.