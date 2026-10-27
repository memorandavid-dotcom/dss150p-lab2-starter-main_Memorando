"""DSS150P Lab 2: Production-Grade Rerunnable Ingestion Pipeline.

Key Architectural Guarantees:
1. File Ingestion with SHA-256 Content Deduplication:
   - Uses cryptographic SHA-256 hashes to detect identical files and prevent duplicate ingestion.
   - Maintains an append/update manifest (raw/manifest.json).
2. Paginated REST API Ingestion with Watermarking:
   - Tracks incremental high-watermark (state/api_watermark.json).
   - Merges historical records with newly fetched records.
   - Deterministically deduplicates by event_id keeping greatest updated_at.
3. Fault-Tolerance & Atomic Persistence:
   - Writes to temporary files (.tmp) before atomic filesystem replace.
   - Advances watermark strictly AFTER durable write succeeds.
4. Persistent Observability & Auditing:
   - Records every run (SUCCESS/FAILED) to pipeline_run_log.csv matching template schema.
5. Robust Error Handling:
   - Handles connection refusal, timeouts, HTTP errors, and I/O failures gracefully.
"""
from pathlib import Path
from datetime import datetime, timezone
import json
import hashlib
import shutil
import uuid
import sys
import logging
import requests

# Root directory configuration
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / 'data'
RAW_DIR = ROOT / 'raw'
STATE_DIR = ROOT / 'state'
RUN_LOG_PATH = ROOT / 'pipeline_run_log.csv'
API_URL = 'http://127.0.0.1:8000/api/events'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("ingest_pipeline")


def utc_now() -> str:
    """Return current UTC time in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    """Compute SHA-256 checksum over binary stream in 1MB chunks."""
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def log_pipeline_run(
    run_id: str,
    started_at: str,
    finished_at: str,
    status: str,
    source: str,
    records_read: int,
    records_written: int,
    duplicates_removed: int,
    watermark_before: str | None,
    watermark_after: str | None,
    error_message: str = ""
):
    """Persist execution metrics into pipeline_run_log.csv."""
    header = (
        "run_id,started_at,finished_at,status,source,"
        "records_read,records_written,duplicates_removed,"
        "watermark_before,watermark_after,error_message\n"
    )
    if not RUN_LOG_PATH.exists():
        RUN_LOG_PATH.write_text(header, encoding='utf-8')

    row = (
        f"{run_id},{started_at},{finished_at},{status},{source},"
        f"{records_read},{records_written},{duplicates_removed},"
        f"{watermark_before or ''},{watermark_after or ''},\"{error_message}\"\n"
    )
    with RUN_LOG_PATH.open('a', encoding='utf-8') as f:
        f.write(row)
    logger.info(f"Logged run {run_id} ({source}) -> Status: {status}")


def load_watermark() -> str | None:
    """Load latest committed watermark from state storage."""
    p = STATE_DIR / 'api_watermark.json'
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding='utf-8'))
        return data.get('updated_at')
    except Exception as e:
        logger.warning(f"Could not read watermark state file: {e}")
        return None


def save_watermark(value: str):
    """Atomically commit watermark value to state storage."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    target = STATE_DIR / 'api_watermark.json'
    temp = STATE_DIR / 'api_watermark.json.tmp'
    temp.write_text(json.dumps({'updated_at': value}, indent=2), encoding='utf-8')
    temp.replace(target)
    logger.info(f"Watermark advanced to: {value}")


def ingest_files(run_id: str) -> bool:
    """Ingest static CSV, JSON, and Parquet files with SHA-256 duplicate prevention."""
    started_at = utc_now()
    files_dir = RAW_DIR / 'files'
    files_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = RAW_DIR / 'manifest.json'

    manifest = []
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        except json.JSONDecodeError:
            manifest = []

    # Map existing files by filename and by content hash
    manifest_by_name = {m['source_file']: m for m in manifest}
    existing_hashes = {m['sha256']: m['source_file'] for m in manifest}

    source_files = sorted([f for f in DATA_DIR.glob('*') if f.is_file()])
    records_read = len(source_files)
    records_written = 0
    duplicates_removed = 0
    changed = False

    logger.info(f"Starting file ingestion across {records_read} candidate files in {DATA_DIR}...")

    try:
        for sf in source_files:
            file_hash = sha256_file(sf)
            file_bytes = sf.stat().st_size

            # Check 1: File already recorded under this filename with identical content hash
            if sf.name in manifest_by_name and manifest_by_name[sf.name].get('sha256') == file_hash:
                logger.info(f"Skipping unchanged file '{sf.name}' (SHA-256 match)")
                duplicates_removed += 1
                continue

            # Check 2: Content hash matches another existing file (duplicate payload prevention)
            if file_hash in existing_hashes and existing_hashes[file_hash] != sf.name:
                logger.info(
                    f"Skipping duplicate content in '{sf.name}' (identical hash to '{existing_hashes[file_hash]}')"
                )
                duplicates_removed += 1
                continue

            # Copy file to raw landing zone
            dest = files_dir / sf.name
            shutil.copy2(sf, dest)
            records_written += 1
            changed = True

            entry = {
                "source_file": sf.name,
                "ingested_at": utc_now(),
                "sha256": file_hash,
                "bytes": file_bytes,
            }
            manifest_by_name[sf.name] = entry
            existing_hashes[file_hash] = sf.name
            logger.info(f"Landed raw file '{sf.name}' ({file_bytes:,} bytes, SHA-256: {file_hash[:8]}...)")

        if changed or not manifest_path.exists():
            manifest_path.write_text(json.dumps(list(manifest_by_name.values()), indent=2), encoding='utf-8')
            logger.info(f"Updated raw manifest at {manifest_path}")

        finished_at = utc_now()
        log_pipeline_run(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            status="SUCCESS",
            source="files",
            records_read=records_read,
            records_written=records_written,
            duplicates_removed=duplicates_removed,
            watermark_before=None,
            watermark_after=None
        )
        return True

    except Exception as e:
        finished_at = utc_now()
        logger.error(f"File ingestion failed: {e}")
        log_pipeline_run(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            status="FAILED",
            source="files",
            records_read=records_read,
            records_written=records_written,
            duplicates_removed=duplicates_removed,
            watermark_before=None,
            watermark_after=None,
            error_message=str(e)
        )
        return False


def fetch_api_page(page: int, per_page: int = 50, updated_after: str | None = None) -> dict:
    """Fetch a single page of events with network error differentiation."""
    params = {'page': page, 'per_page': per_page}
    if updated_after:
        params['updated_after'] = updated_after

    response = requests.get(API_URL, params=params, timeout=15)
    response.raise_for_status()
    return response.json()


def load_existing_api_events() -> list[dict]:
    """Read previously committed events from raw/api/events.jsonl."""
    target_file = RAW_DIR / 'api' / 'events.jsonl'
    existing = []
    if target_file.exists():
        with target_file.open('r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        existing.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    return existing


def ingest_api(run_id: str) -> bool:
    """Paginate REST API, merge history, deduplicate, and atomically commit."""
    started_at = utc_now()
    watermark_before = load_watermark()
    watermark_after = watermark_before
    logger.info(f"Starting API ingestion with active watermark: {watermark_before}")

    existing_events = load_existing_api_events()
    newly_fetched = []
    page = 1
    has_more = True
    total_pages_fetched = 0

    try:
        while has_more:
            logger.info(f"Fetching API page {page} (updated_after={watermark_before})...")
            data = fetch_api_page(page=page, per_page=50, updated_after=watermark_before)
            items = data.get('items', [])
            ingested_time = utc_now()

            for item in items:
                item['_ingested_at'] = ingested_time
                item['_source'] = 'local_api'
                newly_fetched.append(item)

            total_pages_fetched += 1
            has_more = data.get('has_more', False)
            next_page = data.get('next_page')
            if has_more and next_page:
                page = next_page
            elif has_more:
                page += 1

    except requests.exceptions.ConnectionError:
        err_msg = (
            "API Server Connection Refused: Cannot reach http://127.0.0.1:8000. "
            "Ensure the server is running via 'python src/local_api_server.py'."
        )
        logger.error(err_msg)
        log_pipeline_run(
            run_id=run_id,
            started_at=started_at,
            finished_at=utc_now(),
            status="FAILED",
            source="api",
            records_read=len(newly_fetched),
            records_written=0,
            duplicates_removed=0,
            watermark_before=watermark_before,
            watermark_after=watermark_before,
            error_message=err_msg
        )
        return False

    except requests.exceptions.Timeout:
        err_msg = "API Server Timeout: The server took longer than 15 seconds to respond."
        logger.error(err_msg)
        log_pipeline_run(
            run_id=run_id,
            started_at=started_at,
            finished_at=utc_now(),
            status="FAILED",
            source="api",
            records_read=len(newly_fetched),
            records_written=0,
            duplicates_removed=0,
            watermark_before=watermark_before,
            watermark_after=watermark_before,
            error_message=err_msg
        )
        return False

    except requests.exceptions.RequestException as e:
        err_msg = f"HTTP Request Error during pagination: {e}"
        logger.error(err_msg)
        log_pipeline_run(
            run_id=run_id,
            started_at=started_at,
            finished_at=utc_now(),
            status="FAILED",
            source="api",
            records_read=len(newly_fetched),
            records_written=0,
            duplicates_removed=0,
            watermark_before=watermark_before,
            watermark_after=watermark_before,
            error_message=err_msg
        )
        return False

    records_read = len(newly_fetched)
    logger.info(f"Retrieved {records_read} records across {total_pages_fetched} pages.")

    # Combine existing history + new items
    combined_pool = existing_events + newly_fetched

    if not combined_pool:
        logger.info("No records to process or persist.")
        log_pipeline_run(
            run_id=run_id,
            started_at=started_at,
            finished_at=utc_now(),
            status="SUCCESS",
            source="api",
            records_read=0,
            records_written=0,
            duplicates_removed=0,
            watermark_before=watermark_before,
            watermark_after=watermark_before
        )
        return True

    # Deduplicate on event_id keeping greatest updated_at
    dedup_dict = {}
    for item in combined_pool:
        eid = item['event_id']
        if eid not in dedup_dict or item['updated_at'] > dedup_dict[eid]['updated_at']:
            dedup_dict[eid] = item

    unique_items = list(dedup_dict.values())
    unique_items.sort(key=lambda it: (it['updated_at'], it['event_id']))
    duplicates_removed = len(combined_pool) - len(unique_items)
    records_written = len(unique_items)

    # Atomic write to raw/api/events.jsonl via temporary file
    api_raw_dir = RAW_DIR / 'api'
    api_raw_dir.mkdir(parents=True, exist_ok=True)
    target_file = api_raw_dir / 'events.jsonl'
    temp_file = api_raw_dir / 'events.jsonl.tmp'

    try:
        with temp_file.open('w', encoding='utf-8') as f:
            for item in unique_items:
                f.write(json.dumps(item) + '\n')
        temp_file.replace(target_file)
        logger.info(f"Atomically committed {records_written} unique events to {target_file}")
    except Exception as e:
        err_msg = f"Atomic write to disk failed: {e}"
        logger.error(err_msg)
        if temp_file.exists():
            temp_file.unlink()
        log_pipeline_run(
            run_id=run_id,
            started_at=started_at,
            finished_at=utc_now(),
            status="FAILED",
            source="api",
            records_read=records_read,
            records_written=0,
            duplicates_removed=duplicates_removed,
            watermark_before=watermark_before,
            watermark_after=watermark_before,
            error_message=err_msg
        )
        return False

    # Advance watermark ONLY after successful atomic disk commit
    max_updated_at = max(it['updated_at'] for it in unique_items)
    if watermark_before is None or max_updated_at > watermark_before:
        save_watermark(max_updated_at)
        watermark_after = max_updated_at

    finished_at = utc_now()
    log_pipeline_run(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        status="SUCCESS",
        source="api",
        records_read=records_read,
        records_written=records_written,
        duplicates_removed=duplicates_removed,
        watermark_before=watermark_before,
        watermark_after=watermark_after
    )
    return True


def run_pipeline() -> bool:
    """Run full pipeline with overall execution tracking."""
    run_id = str(uuid.uuid4())
    logger.info(f"=== Starting Ingestion Pipeline Run ID: {run_id} ===")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    files_ok = ingest_files(run_id=run_id)
    api_ok = ingest_api(run_id=run_id)

    overall_ok = files_ok and api_ok
    if overall_ok:
        logger.info(f"=== Pipeline Run {run_id} Completed Successfully! ===")
    else:
        logger.warning(f"=== Pipeline Run {run_id} Completed with Errors ===")
    return overall_ok


if __name__ == '__main__':
    success = run_pipeline()
    sys.exit(0 if success else 1)