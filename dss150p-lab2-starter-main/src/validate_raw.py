"""Comprehensive Automated Validation Suite for Raw Area and Ingestion State.

Assertions verified:
1. Expected raw directories, files, manifest, and watermark exist.
2. Persistent pipeline run log (pipeline_run_log.csv) exists, schema-valid, and non-empty.
3. Cryptographic SHA-256 integrity: verifies files in raw/files/ match manifest hashes.
4. Primary key uniqueness: asserts 0 duplicate event_ids in raw/api/events.jsonl.
5. Ingestion metadata presence: asserts _ingested_at and _source are attached to all events.
6. Timestamp validity: asserts all timestamps are well-formed ISO-8601.
7. Watermark synchronization: asserts state/api_watermark.json equals MAX(updated_at).
8. Monotonicity: asserts historical event counts never regress on reruns.
"""
from pathlib import Path
from datetime import datetime
import json
import csv
import hashlib
import sys

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / 'raw'
STATE_DIR = ROOT / 'state'
RUN_LOG_PATH = ROOT / 'pipeline_run_log.csv'


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def validate_raw():
    print("=" * 70)
    print("AUTOMATED VALIDATION SUITE: RAW AREA & STATE AUDITING")
    print("=" * 70)

    # 1. Assert expected files and directories exist
    manifest_path = RAW_DIR / 'manifest.json'
    api_events_path = RAW_DIR / 'api' / 'events.jsonl'
    watermark_path = STATE_DIR / 'api_watermark.json'

    assert manifest_path.exists(), f"Assertion Error: Missing manifest file at {manifest_path}"
    assert api_events_path.exists(), f"Assertion Error: Missing API events file at {api_events_path}"
    assert watermark_path.exists(), f"Assertion Error: Missing watermark file at {watermark_path}"
    print(" [PASS] 1. Expected raw directories, manifest, and watermark files exist.")

    # 2. Assert Persistent Pipeline Run Log exists and is valid
    assert RUN_LOG_PATH.exists(), (
        f"Assertion Error: Missing persistent pipeline run log at {RUN_LOG_PATH}! "
        "The pipeline must record execution metrics to pipeline_run_log.csv on every run."
    )
    expected_header = [
        "run_id", "started_at", "finished_at", "status", "source",
        "records_read", "records_written", "duplicates_removed",
        "watermark_before", "watermark_after", "error_message"
    ]
    with RUN_LOG_PATH.open('r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader, None)
        assert header == expected_header, (
            f"Assertion Error: pipeline_run_log.csv header mismatch!\nExpected: {expected_header}\nGot: {header}"
        )
        rows = list(reader)
        assert len(rows) > 0, "Assertion Error: pipeline_run_log.csv contains no logged execution runs!"
        for r in rows:
            assert len(r) == len(expected_header), f"Malformed row in pipeline_run_log.csv: {r}"
            assert r[3] in ["SUCCESS", "FAILED"], f"Invalid run status: {r[3]}"
    print(f" [PASS] 2. Persistent pipeline run log verified ({len(rows)} execution runs logged).")

    # 3. Assert Cryptographic SHA-256 Manifest Integrity
    with manifest_path.open('r', encoding='utf-8') as f:
        manifest = json.load(f)
    assert isinstance(manifest, list), "Manifest must be a JSON array of file entries"
    assert len(manifest) > 0, "Manifest must not be empty"

    for entry in manifest:
        source_file = entry.get('source_file')
        recorded_sha = entry.get('sha256')
        target_path = RAW_DIR / 'files' / source_file

        assert target_path.exists(), f"Manifest lists file {source_file} but it is missing from raw/files/"
        actual_sha = sha256_file(target_path)
        assert actual_sha == recorded_sha, (
            f"Cryptographic hash mismatch for {source_file}!\n"
            f"Manifest: {recorded_sha}\nActual  : {actual_sha}"
        )
    print(f" [PASS] 3. Manifest cryptographic integrity verified ({len(manifest)} files validated).")

    # 4. Load API Events and Assert Unique event_ids (Deduplication Proof)
    api_events = []
    with api_events_path.open('r', encoding='utf-8') as f:
        for idx, line in enumerate(f, 1):
            line = line.strip()
            if line:
                try:
                    api_events.append(json.loads(line))
                except json.JSONDecodeError as e:
                    raise AssertionError(f"Malformed JSONL at line {idx} in {api_events_path}: {e}")

    assert len(api_events) > 0, "raw/api/events.jsonl is unexpectedly empty!"
    event_ids = [e['event_id'] for e in api_events]
    assert len(event_ids) == len(set(event_ids)), (
        f"Assertion Error: Duplicate event_ids detected in raw/api/events.jsonl! "
        f"Total: {len(event_ids)}, Unique: {len(set(event_ids))}"
    )
    print(f" [PASS] 4. API event_id uniqueness verified ({len(api_events)} distinct events, 0 duplicates).")

    # 5. Assert Required Ingestion Metadata Fields Present
    for e in api_events:
        eid = e.get('event_id')
        assert '_ingested_at' in e, f"Missing '_ingested_at' in event {eid}"
        assert '_source' in e, f"Missing '_source' in event {eid}"
        assert e['_source'] == 'local_api', f"Unexpected source value in event {eid}: {e['_source']}"
    print(" [PASS] 5. Ingestion lineage metadata (_ingested_at, _source) verified across all events.")

    # 6. Assert Timestamps are Well-Formed ISO-8601
    for e in api_events:
        eid = e.get('event_id')
        for ts_field in ['updated_at', '_ingested_at']:
            try:
                datetime.fromisoformat(e[ts_field])
            except (ValueError, TypeError) as err:
                raise AssertionError(f"Invalid timestamp format for '{ts_field}' in event {eid}: {err}")
    print(" [PASS] 6. Timestamp validity verified (updated_at and _ingested_at parseable).")

    # 7. Assert Watermark Matches Max updated_at
    watermark_data = json.loads(watermark_path.read_text(encoding='utf-8'))
    saved_watermark = watermark_data.get('updated_at')
    max_updated_at = max(e['updated_at'] for e in api_events)

    assert saved_watermark == max_updated_at, (
        f"Assertion Error: Watermark synchronization failure!\n"
        f"Saved in state/api_watermark.json : {saved_watermark}\n"
        f"Max updated_at in raw/events.jsonl: {max_updated_at}"
    )
    print(f" [PASS] 7. Watermark synchronized perfectly with max(updated_at) ({saved_watermark}).")

    # 8. Assert Event Count Monotonicity (History Loss Protection)
    high_water_path = STATE_DIR / 'event_count_high_water.json'
    current_count = len(api_events)
    previous_count = 0

    if high_water_path.exists():
        try:
            previous_count = json.loads(high_water_path.read_text(encoding='utf-8')).get('max_event_count', 0)
        except Exception:
            previous_count = 0

    assert current_count >= previous_count, (
        f"Assertion Error: Event count regressed! Prior run had {previous_count} events, "
        f"now only {current_count}. Ingestion may have overwritten history."
    )
    if current_count > previous_count:
        high_water_path.write_text(json.dumps({'max_event_count': current_count}, indent=2), encoding='utf-8')
    print(f" [PASS] 8. Monotonicity confirmed (historical count preserved: {previous_count} -> {current_count}).")

    print("\n" + "=" * 70)
    print("ALL RAW DATA VALIDATION CHECKS PASSED (8/8)!")
    print("=" * 70 + "\n")
    return True


if __name__ == '__main__':
    try:
        success = validate_raw()
        sys.exit(0 if success else 1)
    except AssertionError as err:
        print(f"\n[FAIL] VALIDATION FAILED: {err}\n")
        sys.exit(1)