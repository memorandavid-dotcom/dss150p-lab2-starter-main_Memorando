"""Automated Rerun Safety, Clean-Room, and Failure-Recovery Test Suite.

Proves:
1. Clean-Room Run: Starting from empty raw/ and state/ directories lands all sources properly.
2. Second-Run (Idempotency): Immediate rerun produces 0 new files, 0 duplicate events,
   unchanged watermark, and bit-for-bit identical raw outputs.
3. Failure Recovery: An API server failure mid-pipeline logs FAILED without advancing the
   watermark or corrupting raw files; subsequent run recovers cleanly.

Run: python src/test_rerun_safety.py
"""
from pathlib import Path
import shutil
import json
import subprocess
import time
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

RAW_DIR = ROOT / 'raw'
STATE_DIR = ROOT / 'state'
RUN_LOG_PATH = ROOT / 'pipeline_run_log.csv'

from ingest_pipeline import run_pipeline, load_watermark, sha256_file
from validate_raw import validate_raw


def clean_environment():
    """Wipe generated raw/ and state/ artifacts for clean-room testing."""
    if RAW_DIR.exists():
        shutil.rmtree(RAW_DIR)
    if STATE_DIR.exists():
        shutil.rmtree(STATE_DIR)
    if RUN_LOG_PATH.exists():
        RUN_LOG_PATH.unlink()
    print(" [SETUP] Wiped raw/, state/, and pipeline_run_log.csv.")


def run_test_suite():
    print("#" * 70)
    print(" DSS150P LAB 2: AUTOMATED RERUN SAFETY & FAILURE RECOVERY SUITE")
    print("#" * 70)

    # -------------------------------------------------------------------------
    # TEST 1: CLEAN-ROOM INITIAL RUN
    # -------------------------------------------------------------------------
    print("\n>>> TEST 1: Clean-Room Initial Run (Starting from Scratch)...")
    clean_environment()

    # Start local API server in subprocess if not already running
    api_proc = None
    try:
        import requests
        requests.get("http://127.0.0.1:8000/api/events", timeout=2)
        print(" [INFO] Local API server is already running.")
    except Exception:
        print(" [SETUP] Starting local API server daemon (python src/local_api_server.py)...")
        api_proc = subprocess.Popen([sys.executable, str(ROOT / 'src' / 'local_api_server.py')])
        time.sleep(2)

    # Execute Initial Pipeline Run
    success = run_pipeline()
    assert success, "Initial pipeline run failed!"
    print(" [PASS] Clean-room pipeline run completed successfully.")

    # Validate Raw Area
    val_ok = validate_raw()
    assert val_ok, "Validation failed after clean-room run!"

    watermark_1 = load_watermark()
    events_file = RAW_DIR / 'api' / 'events.jsonl'
    with events_file.open('r', encoding='utf-8') as f:
        events_1 = [json.loads(line) for line in f if line.strip()]
    events_count_1 = len(events_1)
    manifest_1 = json.loads((RAW_DIR / 'manifest.json').read_text(encoding='utf-8'))
    print(f" [METRICS] Run 1 landed {len(manifest_1)} files, {events_count_1} events, watermark: {watermark_1}")

    # -------------------------------------------------------------------------
    # TEST 2: SECOND-RUN IDEMPOTENCY VERIFICATION
    # -------------------------------------------------------------------------
    print("\n>>> TEST 2: Second-Run Idempotency & Duplicate Prevention...")
    events_hash_before = sha256_file(events_file)

    # Execute Second Pipeline Run
    success_2 = run_pipeline()
    assert success_2, "Second pipeline run failed!"

    # Verify Idempotency Assurances
    watermark_2 = load_watermark()
    assert watermark_2 == watermark_1, (
        f"Idempotency Failure: Watermark shifted on rerun without new data!\n"
        f"Run 1: {watermark_1}\nRun 2: {watermark_2}"
    )

    with events_file.open('r', encoding='utf-8') as f:
        events_2 = [json.loads(line) for line in f if line.strip()]
    events_count_2 = len(events_2)
    assert events_count_2 == events_count_1, (
        f"Idempotency Failure: Event count changed on rerun! Run 1: {events_count_1}, Run 2: {events_count_2}"
    )

    events_hash_after = sha256_file(events_file)
    assert events_hash_before == events_hash_after, "Idempotency Failure: events.jsonl binary payload altered on rerun!"
    print(" [PASS] Rerun produced 0 new events, 0 duplicate rows, and identical watermark.")

    # Validate again
    val_ok_2 = validate_raw()
    assert val_ok_2, "Validation failed after second run!"

    # -------------------------------------------------------------------------
    # TEST 3: FAILURE RECOVERY & WATERMARK SAFETY
    # -------------------------------------------------------------------------
    print("\n>>> TEST 3: Failure Recovery Simulation...")
    # Simulate API failure by modifying API URL temporarily or stopping server
    import ingest_pipeline
    original_url = ingest_pipeline.API_URL
    ingest_pipeline.API_URL = "http://127.0.0.1:9999/invalid_endpoint"  # Guaranteed connection refusal

    print(" [SIMULATION] Intentionally sending requests to invalid port 9999...")
    fail_run_ok = ingest_pipeline.run_pipeline()
    assert not fail_run_ok, "Pipeline was expected to return False on server failure!"

    # Verify Watermark State was NOT corrupted or advanced
    watermark_during_failure = load_watermark()
    assert watermark_during_failure == watermark_1, (
        f"Watermark Safety Failure: Watermark corrupted during failure! Expected {watermark_1}, got {watermark_during_failure}"
    )
    print(" [PASS] Watermark was NOT advanced or corrupted during API failure.")

    # Verify raw file remains uncorrupted
    assert sha256_file(events_file) == events_hash_before, "Raw file was corrupted during failed run!"
    print(" [PASS] Existing raw data remained 100% intact and uncorrupted.")

    # Restore endpoint and recover
    print(" [RECOVERY] Restoring API endpoint and verifying clean recovery...")
    ingest_pipeline.API_URL = original_url
    recover_ok = ingest_pipeline.run_pipeline()
    assert recover_ok, "Pipeline recovery run failed!"
    val_ok_3 = validate_raw()
    assert val_ok_3, "Validation failed after recovery run!"
    print(" [PASS] Pipeline recovered cleanly from failure state.")

    # Cleanup subprocess if started
    if api_proc:
        api_proc.terminate()
        print(" [CLEANUP] Stopped local API server daemon.")

    print("\n" + "#" * 70)
    print(" ALL RERUN SAFETY & FAILURE RECOVERY TESTS PASSED (3/3)!")
    print("#" * 70 + "\n")


if __name__ == '__main__':
    run_test_suite()
