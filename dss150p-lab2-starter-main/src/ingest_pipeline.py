"""Week 3 starter: rerunnable ingestion to a raw area.
Students implement file ingestion + paginated REST API ingestion + watermark + duplicate prevention.
"""
from pathlib import Path
from datetime import datetime, timezone
import json, hashlib, shutil
import requests

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'
RAW = ROOT / 'raw'
STATE = ROOT / 'state'
API_URL = 'http://127.0.0.1:8000/api/events'

def utc_now(): 
    return datetime.now(timezone.utc).isoformat()

def sha256_file(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''): 
            h.update(chunk)
    return h.hexdigest()

def load_watermark():
    p = STATE / 'api_watermark.json'
    if not p.exists(): 
        return None
    return json.loads(p.read_text())['updated_at']

def save_watermark(value):
    STATE.mkdir(exist_ok=True)
    (STATE / 'api_watermark.json').write_text(json.dumps({'updated_at': value}, indent=2))

def ingest_files():
    """Copy CSV/JSON/Parquet to raw/files/ and record a manifest entry."""
    files_dir = RAW / 'files'
    files_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = RAW / 'manifest.json'
    
    manifest = []
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
        except json.JSONDecodeError:
            manifest = []
            
    source_files = [f for f in DATA.glob('*') if f.is_file()]
    
    for sf in source_files:
        file_hash = sha256_file(sf)
        file_bytes = sf.stat().st_size
        dest = files_dir / sf.name
        
        # Copy file to raw area
        shutil.copy2(sf, dest)
        
        entry = {
            "source_file": sf.name,
            "ingested_at": utc_now(),
            "sha256": file_hash,
            "bytes": file_bytes
        }
        
        # Update manifest cleanly (replace old entry for the same file if it exists)
        manifest = [m for m in manifest if m["source_file"] != sf.name]
        manifest.append(entry)
        
    manifest_path.write_text(json.dumps(manifest, indent=2))

def fetch_api_page(page, per_page=20, updated_after=None):
    params = {'page': page, 'per_page': per_page}
    if updated_after: 
        params['updated_after'] = updated_after
    r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def ingest_api():
    """
    1) read watermark
    2) follow pagination until has_more=False
    3) append ingestion metadata (_ingested_at, _source)
    4) deduplicate by event_id keeping greatest updated_at
    5) write raw/api/events.jsonl atomically
    6) update watermark only after successful write
    """
    watermark = load_watermark()
    all_items = []
    page = 1
    has_more = True
    
    # 2) Follow pagination until has_more=False
    while has_more:
        data = fetch_api_page(page, per_page=50, updated_after=watermark)
        items = data.get('items', [])
        ingested_time = utc_now()
        
        # 3) Append ingestion metadata
        for item in items:
            item['_ingested_at'] = ingested_time
            item['_source'] = 'local_api'
            all_items.append(item)
            
        has_more = data.get('has_more', False)
        page = data.get('next_page')
        if not page and has_more:
            page = data['page'] + 1

    if not all_items:
        return
        
    # 4) Deduplicate by event_id keeping greatest updated_at
    dedup_dict = {}
    for item in all_items:
        eid = item['event_id']
        if eid not in dedup_dict:
            dedup_dict[eid] = item
        else:
            if item['updated_at'] > dedup_dict[eid]['updated_at']:
                dedup_dict[eid] = item
                
    unique_items = list(dedup_dict.values())
    
    # 5) Write raw/api/events.jsonl atomically (write to .tmp then replace)
    api_raw_dir = RAW / 'api'
    api_raw_dir.mkdir(parents=True, exist_ok=True)
    target_file = api_raw_dir / 'events.jsonl'
    temp_file = api_raw_dir / 'events.jsonl.tmp'
    
    with temp_file.open('w', encoding='utf-8') as f:
        for item in unique_items:
            f.write(json.dumps(item) + '\n')
            
    temp_file.replace(target_file)
    
    # 6) Update watermark only after successful write using max updated_at
    max_updated_at = max(item['updated_at'] for item in unique_items)
    if watermark is None or max_updated_at > watermark:
        save_watermark(max_updated_at)

if __name__ == '__main__':
    RAW.mkdir(exist_ok=True)
    STATE.mkdir(exist_ok=True)
    ingest_files()
    ingest_api()
    print("Ingestion pipeline completed successfully!")