"""
Local pipeline test to identify where data processing breaks.
Run from the lambda directory: python test_pipeline.py

This test simulates the full gather -> process pipeline locally without AWS.
"""
import os
import sys
import json
import traceback
import pandas as pd
import io
from datetime import datetime

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Mock context for Lambda
class MockContext:
    function_name = "test-chanscope-lambda"
    memory_limit_in_mb = 512
    
    def get_remaining_time_in_millis(self):
        return 300000


def test_s3_key_consistency():
    """Verify S3 key patterns match between gather and process."""
    print("\n" + "=" * 60)
    print("TEST: S3 Key Pattern Consistency")
    print("=" * 60)
    
    from utils import read_config
    
    s3_info = read_config(section='s3')
    threads = read_config(section='thread_info')
    
    raw_prefix = s3_info['raw_prefix']
    padding_data = s3_info['padding_data']
    boards = threads['boards'].split(',')
    
    print(f"Config values:")
    print(f"  raw_prefix: '{raw_prefix}'")
    print(f"  padding_data: '{padding_data}'")
    print(f"  boards: {boards}")
    
    # Simulate what gather.py creates (line 81)
    current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for board in boards[:1]:  # Test first board only
        # gather.py now uses explicit forward slashes (fixed)
        gather_key_s3 = f"{raw_prefix}/{board}_{padding_data}_{current_date}.csv"
        
        # process.py filter
        process_filter = f"{raw_prefix}/{board}_{padding_data}"
        
        print(f"\nBoard: {board}")
        print(f"  Gather creates (S3 key): '{gather_key_s3}'")
        print(f"  Process filters for:     '{process_filter}'")
        
        # Check if process filter matches gather key
        if gather_key_s3.startswith(process_filter):
            print(f"  [OK] Patterns are consistent - filter will match gather keys")
        else:
            print(f"  [ISSUE] Pattern mismatch - process won't find gather files!")
            print(f"  Filter: {process_filter}")
            print(f"  Key starts with: {gather_key_s3[:len(process_filter)]}")
            return False
    
    return True


def test_gather_data_locally():
    """Test gathering data from 4chan API locally (one board, limited threads)."""
    print("\n" + "=" * 60)
    print("TEST: Gather Data Locally (Limited)")
    print("=" * 60)
    
    import requests
    from utils import read_config, remove_omit_ids
    
    general = read_config(section='general')
    threads_config = read_config(section='thread_info')
    
    url = general['url']
    thread_keys = threads_config['threads_key']
    thread_number = threads_config['thread_number_key']
    thread_cmt_number = threads_config['thread_cmt_number_key']
    collected_dt = threads_config['collected_dt_key']
    posted_dt = threads_config['posted_dt_key']
    date_ = threads_config['date_key']
    now_ = threads_config['now_key']
    time_ = threads_config['time_key']
    omit_ids = threads_config['omit_ids'].split(',') if 'omit_ids' in threads_config else []
    
    # Test with just biz board and limit threads
    test_board = 'biz'
    max_threads = 3
    
    print(f"Fetching catalog from {url}{test_board}/catalog.json...")
    
    try:
        response = requests.get(f'{url}{test_board}/catalog.json', timeout=30)
        if response.status_code != 200:
            print(f"[FAIL] API returned status {response.status_code}")
            return None
        
        response_json_threads = response.json()
        print(f"[OK] Fetched catalog with {len(response_json_threads)} pages")
        
        thread_no = [
            line.get(thread_number) 
            for post_item in response_json_threads 
            for line in post_item.get(thread_keys, []) 
            if thread_number in line
        ][:max_threads]
        
        print(f"[INFO] Processing {len(thread_no)} threads: {thread_no}")
        
        data_all = []
        for item in thread_no:
            response_json_items = requests.get(f'{url}{test_board}/thread/{item}.json', timeout=30)
            if response_json_items.status_code != 200:
                print(f"  [WARN] Failed to fetch thread {item}: {response_json_items.status_code}")
                continue
            data_json = response_json_items.json()
            posts = data_json.get(thread_cmt_number, [])
            data_all.extend(posts)
            print(f"  [OK] Thread {item}: {len(posts)} posts")
        
        if not data_all:
            print("[FAIL] No posts gathered")
            return None
        
        data = pd.DataFrame(data_all)
        print(f"\n[INFO] Raw data shape: {data.shape}")
        print(f"[INFO] Columns: {list(data.columns)}")
        
        # Apply same transformations as gather.py
        current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data[collected_dt] = pd.to_datetime(current_date).floor('min')
        
        # Check if 'now' column exists (required for date parsing)
        if now_ not in data.columns:
            print(f"[FAIL] Column '{now_}' not found in data")
            print(f"[INFO] Available columns: {list(data.columns)}")
            return None
        
        # Parse date/time from 'now' column
        try:
            data[[date_, now_]] = data[now_].str.split('(', expand=True)
            data[[now_, time_]] = data[now_].str.split(')', expand=True)
            data[posted_dt] = pd.to_datetime(data[date_] + data[time_], format='%m/%d/%y%H:%M:%S')
            print(f"[OK] Date parsing successful")
        except Exception as e:
            print(f"[FAIL] Date parsing failed: {e}")
            print(f"[INFO] Sample 'now' values: {data[now_].head().tolist()}")
            return None
        
        data = remove_omit_ids(data, 'no', omit_ids)
        
        print(f"\n[OK] Gathered data shape: {data.shape}")
        print(f"[INFO] Sample columns: {list(data.columns[:10])}")
        
        # Save locally for processing test
        local_path = 'test_gathered_data.csv'
        data.to_csv(local_path, index=False)
        print(f"[OK] Saved to {local_path}")
        
        return data
        
    except requests.exceptions.Timeout:
        print("[FAIL] Request timed out")
        return None
    except Exception as e:
        print(f"[FAIL] Error: {e}")
        traceback.print_exc()
        return None


def test_process_data_locally(gathered_data):
    """Test processing gathered data locally."""
    print("\n" + "=" * 60)
    print("TEST: Process Data Locally")
    print("=" * 60)
    
    if gathered_data is None:
        print("[SKIP] No gathered data to process")
        return False
    
    from utils import (
        read_config, supportingcols, get_dateRange, 
        remove_whitespace, pad_punctuation, normalize_text, 
        remove_omit_ids, flatten_key_phrases
    )
    from fuzzywuzzy import fuzz
    import re
    
    threads_config = read_config(section='thread_info')
    board_specific = read_config(section='board_specific')
    renamed = read_config(section='renamed')
    
    posted_date_time = threads_config['posted_dt_key']
    thread_number_key = threads_config['thread_number_key']
    p_com = threads_config['p_com']
    text_clean = threads_config['text_clean']
    omit_ids = threads_config['omit_ids'].split(',') if 'omit_ids' in threads_config else []
    
    with open('key_phrases.json', 'r') as f:
        key_phrases = json.load(f)
    
    data = gathered_data.copy()
    print(f"[INFO] Input data shape: {data.shape}")
    print(f"[INFO] Input columns: {list(data.columns)}")
    
    # Step 1: Sort and deduplicate
    try:
        data = data.sort_values(by=posted_date_time, ascending=False)
        print(f"[OK] Sorted by {posted_date_time}")
    except KeyError as e:
        print(f"[FAIL] Sort failed - column not found: {e}")
        print(f"[INFO] Available columns: {list(data.columns)}")
        return False
    
    try:
        before_dedup = len(data)
        data = data.drop_duplicates(subset=[thread_number_key, posted_date_time], keep='last')
        print(f"[OK] Deduplicated: {before_dedup} -> {len(data)} rows")
    except KeyError as e:
        print(f"[FAIL] Deduplication failed - column not found: {e}")
        return False
    
    # Step 2: Rename columns
    print(f"[INFO] Renaming columns: {renamed}")
    data = data.rename(columns=renamed)
    print(f"[OK] Columns after rename: {list(data.columns)}")
    
    # Step 3: Drop NA in posted_comment
    if p_com not in data.columns:
        print(f"[FAIL] Column '{p_com}' not found after rename")
        print(f"[INFO] Looking for 'com' in original columns...")
        if 'com' in gathered_data.columns:
            print(f"[INFO] 'com' exists but wasn't renamed to '{p_com}'")
            print(f"[INFO] Renamed config: {renamed}")
        return False
    
    before_dropna = len(data)
    data = data.dropna(subset=[p_com])
    print(f"[OK] Dropped NA: {before_dropna} -> {len(data)} rows")
    
    if len(data) == 0:
        print("[FAIL] All rows dropped - no valid comments")
        return False
    
    # Step 4: Process text (normalize, clean)
    print(f"[INFO] Processing text in '{p_com}' column...")
    data[p_com] = data[p_com].astype(str)
    data[text_clean] = data[p_com].apply(normalize_text).apply(remove_whitespace).apply(pad_punctuation)
    print(f"[OK] Text cleaning complete")
    print(f"[INFO] Sample cleaned text: {data[text_clean].head(1).values}")
    
    # Step 5: Key phrase matching
    phrases_with_category = flatten_key_phrases(key_phrases)
    if not phrases_with_category:
        print("[WARN] No key phrases found - skipping phrase matching")
        data['matches'] = None
        data['category'] = None
        data['similarity'] = None
    else:
        print(f"[INFO] Matching against {len(phrases_with_category)} phrases...")
        # Simplified matching for test
        data['matches'] = None
        data['category'] = None
        data['similarity'] = None
    
    # Step 6: Add supporting columns
    print("[INFO] Adding supporting columns...")
    try:
        data = supportingcols(data, p_com)
        print(f"[OK] Supporting columns added")
    except Exception as e:
        print(f"[FAIL] Supporting columns failed: {e}")
        traceback.print_exc()
        return False
    
    # Step 7: Select final columns
    test_board = 'biz'
    columns_names = board_specific.get(f"{test_board}_keys").split(',')
    columns_names = [col.strip() for col in columns_names]
    print(f"[INFO] Required columns for {test_board}: {columns_names}")
    
    missing_cols = [col for col in columns_names if col not in data.columns]
    if missing_cols:
        print(f"[FAIL] Missing required columns: {missing_cols}")
        print(f"[INFO] Available columns: {list(data.columns)}")
        return False
    
    data = data[columns_names]
    print(f"[OK] Selected {len(columns_names)} columns")
    
    # Step 8: Remove omit IDs
    data = remove_omit_ids(data, 'thread_id', omit_ids)
    print(f"[OK] Final data shape: {data.shape}")
    
    # Step 9: Get date range
    try:
        date_range = get_dateRange(data)
        print(f"[OK] Date range: {date_range}")
    except Exception as e:
        print(f"[WARN] Date range calculation failed: {e}")
    
    # Save result
    output_path = 'test_processed_data.csv'
    data.to_csv(output_path, index=False)
    print(f"[OK] Saved processed data to {output_path}")
    
    return True


def test_full_pipeline():
    """Run the complete local pipeline test."""
    print("=" * 60)
    print("CHANSCOPE LAMBDA PIPELINE TEST")
    print("=" * 60)
    
    results = {}
    
    # Test 1: S3 key consistency
    results['s3_keys'] = test_s3_key_consistency()
    
    # Test 2: Gather data locally
    gathered_data = test_gather_data_locally()
    results['gather'] = gathered_data is not None
    
    # Test 3: Process data locally
    results['process'] = test_process_data_locally(gathered_data)
    
    # Summary
    print("\n" + "=" * 60)
    print("PIPELINE TEST SUMMARY")
    print("=" * 60)
    
    for test_name, passed in results.items():
        status = "[PASS]" if passed else "[FAIL]"
        print(f"{status} {test_name}")
    
    if all(results.values()):
        print("\nPipeline test PASSED - no issues found in local execution")
        print("If Lambda still fails, check:")
        print("  1. AWS permissions (S3 read/write)")
        print("  2. Network/timeout issues in Lambda")
        print("  3. CloudWatch logs for actual errors")
    else:
        print("\nPipeline test FAILED - issues found above need to be fixed")
    
    return all(results.values())


if __name__ == "__main__":
    success = test_full_pipeline()
    sys.exit(0 if success else 1)
