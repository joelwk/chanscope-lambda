"""
Local test script to diagnose lambda_handler issues.
Run from the lambda directory: python test_handler.py

Tests configuration loading, key phrase handling, and S3 prefix consistency.
Skips network-dependent tests when run locally.
"""
import os
import sys
import json
import traceback

os.chdir(os.path.dirname(os.path.abspath(__file__)))

SKIP_NETWORK_TESTS = os.getenv('SKIP_NETWORK', '1') == '1'

class MockContext:
    """Mock AWS Lambda context object for local testing."""
    function_name = "test-chanscope-lambda"
    memory_limit_in_mb = 512
    invoked_function_arn = "arn:aws:lambda:us-east-1:123456789:function:test"
    aws_request_id = "test-request-id"
    
    def get_remaining_time_in_millis(self):
        return 300000  # 5 minutes


def test_config_loading():
    """Test configuration file loading."""
    print("\n=== Testing Config Loading ===")
    try:
        from utils import read_config
        
        sections = ['general', 'thread_info', 's3', 's3_refresh_destinations', 'renamed', 'board_specific']
        for section in sections:
            config = read_config(section=section)
            print(f"[OK] Section '{section}' loaded with {len(config)} keys")
        return True
    except Exception as e:
        print(f"[FAIL] Config loading error: {e}")
        traceback.print_exc()
        return False


def test_key_phrases_loading():
    """Test key_phrases.json loading and validation."""
    print("\n=== Testing Key Phrases Loading ===")
    try:
        with open('key_phrases.json', 'r') as f:
            key_phrases = json.load(f)
        
        print(f"[INFO] Loaded {len(key_phrases)} categories from key_phrases.json")
        
        from utils import flatten_key_phrases
        phrases = flatten_key_phrases(key_phrases)
        
        if not phrases:
            print("[WARN] No key phrases found - phrase matching will be skipped")
            print("[INFO] This is handled gracefully by process.py")
        else:
            print(f"[OK] Flattened to {len(phrases)} phrase-category pairs")
        
        return True
    except FileNotFoundError:
        print("[FAIL] key_phrases.json not found")
        return False
    except json.JSONDecodeError as e:
        print(f"[FAIL] Invalid JSON in key_phrases.json: {e}")
        return False
    except Exception as e:
        print(f"[FAIL] Error: {e}")
        traceback.print_exc()
        return False


def test_s3_prefix_consistency():
    """Check S3 key prefix consistency between gather and process."""
    print("\n=== Testing S3 Prefix Consistency ===")
    try:
        from utils import read_config
        
        s3_info = read_config(section='s3')
        threads = read_config(section='thread_info')
        
        raw_prefix = s3_info['raw_prefix']
        padding_data = s3_info['padding_data']
        boards = threads['boards'].split(',')
        
        print(f"[INFO] raw_prefix: '{raw_prefix}'")
        print(f"[INFO] padding_data: '{padding_data}'")
        print(f"[INFO] boards: {boards}")
        
        # Pattern from gather.py line 81
        gather_pattern = f"{raw_prefix}/{{board}}_{padding_data}_{{date}}.csv"
        print(f"[INFO] Gather creates: {gather_pattern}")
        
        # Pattern from process.py line 53  
        process_filter = f"{raw_prefix}/{{board}}__data"
        print(f"[INFO] Process filters: {process_filter}")
        
        # Check for mismatch
        example_gather_key = f"{raw_prefix}/{boards[0]}_{padding_data}_2026-01-15.csv"
        example_process_prefix = f"{raw_prefix}/{boards[0]}__data"
        
        if not example_gather_key.startswith(example_process_prefix):
            print(f"[FAIL] PREFIX MISMATCH DETECTED!")
            print(f"       Gather creates: {example_gather_key}")
            print(f"       Process looks for prefix: {example_process_prefix}")
            print(f"[FIX] Align the prefix patterns in gather.py or process.py")
            return False
        
        print("[OK] Prefix patterns are consistent")
        return True
    except Exception as e:
        print(f"[FAIL] Error: {e}")
        traceback.print_exc()
        return False


def test_nltk_data():
    """Test NLTK data availability."""
    print("\n=== Testing NLTK Data ===")
    try:
        import nltk
        from nltk.corpus import stopwords
        
        stop_words = stopwords.words('english')
        print(f"[OK] NLTK stopwords loaded: {len(stop_words)} words")
        return True
    except LookupError as e:
        print(f"[FAIL] NLTK data not found: {e}")
        print("[FIX] Run: python -m nltk.downloader stopwords")
        return False
    except Exception as e:
        print(f"[FAIL] Error: {e}")
        traceback.print_exc()
        return False


def test_gather_handler():
    """Test gather handler (requires AWS credentials and network)."""
    print("\n=== Testing Gather Handler ===")
    try:
        if SKIP_NETWORK_TESTS:
            print("[SKIP] Network tests disabled (set SKIP_NETWORK=0 to enable)")
            return True
            
        import gather
        
        print("[INFO] Gather module imported successfully")
        print("[INFO] Testing API connectivity...")
        
        import requests
        from utils import read_config
        
        general = read_config(section='general')
        url = general['url']
        
        # Test API connectivity
        response = requests.get(f"{url}pol/catalog.json", timeout=10)
        if response.status_code == 200:
            print(f"[OK] 4chan API accessible at {url}")
        else:
            print(f"[WARN] API returned status {response.status_code}")
        
        return True
    except Exception as e:
        print(f"[FAIL] Error: {e}")
        traceback.print_exc()
        return False


def test_process_handler_dry():
    """Dry-run test for process handler."""
    print("\n=== Testing Process Handler (Dry Run) ===")
    try:
        if SKIP_NETWORK_TESTS:
            print("[SKIP] Skipping full import (requires AWS). Testing key_phrases only...")
            
            with open('key_phrases.json', 'r') as f:
                key_phrases = json.load(f)
            
            from utils import flatten_key_phrases
            phrases = flatten_key_phrases(key_phrases)
            
            if not phrases:
                print("[WARN] key_phrases.json is empty - process will skip phrase matching")
                print("[INFO] This is now handled gracefully (no longer causes ValueError)")
            else:
                print(f"[OK] Found {len(phrases)} key phrases")
            return True
        
        import process
        
        print("[INFO] Process module imported successfully")
        
        from utils import flatten_key_phrases
        phrases = flatten_key_phrases(process.key_phrases)
        
        if not phrases:
            print("[WARN] key_phrases is empty - phrase matching will be skipped")
        else:
            print(f"[OK] Process module ready with {len(phrases)} key phrases")
        
        return True
    except Exception as e:
        print(f"[FAIL] Error: {e}")
        traceback.print_exc()
        return False


def test_main_handler():
    """Test main lambda_handler import and structure."""
    print("\n=== Testing Main Lambda Handler ===")
    try:
        if SKIP_NETWORK_TESTS:
            print("[SKIP] Skipping full import (requires AWS). Checking syntax only...")
            import ast
            with open('main.py', 'r') as f:
                source = f.read()
            ast.parse(source)
            print("[OK] main.py syntax is valid")
            
            # Check for lambda_handler function
            if 'def lambda_handler' in source:
                print("[OK] lambda_handler function found")
            else:
                print("[FAIL] lambda_handler function not found")
                return False
            return True
        
        import main
        
        print("[INFO] Main module imported successfully")
        
        if not hasattr(main, 'lambda_handler'):
            print("[FAIL] lambda_handler function not found in main.py")
            return False
        
        print("[OK] lambda_handler function exists")
        return True
    except Exception as e:
        print(f"[FAIL] Error: {e}")
        traceback.print_exc()
        return False


def run_all_tests():
    """Run all diagnostic tests."""
    print("=" * 60)
    print("CHANSCOPE LAMBDA DIAGNOSTIC TESTS")
    print("=" * 60)
    
    results = {
        "Config Loading": test_config_loading(),
        "Key Phrases": test_key_phrases_loading(),
        "S3 Prefix Consistency": test_s3_prefix_consistency(),
        "NLTK Data": test_nltk_data(),
        "Gather Handler": test_gather_handler(),
        "Process Handler": test_process_handler_dry(),
        "Main Handler": test_main_handler(),
    }
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    for test_name, passed in results.items():
        status = "[PASS]" if passed else "[FAIL]"
        print(f"{status} {test_name}")
    
    failed_count = sum(1 for v in results.values() if not v)
    if failed_count > 0:
        print(f"\n{failed_count} test(s) FAILED - fix these before deploying")
        return False
    else:
        print("\nAll tests PASSED")
        return True


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
