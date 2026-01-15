import os
import traceback
import gather
import process
import refresh


def lambda_handler(event, context):
    """
    Main Lambda handler that orchestrates gather, process, and refresh operations.
    Each handler is executed independently to isolate failures.
    """
    results = {
        'gather_result': None,
        'process_result': None,
        'refresh_result': None,
        'errors': []
    }
    
    # Gather phase
    try:
        print("Starting gather phase...")
        results['gather_result'] = gather.handle_gather(event, context)
        print(f"Gather completed: {results['gather_result']}")
    except Exception as e:
        error_msg = f"Gather failed: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        results['errors'].append({'phase': 'gather', 'error': str(e)})
    
    # Process phase
    try:
        print("Starting process phase...")
        results['process_result'] = process.handle_process(event, context)
        print(f"Process completed: {results['process_result']}")
    except Exception as e:
        error_msg = f"Process failed: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        results['errors'].append({'phase': 'process', 'error': str(e)})
    
    # Refresh phase
    try:
        print("Starting refresh phase...")
        results['refresh_result'] = refresh.handle_refresh(event, context)
        print(f"Refresh completed: {results['refresh_result']}")
    except Exception as e:
        error_msg = f"Refresh failed: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        results['errors'].append({'phase': 'refresh', 'error': str(e)})
    
    # Summary
    if results['errors']:
        print(f"Lambda completed with {len(results['errors'])} error(s)")
    else:
        print("Lambda completed successfully")
    
    return results
