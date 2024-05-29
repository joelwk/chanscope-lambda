import os
import nltk
import gather
import process
import refresh

def lambda_handler(event, context):
    gather_result = gather.handle_gather(event, context)
    process_result = process.handle_process(event, context)
    refresh_result = refresh.handle_refresh(event, context)
    
    return {
        'gather_result': gather_result,
        'process_result': process_result,
        'refresh_result': refresh_result,
    }
