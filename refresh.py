import boto3
import configparser
from datetime import datetime, timedelta, timezone
from utils import read_config, supportingcols, get_dateRange

config_path = 'config.ini'

s3_info = read_config(section='s3', config_path=config_path)
general = read_config(section='general', config_path=config_path)
s3_destinations = read_config(section='s3_destinations', config_path=config_path)

def handle_refresh(event, context):
    
    # Configuration and S3 client initialization
    source_bucket = s3_info['bucket']
    source_prefix = s3_info['data_prefix']
    destination_bucket = s3_destinations['dest_bucket']
    lookback_days = int(s3_destinations['lookback_days'])
    time_threshold = datetime.utcnow() - timedelta(days=lookback_days)
    time_threshold = time_threshold.replace(tzinfo=timezone.utc)
    
    # Retrieve continuation tokens from the event
    source_continuation_token = event.get('sourceContinuationToken', None)

    # Initialize S3 paginator without PaginationConfig
    s3 = boto3.client('s3')
    source_paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {
        'Bucket': source_bucket,
        'Prefix': source_prefix,
    }

    # Conditionally add PaginationConfig if there's a continuation token
    if source_continuation_token:
        operation_parameters['PaginationConfig'] = {'StartingToken': source_continuation_token}

    # Processing new files
    print('Processing new files:')
    for page in source_paginator.paginate(**operation_parameters):
        objects = sorted(page.get('Contents', []), key=lambda x: x['LastModified'], reverse=True)
        for obj in objects:
            last_modified = obj['LastModified']
            key = obj['Key']
            if last_modified >= time_threshold:
                print(f'Copying {key}, last modified: {last_modified}')
                s3.copy_object(Bucket=destination_bucket, CopySource={'Bucket': source_bucket, 'Key': key}, Key=key)

            # Check remaining time and potentially exit early with the next token
            if context.get_remaining_time_in_millis() < 10000:
                next_token = page.get('NextContinuationToken', None)
                return {
                    'sourceContinuationToken': next_token,
                    'status': 'incomplete'
                }

    dest_continuation_token = event.get('destContinuationToken', None)

    # Initialize S3 paginator for destination bucket
    dest_paginator = s3.get_paginator('list_objects_v2')
    dest_operation_parameters = {
        'Bucket': destination_bucket,
        'Prefix': source_prefix,
    }

    # Conditionally add PaginationConfig for the destination bucket if there's a continuation token
    if dest_continuation_token:
        dest_operation_parameters['PaginationConfig'] = {'StartingToken': dest_continuation_token}

    print('Deleting old files from destination bucket:')
    for page in dest_paginator.paginate(**dest_operation_parameters):
        for obj in page.get('Contents', []):
            last_modified = obj['LastModified']
            key = obj['Key']
            if last_modified < time_threshold:
                print(f'Deleting {key} from destination, last modified: {last_modified}')
                s3.delete_object(Bucket=destination_bucket, Key=key)
            
            # Check remaining time and potentially exit early with the next token for deletion
            if context.get_remaining_time_in_millis() < 10000:
                next_token = page.get('NextContinuationToken', None)
                return {
                    'destContinuationToken': next_token,
                    'status': 'incomplete',
                    'sourceContinuationToken': source_continuation_token
                }
    
    return "Refresh completed"