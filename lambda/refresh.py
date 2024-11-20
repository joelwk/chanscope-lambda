import boto3
import configparser
from datetime import datetime, timedelta, timezone
from utils import read_config, supportingcols, get_dateRange

config_path = 'config.ini'

s3_info = read_config(section='s3', config_path=config_path)
general = read_config(section='general', config_path=config_path)
s3_destinations = read_config(section='s3_refresh_destinations', config_path=config_path)

def handle_refresh(event, context):
    s3 = boto3.client('s3')

    def refresh_bucket(source_bucket, source_prefix, destination_bucket, lookback_days, source_token, dest_token):
        time_threshold = datetime.utcnow() - timedelta(days=lookback_days)
        time_threshold = time_threshold.replace(tzinfo=timezone.utc)

        # Initialize S3 paginator for source bucket
        source_paginator = s3.get_paginator('list_objects_v2')
        operation_parameters = {
            'Bucket': source_bucket,
            'Prefix': source_prefix,
        }

        if source_token:
            operation_parameters['ContinuationToken'] = source_token

        # Processing new files
        for page in source_paginator.paginate(**operation_parameters):
            objects = sorted(page.get('Contents', []), key=lambda x: x['LastModified'], reverse=True)
            for obj in objects:
                last_modified = obj['LastModified']
                key = obj['Key']
                if last_modified >= time_threshold:
                    print(f'Copying {key}, last modified: {last_modified}')
                    s3.copy_object(Bucket=destination_bucket, CopySource={'Bucket': source_bucket, 'Key': key}, Key=key)
                
                if context.get_remaining_time_in_millis() < 10000:
                    next_token = page.get('NextContinuationToken', None)
                    return {
                        'ContinuationToken': next_token,
                        'status': 'incomplete'
                    }

        # Initialize S3 paginator for destination bucket
        dest_paginator = s3.get_paginator('list_objects_v2')
        dest_operation_parameters = {
            'Bucket': destination_bucket,
            'Prefix': source_prefix,
        }

        if dest_token:
            dest_operation_parameters['ContinuationToken'] = dest_token

        # Deleting old files from destination bucket
        for page in dest_paginator.paginate(**dest_operation_parameters):
            for obj in page.get('Contents', []):
                last_modified = obj['LastModified']
                key = obj['Key']
                if last_modified < time_threshold:
                    print(f'Deleting {key} from destination, last modified: {last_modified}')
                    s3.delete_object(Bucket=destination_bucket, Key=key)

                if context.get_remaining_time_in_millis() < 10000:
                    next_token = page.get('NextContinuationToken', None)
                    return {
                        'ContinuationToken': next_token,
                        'status': 'incomplete'
                    }

        return "Refresh completed"

    # Primary refresh
    primary_result = refresh_bucket(
        s3_info['bucket'], s3_info['data_prefix'], 
        s3_destinations['roling_bucket'], int(s3_destinations['lookback_days']),
        event.get('sourceContinuationToken'), event.get('destContinuationToken')
    )

    if primary_result != "Refresh completed":
        return primary_result

    # Alternate refresh
    alt_result = refresh_bucket(
        s3_info['bucket'], s3_info['data_prefix'], 
        s3_destinations['daily_bucket_rolling'], int(s3_destinations['daily_lookback_days']),
        event.get('altSourceContinuationToken'), event.get('altDestContinuationToken')
    )

    return alt_result if alt_result != "Refresh completed" else "Both refresh processes completed"