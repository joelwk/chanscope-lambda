import requests
import boto3
import os
import json
import csv
import configparser
import logging
import datetime
import warnings
import pandas as pd

from botocore.exceptions import ClientError
from utils import read_config, supportingcols, get_dateRange, remove_omit_ids

s3 = boto3.client('s3')

config_path = 'config.ini'

general = read_config(section='general', config_path=config_path)
threads = read_config(section='thread_info', config_path=config_path)
s3_info = read_config(section='s3', config_path=config_path)

url = general['url']
thread_keys = threads['threads_key']
thread_number = threads['thread_number_key']
thread_cmt_number = threads['thread_cmt_number_key']
collected_dt = threads['collected_dt_key']
posted_dt = threads['posted_dt_key']
date_ = threads['date_key']
now_ = threads['now_key']
time_ = threads['time_key']

omit_ids = threads['omit_ids'].split(',') if 'omit_ids' in threads else []
boards = threads['boards'].split(',') if 'boards' in threads else []

bucket_name = s3_info['bucket']
raw_prefix = s3_info['raw_prefix']
path_padding = s3_info['padding_data']

def safe_to_datetime(column, format='%Y-%m-%d %H:%M:%S', utc=False):
    if utc:
        return pd.to_datetime(column, format=format, errors='coerce', utc=True).dt.floor('min')
    else:
        return pd.to_datetime(column, format=format, errors='coerce').dt.floor('min')

def handle_gather(event, context):
    s3 = boto3.client('s3')
    for _board_ in boards:
        response = requests.get(f'{url}/{_board_}/catalog.json')
        if response.status_code != 200:
            print(f"Failed to fetch catalog for board {_board_}: {response.status_code}")
            continue
        try:
            response_json_threads = response.json()
        except ValueError as e:
            print(f"JSON decoding failed for board {_board_}: {str(e)}")
            continue
        thread_no = [line.get(thread_number) for post_item in response_json_threads for line in post_item.get(thread_keys, []) if thread_number in line]
        data_all = []
        for item in thread_no:
            response_json_items = requests.get(f'{url}/{_board_}/thread/{item}.json')
            if response_json_items.status_code != 200:
                print(f"Failed to fetch thread {item} for board {_board_}: {response_json_items.status_code}")
                continue
            try:
                data_json = response_json_items.json()
                data_all.extend(data_json.get(thread_cmt_number, []))
            except ValueError as e:
                print(f"JSON decoding failed for thread {item}: {str(e)}")
                continue
        data = pd.DataFrame(data_all)
        current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data[collected_dt] = pd.to_datetime(current_date).floor('min')
        data[[date_, now_]] = data[now_].str.split('(', expand=True)
        data[[now_, time_]] = data[now_].str.split(')', expand=True)
        data[posted_dt] = pd.to_datetime(data[date_] + data[time_], format='%m/%d/%y%H:%M:%S')
        data[time_] = safe_to_datetime(data[time_], utc=True).apply(lambda x: x.strftime("%H:%M:%S") if pd.notnull(x) else None)
        data = remove_omit_ids(data, 'no', omit_ids)
        filename = f'{_board_}_{path_padding}_{current_date}.csv'
        local_path = os.path.join('/tmp', filename)
        s3_key = os.path.join(raw_prefix, f"{_board_}_{path_padding}_{current_date}.csv")
        try:
            data.to_csv(local_path, index=False)
            with open(local_path, "rb") as f:
                s3.upload_fileobj(f, bucket_name, s3_key)
            print(f"File saved and uploaded for board {_board_}: local_path {local_path} : s3_key {s3_key}")
        except ClientError as e:
            print(f"Failed to upload file to S3 for board {_board_}: {str(e)}")
    return "Gather completed"