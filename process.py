import boto3
import pandas as pd
import numpy as np
import io

import os
import glob
from pathlib import Path
import configparser
import warnings
from datetime import datetime, timedelta

from utils import read_config, supportingcols, get_dateRange, remove_whitespace, pad_punctuation, normalize_text, remove_omit_ids

config_path = 'config.ini'

general = read_config(section='general', config_path=config_path)
s3 = read_config(section='s3', config_path=config_path)
threads = read_config(section='thread_info', config_path=config_path)
board_specific = read_config(section='board_specific', config_path=config_path)
renamed = read_config(section='renamed', config_path=config_path)

general = {key: general[key] for key in general}
threads = {key: threads[key] for key in threads}

s3_bucket = s3['bucket']
data_prefix = s3['data_prefix']
raw_prefix = s3['raw_prefix']
date_format_ = general['date_format']
time_format_ = general['time_format']
time_ = threads['time_key']
date_ = threads['date_key']
posted_date_time = threads['posted_dt_key']
boards = threads['boards'].split(',') if 'boards' in threads else []

text_clean = threads['text_clean']
thread_number_key = threads['thread_number_key']
p_com = threads['p_com']
matches = threads['matches']
omit_ids = threads['omit_ids'].split(',') if 'omit_ids' in threads else []

def process_data(data, input_col, clean_col):
    data[input_col] = data[input_col].astype(str)
    data[clean_col] = data[input_col].apply(normalize_text).apply(remove_whitespace).apply(pad_punctuation)
    return data[data[clean_col].notnull() & data[clean_col].str.strip().astype(bool)]

def handle_process(event, context):
    s3_resource = boto3.resource('s3')
    for _board_ in boards:
        print(f"Processing board: {_board_}")
        bucket_objects = s3_resource.Bucket(s3_bucket).objects.filter(Prefix=f"{raw_prefix}/{_board_}__data")
        object_lists = []
        for obj in bucket_objects:
            print(f"Processing file: {obj.key}")
            body = obj.get()['Body'].read()
            data = pd.read_csv(io.BytesIO(body), encoding='utf8')
            object_lists.append(data)
        if not object_lists:
            print(f"No data available for board {_board_}. Skipping...")
            continue
        data = pd.concat(object_lists, ignore_index=True)
        data = data.sort_values(by=posted_date_time, ascending=False)
        data = data.drop_duplicates(subset=[thread_number_key, posted_date_time], keep='last')
        data = data.rename(columns=renamed)
        data = data.dropna(subset=[p_com])
        data = process_data(data, 'posted_comment', text_clean)
        word_list = []
        for i, k in f.items():
            if i == _board_:
                k = k.split(',')
                word_list.extend(k)
        data[matches] = data[text_clean].str.findall(fr'\b({"|".join(word_list)})\b').str.join(',').replace('', np.nan)
        data = supportingcols(data, p_com)
        columns_names = board_specific.get(f"{_board_}_keys").split(',')
        columns_names = [col.strip() for col in columns_names]
        data = data[columns_names]
        data = remove_omit_ids(data, 'thread_id',omit_ids)
        date_range = get_dateRange(data)
        save_path = f'{data_prefix}/chanscope_{_board_}_{date_range}_processed.csv'
        csv_buffer = io.StringIO()
        data.to_csv(csv_buffer, index=False)
        s3_resource.Object(s3_bucket, save_path).put(Body=csv_buffer.getvalue())
    return "Process completed"