import boto3
import pandas as pd
import numpy as np
import io

import os
import json
import glob
from pathlib import Path
import configparser
import warnings
from datetime import datetime, timedelta

from utils import read_config, supportingcols, get_dateRange, remove_whitespace, pad_punctuation, normalize_text, remove_omit_ids, flatten_key_phrases

import re
from fuzzywuzzy import fuzz

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
padding_data = s3['padding_data']
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

with open('key_phrases.json', 'r') as f:
    key_phrases = json.load(f)

def handle_process(event, context):
    s3_resource = boto3.resource('s3')
    for _board_ in boards:
        print(f"Processing board: {_board_}")
        filter_prefix = f"{raw_prefix}/{_board_}_{padding_data}"
        print(f"Filtering S3 bucket '{s3_bucket}' with prefix: '{filter_prefix}'")
        
        bucket_objects = s3_resource.Bucket(s3_bucket).objects.filter(Prefix=filter_prefix)
        object_lists = []
        file_count = 0
        
        for obj in bucket_objects:
            file_count += 1
            print(f"Found file [{file_count}]: {obj.key}")
            try:
                body = obj.get()['Body'].read()
                data = pd.read_csv(io.BytesIO(body), encoding='utf8')
                print(f"  Loaded {len(data)} rows from {obj.key}")
                object_lists.append(data)
            except Exception as e:
                print(f"  ERROR reading {obj.key}: {str(e)}")
                continue
        
        print(f"Total files found for {_board_}: {file_count}")
        
        if not object_lists:
            print(f"No data available for board {_board_}. Skipping...")
            continue
        
        print(f"Concatenating {len(object_lists)} dataframes...")
        data = pd.concat(object_lists, ignore_index=True)
        print(f"Combined data shape: {data.shape}")
        print(f"Columns available: {list(data.columns)}")
        
        print(f"Sorting by {posted_date_time}...")
        data = data.sort_values(by=posted_date_time, ascending=False)
        
        print(f"Deduplicating by [{thread_number_key}, {posted_date_time}]...")
        before_dedup = len(data)
        data = data.drop_duplicates(subset=[thread_number_key, posted_date_time], keep='last')
        print(f"Deduplicated: {before_dedup} -> {len(data)} rows")
        
        print(f"Renaming columns: {renamed}")
        data = data.rename(columns=renamed)
        
        print(f"Dropping rows with NA in '{p_com}'...")
        before_dropna = len(data)
        data = data.dropna(subset=[p_com])
        print(f"After dropna: {before_dropna} -> {len(data)} rows")
        
        if len(data) == 0:
            print(f"No valid data after dropna for board {_board_}. Skipping...")
            continue
        
        print("Processing text and matching key phrases...")
        data = process_data_with_regex_and_partial_match(data, 'posted_comment', text_clean, key_phrases)
        
        print("Adding supporting columns...")
        data = supportingcols(data, p_com)
        
        columns_names = board_specific.get(f"{_board_}_keys").split(',')
        columns_names = [col.strip() for col in columns_names]
        print(f"Selecting columns for {_board_}: {columns_names}")
        
        missing_cols = [col for col in columns_names if col not in data.columns]
        if missing_cols:
            print(f"WARNING: Missing columns: {missing_cols}")
            print(f"Available columns: {list(data.columns)}")
        
        data = data[columns_names]
        data = remove_omit_ids(data, 'thread_id', omit_ids)
        print(f"Final data shape: {data.shape}")
        
        # Save processed data
        date_range = get_dateRange(data)
        save_path = f'{data_prefix}/chanscope_{_board_}_{date_range}_processed.csv'
        print(f"Saving to S3: {s3_bucket}/{save_path}")
        csv_buffer = io.StringIO()
        data.to_csv(csv_buffer, index=False)
        s3_resource.Object(s3_bucket, save_path).put(Body=csv_buffer.getvalue())
        print(f"Successfully saved {len(data)} rows for board {_board_}")
    return "Process completed"

def process_data(data, input_col, clean_col):
    data[input_col] = data[input_col].astype(str)
    data[clean_col] = data[input_col].apply(normalize_text).apply(remove_whitespace).apply(pad_punctuation)
    return data[data[clean_col].notnull() & data[clean_col].str.strip().astype(bool)]

def regex_partial_match(row_text, phrases_with_category, threshold=70):
    """
    Perform partial matches using regex and fuzzy logic for higher accuracy.
    """
    for phrase, category in phrases_with_category:
        # Create a regex pattern to match a phrase (with word boundaries)
        pattern = re.compile(r'\b' + re.escape(phrase) + r'\b', re.IGNORECASE)
        if pattern.search(row_text):  # Check for regex match
            similarity = fuzz.partial_ratio(row_text, phrase)  # Fuzzy match score
            if similarity >= threshold:  # Check similarity threshold
                return phrase, category, similarity  # Return the first match
    return None, None, None
    
def process_data_with_regex_and_partial_match(data, input_col, clean_col, key_phrases):
    data = process_data(data, input_col, clean_col)
    if clean_col not in data.columns:
        raise KeyError(f"The column '{clean_col}' does not exist in the DataFrame.")
    phrases_with_category = flatten_key_phrases(key_phrases)
    if not phrases_with_category:
        print("Warning: No key phrases found in key_phrases.json. Skipping phrase matching.")
        data['matches'] = None
        data['category'] = None
        data['similarity'] = None
        return data
    match_results = data[clean_col].apply(
        lambda x: regex_partial_match(x, phrases_with_category) if pd.notna(x) else (None, None, None))
    data.loc[:, 'matches'], data.loc[:, 'category'], data.loc[:, 'similarity'] = zip(*match_results)
    return data