import os
import configparser
import pandas as pd
import warnings
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from unicodedata import normalize
from urllib.parse import urlparse
import nltk

from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
import re
import string

warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

config_path = 'config.ini'
nltk_data_path = os.getenv('NLTK_DATA', 'nltk_data') 
if nltk_data_path not in nltk.data.path:
    nltk.data.path.append(nltk_data_path)

def read_config(section="general", config_path=config_path):
    if not os.path.exists(config_path):
        print(f"Configuration file {config_path} not found.")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    config = configparser.ConfigParser()
    config.read(config_path)
    if section not in config.sections():
        print(f"Section '{section}' not found in configuration.")
        raise KeyError(f"Section not found: {section}")
    return {key: config[section][key] for key in config[section]}

url_regex = re.compile(r'http\S+|www.\S+')
whitespace_regex = re.compile(r'\s+')
punctuation_regex = re.compile(f"([{string.punctuation}])")
non_alphanumeric_regex = re.compile(r'[^a-zA-Z0-9.,!?\' ]')
punctuation_regex = re.compile(f"([{string.punctuation}])")
contraction_mapping = pd.read_json('contraction_mapping.json', typ='series').to_dict()
config_params = read_config(section="general", config_path=config_path)
   
wiki_markup_regex = re.compile(
    r'thumb\|[\dpx]*\|?|'
    r'right\|[\dpx]*\|?|'
    r'left\|[\dpx]*\|?|'
    r'center\|[\dpx]*\|?|'
    r'[\dpx]+\|'
)

def pad_punctuation(s):
    if string_to_bool(config_params.get("padding", "False")):
        if not isinstance(s, str):
            return ""
        s = punctuation_regex.sub(r" \1 ", s)
        print(s)
        return whitespace_regex.sub(' ', s).strip()
    return s
    
def normalize_text(text):
    if isinstance(text, str):
        try:
            # Existing normalization steps
            text = url_regex.sub(lambda m: urlparse(m.group(0)).netloc.replace('www.', ''), text)
            text = normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')
            text = wiki_markup_regex.sub('', text)  # Remove wiki markup
            text = re.sub(r'\n\n.*?\n\n*?\n', ' ', text)
            text = text.replace('\n', ' ')
            text = ' '.join(BeautifulSoup(text, 'html.parser').stripped_strings)
            text = re.sub(r'>>\d+', ' ', text)
            # Revised pattern to remove 'thumb|', 'px', '200px|', 'right|', and similar patterns
            text = re.sub(r'thumb\|\d*x\d*px\|right\|', '', text)
            text = re.sub(r'thumb\|\d*x\d*px\|', '', text)
            text = re.sub(r'thumb\|', '', text)
            text = re.sub(r'\d*x\d*px\|', '', text)
            text = re.sub(r'^\s*>+', '', text, flags=re.MULTILINE)
            # Existing normalization steps continued
            if string_to_bool(config_params.get("contraction_mapping", "False")):
                text = ' '.join(contraction_mapping.get(t, t) for t in text.split())
            if string_to_bool(config_params.get("non_alpha_numeric", "False")):
                text = non_alphanumeric_regex.sub(' ', text)
            return whitespace_regex.sub(' ', text).strip()
        except ValueError:
            return text
    return text

def remove_whitespace(text):
    if isinstance(text, str):
        return " ".join(text.split())
    return text

def supportingcols(data, posted_comment):
    def count_stopwords(text):
        stop = stop_words_()
        tokenizer = RegexpTokenizer(r'\w+|\$[\d\.]+|\S+')
        word_tokens = tokenizer.tokenize(text)
        stopwords_x = [w for w in word_tokens if w in stop]
        return len(stopwords_x)

    text = data.posted_comment
    text = text.astype(str)
    wordcount = text.apply(lambda x: len(str(x).split(" ")))
    char_cnt = text.str.len()
    stop_words = data.posted_comment.apply(lambda x:count_stopwords(x))
    data['word_cnt'] = wordcount
    data['char_cnt'] = char_cnt
    data["stopwords_count"] = stop_words
    return data

def string_to_bool(string_value):
    return string_value.lower() in ['true', '1', 't', 'y', 'yes', 'on']

def stop_words_():
    return nltk.corpus.stopwords.words('english')

def remove_stop_(text):
    #stop.extend(pos_list)
    stop = stop_words_()
    tokens = [w for w in text.split() if not w in stop]
    words_kept = list()
    for i in tokens:
        words_kept.append(i)
    return (' '.join(words_kept)).strip()

def get_dateRange(data):
    data['date'] = pd.to_datetime(data['date'])
    data = data.dropna(subset='date')
    min_date = data.date.min()
    max_date = data.date.max()
    date_range = str(min_date.strftime("%Y-%m-%d")) + '_' + str(max_date.strftime("%Y-%m-%d"))
    return date_range

def remove_omit_ids(df, column_name='thread_id', omit_ids=[]):
    column_dtype = df[column_name].dtype
    casted_omit_ids = [column_dtype.type(item) for item in omit_ids]
    omit_set = set(casted_omit_ids)
    filtered_df = df[~df[column_name].isin(omit_set)]
    return filtered_df

def flatten_key_phrases(key_phrases):
    """
    Flattens the key phrases JSON into a list of tuples containing
    phrases and their associated categories.
    """
    phrases_with_category = []
    for category in key_phrases:
        category_name = category.get("category", "Uncategorized")
        phrases = category.get("phrases", [])
        for phrase in phrases:
            phrases_with_category.append((phrase, category_name))
    return phrases_with_category