import json
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
from currency_converter import CurrencyConverter
from datetime import datetime, UTC
from pathlib import Path

from .helpers.jsonl import append_to_jsonl, de_dup_jsonl
from .helpers.parquet import combine_parquet, de_dup_parquet
from .helpers.strings import to_plural, to_singular, fraction_to_float
from .logger import get_logger

MODULE = Path(__file__).stem

# Docs: https://www.ravelry.com/api#Pattern_full_result
def process_patterns():
    start_time = datetime.now(UTC)
    logger = get_logger(MODULE)
    logger.info("Processing projects ...")

    raw_path = Path("data/raw/patterns.jsonl")
    processed_dir = Path("data/processed/patterns")
    processed_dir.mkdir(parents=True, exist_ok=True)

    na_cols = []
    for chunk in pd.read_json(raw_path, lines=True, chunksize=100):
        cols = chunk.columns.to_list()
        max_num_na = len(chunk.index) * 0.4
        na_cols = [col for col in cols if chunk[col].isna().sum() > max_num_na]
        break

    must_keep_cols = ['currency', 'price', 'yardage_max', 'difficulty_average', 'rating_average']
    unused_cols = ['generally_available', 'has_uk_terminology', 'has_us_terminology', 'pdf_url', 'permalink', 'url', 'personal_attributes', 'sizes_available', 'unlisted_product_ids', 'currency_symbol', 'download_location', 'pdf_in_library', 'volumes_in_library', 'yardage_description', 'notes_html', 'notes', 'printings', 'product_id', 'yarn_list_type']

    drop_cols = set(na_cols + unused_cols)
    drop_cols = [col for col in drop_cols if col not in must_keep_cols]
    logger.info(f"Dropping columns from dataset: {drop_cols}")

    c = CurrencyConverter()

    for chunk in pd.read_json(raw_path, lines=True, chunksize=10_000):
        chunk = chunk.drop(columns=[col for col in drop_cols if col in chunk.columns])

        processed_path = processed_dir / f"{uuid.uuid4().hex}.parquet"
        process_pattern_chunk(chunk, c).to_parquet(
            processed_path,
            engine='pyarrow',
            index=False,
        )

    combined_file = combine_parquet(processed_dir)
    if combined_file: de_dup_parquet(combined_file)
    end_time = datetime.now(UTC)

    logger.info("Completed processing patterns.")
    logger.log_duration(start_time, end_time)


def store_extracted(df, path):
    path.parent.mkdir(parents=True, exist_ok=True)

    df = df.drop_duplicates(subset=['id'])
    df['extracted_at'] = datetime.now(UTC)

    df.to_json(
        path,
        orient='records',
        lines=True,
        mode='a',
        date_format='iso',
    )

    de_dup_jsonl(path)

def extract_recursively(row: pd.Series, col: str, category: str):
    if row is None or pd.isnull(row[col]): return # skip invalid rows

    sub_record = row.get(col, None)
    while not pd.isnull(sub_record):
        sub_record[f'{col}_id'] = get_ids(row, col)
        next_sub_record = sub_record.pop(col, None)
        store_extracted(pd.DataFrame([sub_record]), Path(f"data/extracted/{category}.jsonl"))
        sub_record = next_sub_record

def extract_nested_data(series: pd.Series, category: str):
    if isinstance(series.iloc[0], list): series = series.explode()
    series = series.dropna()
    df = series.apply(pd.Series)
    if category in ['pattern_needle_sizes', 'min_needle_sizes', 'max_needle_sizes']:
        for col in ['us','metric']:
            df[col] = df[col].apply(fraction_to_float)
        category = 'needle_sizes'
    # if category == 'pattern_categories':
        # try:
            # df['parent_id'] = df.apply(get_ids, axis=1, column='parent', id='id')
            # df.apply(extract_recursively, axis=1, col='parent', category=category)
            # df = df.drop(columns=['parent'])
        # except Exception as e:
        #     breakpoint()
    store_extracted(df, Path(f"data/extracted/{category}.jsonl"))

def get_ids(row, column, id='id'):
    value = row[column]
    if isinstance(value, list):
        return list(filter(None, map(lambda x: x[id] if id in x else np.nan, value)))
    else:
        return value[id] if isinstance(value, dict) and id in value else np.nan

def get_count(row, column):
    items = row[column]
    return len(items) if isinstance(items, list) else 0

def process_pattern_chunk(chunk, c):
    logger = get_logger(MODULE)

    # drop gauge columns
    gauge_cols = [col for col in chunk.columns if 'gauge' in col]
    chunk = chunk.drop(columns=gauge_cols)

    # convert photos to photos_count
    chunk['photos_count'] = chunk.apply(get_count, axis=1, column='photos')
    chunk = chunk.drop(columns=['photos'])

    # ensure / convert columns to integers
    chunk = process_int_columns(chunk, ['id'])
    chunk = process_int_columns(chunk, [col for col in chunk.columns if col.endswith('count')])
    bool_cols = ['downloadable', 'free', 'ravelry_download']
    chunk = process_int_columns(chunk, bool_cols)

    # if no range, set yardage_max equal to yardage (min)
    chunk['yardage_max'] = chunk['yardage_max'].fillna(chunk['yardage'])

    # convert datetimes
    datetime_cols = ['created_at', 'updated_at', 'published', 'scraped_at']
    chunk = process_datetime_columns(chunk, datetime_cols)

    # convert datatypes - integers
    chunk['yardage'] = chunk['yardage'].astype('Int64')
    chunk['yardage_max'] = chunk['yardage_max'].astype('Int64')

    # convert currencies
    chunk['currency'] = chunk['currency'].replace(r'^\s*$', np.nan, regex=True)
    curr_list = chunk['currency'].dropna().unique()
    rate_map = { curr: c.convert(1, curr, 'USD') for curr in curr_list}
    rate_map['nan'] = None

    chunk['price_usd'] = (chunk['price'] * chunk['currency'].map(rate_map)).round(2)

    # extract nested data
    extract_cols = ['languages', 'yarn_weight', 'craft', 'pattern_categories', 'pattern_attributes', 'pattern_type', 'pattern_needle_sizes']
    extract_cols = [col for col in extract_cols if col in chunk.columns]
    for col in extract_cols:
        logger.info(f"Extracting data from {col}...")
        extract_nested_data(chunk[col], col)
        chunk[f"{col}_ids"] = chunk.apply(get_ids, axis=1, column=col)
        chunk = chunk.drop(columns=[col])

    # extract yarn from packs (special case)
    chunk['yarn_ids'] = chunk.apply(get_ids, axis=1, column='packs', id='yarn_id')
    chunk = chunk.drop(columns=['packs'])

    # extract designer from pattern_author
    chunk['designer_id'] = chunk.apply(get_ids, axis=1, column='pattern_author')
    chunk = chunk.drop(columns=['pattern_author'])

    return chunk


def process_projects():
    start_time = datetime.now(UTC)
    logger = get_logger(MODULE)
    logger.info("Processing projects ...")

    raw_path = Path("data/raw/projects.jsonl")
    processed_path = Path("data/processed/projects.jsonl")
    processed_path.parent.mkdir(parents=True, exist_ok=True)

    na_cols = []
    for chunk in pd.read_json(raw_path, lines=True, chunksize=100):
        cols = chunk.columns.to_list()
        max_num_na = len(chunk.index) * 0.2
        na_cols = [col for col in cols if chunk[col].isna().sum() > max_num_na]
        break

    unused_cols = ['made_for', 'first_photo', 'name', 'permalink', 'links', 'pattern_name', 'project_status_id', 'gauge_pattern', 'completed_day_set', 'started_day_set']
    drop_cols = np.union1d(na_cols, unused_cols)

    logger.info(f"Columns to drop: {drop_cols.tolist()}")

    for chunk in pd.read_json(raw_path, lines=True, chunksize=10_000):
        chunk = chunk.drop(columns=drop_cols)

        process_project_chunk(chunk).to_json(
            processed_path,
            orient='records',
            lines=True,
            mode='a',
            date_format='iso',
        )

    de_dup_jsonl(processed_path)
    end_time = datetime.now(UTC)

    logger.info("Completed processing projects.")
    logger.log_duration(start_time, end_time)


def extract_users(users: pd.Series):
    df = users.dropna().apply(pd.Series)
    df = df[['id', 'username', 'profile_country_code']]
    df = df.drop_duplicates()

    store_extracted(df, Path("data/extracted/users.jsonl"))


def process_project_chunk(chunk):
    extract_users(chunk['user'])
    chunk = chunk.drop(columns=['user'])

    # chunk = process_int_columns(chunk, bool_cols)
    chunk = process_int_columns(chunk, ['id', 'craft_id', 'pattern_id', 'user_id'])
    chunk = process_int_columns(chunk, [col for col in chunk.columns if col.endswith('count')])
    chunk = process_datetime_columns(chunk, ['completed', 'created_at', 'started', 'updated_at', 'scraped_at', 'project_status_changed'])

    chunk = chunk.rename(columns={'status_name': 'status'})

    return chunk


def process_boolean_columns(chunk, bool_cols):
    bool_cols = [col for col in bool_cols if col in chunk.columns]
    chunk = chunk.dropna(subset=bool_cols)
    for col in bool_cols:
        chunk[col] = chunk[col].astype(int)
    return chunk

    
def process_int_columns(chunk, int_cols):
    int_cols = [col for col in int_cols if col in chunk.columns]
    chunk = chunk.dropna(subset=int_cols)
    for col in int_cols:
        chunk[col] = chunk[col].astype(int)
    return chunk

# TODO: fix overflow error - need to ensure dates are valid
def process_datetime_columns(chunk, datetime_cols):
    datetime_cols = [col for col in datetime_cols if col in chunk.columns]
    for col in datetime_cols:
        chunk[col] = pd.to_datetime(chunk[col], utc=True)
    return chunk
