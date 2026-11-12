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

from .helpers.jsonl import de_dup_jsonl
from .helpers.parquet import combine_parquet, de_dup_parquet
from .helpers.transformers import extract_nested_data, extract_users, get_count, get_ids, process_boolean_columns, process_datetime_columns, process_int_columns
from .logger import get_logger

MODULE = Path(__file__).stem
logger = get_logger(MODULE)

# Docs: https://www.ravelry.com/api#Pattern_full_result
def process_patterns():
    start_time = datetime.now(UTC)
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

def process_pattern_chunk(chunk, c):
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

def process_project_chunk(chunk):
    extract_users(chunk['user'])
    chunk = chunk.drop(columns=['user'])

    # chunk = process_int_columns(chunk, bool_cols)
    chunk = process_int_columns(chunk, ['id', 'craft_id', 'pattern_id', 'user_id'])
    chunk = process_int_columns(chunk, [col for col in chunk.columns if col.endswith('count')])
    chunk = process_datetime_columns(chunk, ['completed', 'created_at', 'started', 'updated_at', 'scraped_at', 'project_status_changed'])

    chunk = chunk.rename(columns={'status_name': 'status'})

    return chunk




