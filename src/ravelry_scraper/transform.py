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

def process_yarns():
    start_time = datetime.now(UTC)
    logger.info("Processing yarns ...")

    raw_path = Path("data/raw/yarns.jsonl")
    processed_path = Path("data/processed/yarns.jsonl")

    na_cols = []
    for chunk in pd.read_json(raw_path, lines=True, chunksize=100):
        cols = chunk.columns.to_list()
        max_num_na = len(chunk.index) * 0.4
        na_cols = [col for col in cols if chunk[col].isna().sum() > max_num_na]
        break

    must_keep_cols = ['max_gauge', 'rating_average', 'rating_total']
    unused_cols = ['permalink', 'notes_html', 'texture', 'yarn_attributes', 'yarn_provenance', 'grams', 'name', 'gauge_divisor']

    drop_cols = set(na_cols + unused_cols)
    drop_cols = [col for col in drop_cols if col not in must_keep_cols]
    logger.info(f"Dropping columns from dataset: {drop_cols}")

    # c = CurrencyConverter()

    for chunk in pd.read_json(raw_path, lines=True, chunksize=10_000):
        chunk = chunk.drop(columns=[col for col in drop_cols if col in chunk.columns])

        process_yarn_chunk(chunk).to_json(
            processed_path,
            orient='records',
            lines=True,
            mode='a',
            date_format='iso',
        )

    de_dup_jsonl(processed_path)
    end_time = datetime.now(UTC)

    logger.info("Completed processing yarns.")
    logger.log_duration(start_time, end_time)

def process_yarn_chunk(chunk):
    [ 'gauge_divisor', '',  
         'name',
       '', 'yarn_fibers', '', '',
        '']

    chunk = process_boolean_columns(chunk, ['discontinued', 'machine_washable'])

    chunk = process_int_columns(chunk, ['id'])
    chunk = process_int_columns(chunk, [col for col in chunk.columns if col.endswith('count')])
    chunk = process_int_columns(chunk, [''])

    chunk = process_datetime_columns(chunk, ['scraped_at'])

    # convert photos to photos_count
    chunk['photos_count'] = chunk.apply(get_count, axis=1, column='photos')
    chunk = chunk.drop(columns=['photos'])

    # if no range, set max to min
    chunk['max_gauge'] = chunk['max_gauge'].fillna(chunk['min_gauge'])
    chunk['min_gauge'] = chunk['min_gauge'].astype('Int64')
    chunk['max_gauge'] = chunk['max_gauge'].astype('Int64')

    extract_cols = ['min_needle_size', 'max_needle_size', 'yarn_company', 'yarn_weight']
    extract_cols = [col for col in extract_cols if col in chunk.columns]
    for col in extract_cols:
        extract_nested_data(chunk[col], col)
        chunk[f"{col}_ids"] = chunk.apply(get_ids, axis=1, column=col)
        chunk = chunk.drop(columns=[col])

    return chunk