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

from ..helpers.jsonl import de_dup_jsonl
from ..helpers.parquet import combine_parquet_dir, de_dup_parquet
from ..helpers.transformers import extract_nested_data, extract_users, get_count, get_ids, process_boolean_columns, process_datetime_columns, process_int_columns
from ..logger import get_logger

curr_file = Path(__file__).resolve()
MODULE = f"{curr_file.parent.name}/{curr_file.stem}"
logger = get_logger(MODULE)

def process_yarns():
    with logger.log_duration():
        _process_yarns()

def _process_yarns():
    logger.info("Processing yarns ...")

    raw_path = Path("data/raw/yarns.parquet")
    raw_file = pq.ParquetFile(raw_path)
    processed_dir = Path("data/processed/yarns")
    processed_dir.mkdir(parents=True, exist_ok=True)

    na_cols = []
    for batch in raw_file.iter_batches(batch_size=100):
        chunk = batch.to_pandas()
        cols = chunk.columns.to_list()
        max_num_na = len(chunk.index) * 0.4
        na_cols = [col for col in cols if chunk[col].isna().sum() > max_num_na]
        break

    must_keep_cols = ['max_gauge', 'rating_average', 'rating_total']
    unused_cols = ['permalink', 'notes_html', 'texture', 'yarn_attributes', 'yarn_provenance', 'grams', 'name', 'gauge_divisor']

    drop_cols = set(na_cols + unused_cols)
    drop_cols = [col for col in drop_cols if col not in must_keep_cols]

    logger.info(f"Dropping columns from dataset: {drop_cols}")

    total_records = 0
    for idx, batch in enumerate(raw_file.iter_batches(batch_size=10_000)):
        chunk = batch.to_pandas()
        logger.info(f"Processing chunk {idx+1} with {len(chunk)} rows")

        chunk = chunk.drop(columns=[col for col in drop_cols if col in chunk.columns])
        processed_chunk = process_yarn_chunk(chunk)

        logger.info(f"Processed chunk {idx+1} has {len(processed_chunk)} rows")
        total_records += len(processed_chunk)

        processed_chunk.to_parquet(
            processed_dir / f"{uuid.uuid4().hex}.parquet",
            engine='pyarrow',
            index=False,
            compression='snappy'
        )

    logger.info(f"Total chunks processed: {idx+1}")
    logger.info(f"Total records processed: {total_records}")

    combined_file_path = combine_parquet_dir(processed_dir)
    if combined_file_path: de_dup_parquet(combined_file_path)

    logger.info("Completed processing yarns.")

def process_yarn_chunk(chunk):
    chunk = process_boolean_columns(chunk, ['discontinued', 'machine_washable'])

    chunk = process_int_columns(chunk, ['id'])
    chunk = process_int_columns(chunk, [col for col in chunk.columns if col.endswith('count')])
    chunk = process_int_columns(chunk, ['rating_total', 'yardage'])

    chunk = process_datetime_columns(chunk, ['scraped_at'])

    # convert photos to photos_count
    chunk['photos_count'] = chunk.apply(get_count, axis=1, column='photos')
    chunk = chunk.drop(columns=['photos'])

    # if no range, set max to min
    chunk['max_gauge'] = chunk['max_gauge'].fillna(chunk['min_gauge'])

    extract_cols = ['min_needle_size', 'max_needle_size', 'yarn_company', 'yarn_fibers', 'yarn_weight']
    extract_cols = [col for col in extract_cols if col in chunk.columns]
    for col in extract_cols:
        extract_nested_data(chunk[col], col)
        chunk[f"{col}_ids"] = chunk.apply(get_ids, axis=1, column=col)
        chunk = chunk.drop(columns=[col])

    return chunk