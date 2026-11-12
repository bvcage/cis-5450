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
from ..helpers.parquet import combine_parquet, de_dup_parquet
from ..helpers.transformers import extract_nested_data, extract_users, get_count, get_ids, process_boolean_columns, process_datetime_columns, process_int_columns
from ..logger import get_logger

from ..helpers.parquet import combine_parquet, de_dup_parquet
from ..helpers.transformers import extract_nested_data, get_count, get_ids, process_int_columns, process_datetime_columns, process_boolean_columns
from ..logger import get_logger
from ..stats import StatsManager

curr_file = Path(__file__).resolve()
MODULE = f"{curr_file.parent.name}/{curr_file.stem}"
logger = get_logger(MODULE)
stats = StatsManager()

def process_projects():
    with logger.log_duration():
        _process_projects()

def _process_projects():
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

def process_project_chunk(chunk):
    extract_users(chunk['user'])
    chunk = chunk.drop(columns=['user'])

    # chunk = process_int_columns(chunk, bool_cols)
    chunk = process_int_columns(chunk, ['id', 'craft_id', 'pattern_id', 'user_id'])
    chunk = process_int_columns(chunk, [col for col in chunk.columns if col.endswith('count')])
    chunk = process_datetime_columns(chunk, ['completed', 'created_at', 'started', 'updated_at', 'scraped_at', 'project_status_changed'])

    chunk = chunk.rename(columns={'status_name': 'status'})

    return chunk
