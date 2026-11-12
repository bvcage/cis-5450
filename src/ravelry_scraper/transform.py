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

    start_time = datetime.now(UTC)


    na_cols = []
    for chunk in pd.read_json(raw_path, lines=True, chunksize=100):
        cols = chunk.columns.to_list()
        max_num_na = len(chunk.index) * 0.4
        na_cols = [col for col in cols if chunk[col].isna().sum() > max_num_na]
        break


    drop_cols = set(na_cols + unused_cols)
    drop_cols = [col for col in drop_cols if col not in must_keep_cols]
    logger.info(f"Dropping columns from dataset: {drop_cols}")


    for chunk in pd.read_json(raw_path, lines=True, chunksize=10_000):
        chunk = chunk.drop(columns=[col for col in drop_cols if col in chunk.columns])

            processed_path,
        )

    end_time = datetime.now(UTC)

    logger.log_duration(start_time, end_time)



    chunk = process_int_columns(chunk, ['id'])
    chunk = process_int_columns(chunk, [col for col in chunk.columns if col.endswith('count')])




    extract_cols = [col for col in extract_cols if col in chunk.columns]
    for col in extract_cols:
        extract_nested_data(chunk[col], col)
        chunk[f"{col}_ids"] = chunk.apply(get_ids, axis=1, column=col)
        chunk = chunk.drop(columns=[col])

