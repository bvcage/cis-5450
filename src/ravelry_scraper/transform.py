import json
import inflect
import numpy as np
import pandas as pd
from datetime import datetime, UTC
from pathlib import Path

from .helpers.jsonl import append_to_jsonl, de_dup_jsonl
from .helpers.strings import to_plural, to_singular
from .logger import get_logger

MODULE = Path(__file__).stem

def process_projects():
    start_time = datetime.now(UTC)
    logger = get_logger(MODULE)
    logger.info("Processing projects ...")

    raw_path = Path("data/raw/projects.jsonl")
    processed_path = Path("data/processed/projects.jsonl")

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
        )

    de_dup_jsonl(processed_path)
    end_time = datetime.now(UTC)

    logger.info("Completed processing projects.")
    logger.log_duration(start_time, end_time)


def extract_users(user_list: pd.Series):
    extracted_users_path = Path("data/extracted/users.jsonl")

    df = user_list.apply(pd.Series)
    df = df[['id', 'username', 'profile_country_code']]
    df = df.drop_duplicates()
    df['extracted_at'] = datetime.now(UTC)

    df.to_json(
        extracted_users_path,
        orient='records',
        lines=True,
        mode='a',
    )

    de_dup_jsonl(extracted_users_path)


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

def process_datetime_columns(chunk, datetime_cols):
    datetime_cols = [col for col in datetime_cols if col in chunk.columns]
    for col in datetime_cols:
        chunk[col] = pd.to_datetime(chunk[col], utc=True)
    return chunk
