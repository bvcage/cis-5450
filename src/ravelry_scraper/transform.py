import json
import inflect
import numpy as np
import pandas as pd
from currency_converter import CurrencyConverter
from datetime import datetime, UTC
from pathlib import Path

from .helpers.jsonl import append_to_jsonl, de_dup_jsonl
from .helpers.strings import to_plural, to_singular
from .logger import get_logger

MODULE = Path(__file__).stem

# Docs: https://www.ravelry.com/api#Pattern_full_result
def process_patterns():
    start_time = datetime.now(UTC)
    logger = get_logger(MODULE)
    logger.info("Processing projects ...")

    raw_path = Path("data/raw/patterns.jsonl")
    processed_path = Path("data/processed/patterns.jsonl")

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

        process_pattern_chunk(chunk, c).to_json(
            processed_path,
            orient='records',
            lines=True,
            mode='a',
        )

        break

    de_dup_jsonl(processed_path)
    end_time = datetime.now(UTC)

    logger.info("Completed processing patterns.")
    logger.log_duration(start_time, end_time)


def store_extracted(df, path):
    df = df.drop_duplicates()
    df['extracted_at'] = datetime.now(UTC)

    df.to_json(
        path,
        orient='records',
        lines=True,
        mode='a',
    )

    de_dup_jsonl(path)

def extract_nested_data(series: pd.Series, category: str):
    series = series.dropna()
    if isinstance(series.iloc[0], list): series = series.explode()
    df = series.apply(pd.Series)
    store_extracted(df, Path(f"data/extracted/{category}.jsonl"))

def get_ids(row, column, id='id'):
    value = row[column]
    if isinstance(value, list):
        return list(filter(None, map(lambda x: x[id] if id in x else np.nan, value)))
    else:
        return value[id] if id in value else np.nan

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

    # TODO handle pattern category parents?
    # TODO 'pattern_needle_sizes',
    # extract nested data
    extract_cols = ['languages', 'yarn_weight', 'craft', 'pattern_categories', 'pattern_attributes', 'pattern_type']
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


def process_pattern_attributes():
    df = pd.read_json("data/raw/pattern_attributes.jsonl", lines=True)

    # rename group columns
    df = df.add_prefix('group_')

    attr_col = 'group_pattern_attributes'
    child_col = 'group_children'

    depth = 1
    max_depth = 10
    while depth < max_depth:
        # pattern_attributes
        if isinstance(df[attr_col].dropna().iloc[0], list): df = df.explode(attr_col)
        expanded = df[attr_col].apply(pd.Series).add_prefix(f'{attr_col}_')
        df = pd.concat([df.drop(columns=[attr_col]), expanded], axis=1)

        # children
        if isinstance(df[child_col].dropna().iloc[0], list): df = df.explode(child_col)
        expanded = df[child_col].apply(pd.Series).add_prefix(f'{child_col}_sub_group_')
        df = pd.concat([df.drop(columns=[child_col]), expanded], axis=1)

        attr_col = f'{child_col}_sub_group_pattern_attributes'
        child_col = f'{child_col}_sub_group_children'

        if not attr_col in df.columns: break
        if not child_col in df.columns: break

        depth += 1

    drop_cols = [col for col in df.columns if col.endswith(('0', 'sort_order'))]
    df = df.drop(columns=drop_cols)




    df = pd.read_json("data/raw/pattern_attributes.jsonl", lines=True)
    attrs = df.explode('pattern_attributes')
    keep_cols = ['children', 'pattern_attributes']
    drop_cols = [col for col in attrs.columns if col not in keep_cols]
    attrs.drop(columns=drop_cols)





    attr_col = 'group_pattern_attributes'
    children_col = 'group_children'

    # explode columns
    def explode_and_replace(df, col):
        df = df.explode(col)
        attr_cols = df[col].apply(pd.Series).add_prefix(f'{col}_')
        return pd.concat([df.drop(columns=[col]), attr_cols], axis=1)


    col = ''


    # explode pattern_attributes
    
    df = df.drop(columns=['sort_order'])

    cols = df.columns.tolist()
    main_cols = ['id','name']
    group_cols = [col for col in cols if 'group' in col]
    middle_cols = [col for col in cols if col not in main_cols and col not in group_cols]
    cols = main_cols + middle_cols + group_cols

    df = df[cols]

    # iterate thru children

    # explode children column
    if isinstance(df['group_children_pattern_attributes'].dropna().iloc[0], list): df = df.explode('group_children')

    sub_df = df['children'].apply(pd.Series)
    if 0 in sub_df.columns: sub_df = sub_df.drop(columns=[0])
    sub_df = sub_df.rename(columns={'id': 'group_id', 'name': 'group_name', 'permalink': 'group_permalink'})
    

    # explode pattern_attributes
    sub_df = sub_df.explode('pattern_attributes')
    attr_cols = sub_df['pattern_attributes'].apply(pd.Series)
    sub_df = sub_df.add_prefix('sub_')
    sub_df = pd.concat([sub_df, attr_cols], axis=1)
    if 0 in sub_df.columns: sub_df = sub_df.drop(columns=[0])
    
    # drop cols
    drop_cols = [col for col in [0, 'sort_order'] if col in sub_df.columns]
    sub_df = sub_df.drop(columns=drop_cols)

    cols = sub_df.columns.tolist()
    main_cols = ['id','name']
    group_cols = [col for col in cols if 'group' in col]
    middle_cols = [col for col in cols if col not in main_cols and col not in group_cols]
    cols = main_cols + middle_cols + group_cols

    sub_df = sub_df[cols]



    # iterate thru children


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
