from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from ravelry_scraper.constants import DATA_DIR
from ravelry_scraper.helpers.jsonl import de_dup_jsonl
from ravelry_scraper.helpers.parquet import combine_parquet_files, de_dup_parquet
from ravelry_scraper.helpers.strings import fraction_to_float, to_plural


def extract_linked_data(series: pd.Series, category: str):
    if isinstance(series.iloc[0], (list, np.ndarray)): series = series.explode()
    series = series.dropna()
    df = series.apply(pd.Series)
    if category in ['pattern_needle_sizes', 'min_needle_size', 'max_needle_size']:
        for col in ['us','metric']:
            df[col] = df[col].apply(fraction_to_float)
        category = 'needle_sizes'
    if category == 'pattern_categories':
        extract_nested_data(df['parent'], col='parent', category=category)
        df['parent_id'] = df.apply(get_ids, axis=1, column='parent')
        df = df.drop(columns=['parent'])
    store_extracted(df, Path(f"{DATA_DIR}/extracted/{to_plural(category)}"), filetype="parquet")

def extract_nested_data(series: pd.Series, col: str, category: str):
    extracted = []
    for row in series.dropna().to_numpy():
        sub_record = row.get(col, None)
        while not pd.isnull(sub_record):
            if col in sub_record:
                sub_record[f'{col}_id'] = get_ids(sub_record, col)
            next_sub_record = sub_record.pop(col, None)
            extracted.append(sub_record)
            sub_record = next_sub_record
        
    store_extracted(pd.DataFrame(extracted), Path(f"{DATA_DIR}/extracted/{to_plural(category)}"), filetype="parquet")

def extract_users(users: pd.Series):
    if isinstance(users.iloc[0], (list, np.ndarray)): users = users.explode()
    df = users.dropna().apply(pd.Series)
    df = df[['id', 'username', 'profile_country_code']]
    df = df.drop_duplicates()

    store_extracted(df, Path(f"{DATA_DIR}/extracted/users"), filetype="parquet")

def get_ids(row, column, id='id'):
    if column not in row or row[column] is None: return np.nan
    
    value = row[column]
    if isinstance(value, (list, np.ndarray)):
        return list(filter(None, (x.get(id, np.nan) if isinstance(x, dict) else np.nan for x in value)))
    else:
        return value[id] if isinstance(value, dict) and id in value else np.nan

def get_count(row, column):
    items = row[column]
    return len(items) if isinstance(items, (list, np.ndarray)) else 0

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

def store_extracted(df, path, filetype="jsonl"):
    if filetype == "jsonl":
        _store_extracted_as_jsonl(df, path.with_suffix('.jsonl'))
    elif filetype == "parquet":
        _store_extracted_as_parquet(df, path.with_suffix('.parquet'))

def _store_extracted_as_jsonl(df, path):
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

def _store_extracted_as_parquet(df, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    is_existing = path.is_file()

    file = path.with_suffix('.new.parquet') if is_existing else path

    df = df.drop_duplicates(subset=['id'])
    df['extracted_at'] = datetime.now(UTC)

    df.to_parquet(
        file,
        engine='pyarrow',
        index=False,
        compression='snappy',
    )

    if is_existing:
        combine_parquet_files([path, file], path)

    de_dup_parquet(path)
