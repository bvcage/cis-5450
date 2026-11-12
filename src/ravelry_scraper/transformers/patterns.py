import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import uuid

from currency_converter import CurrencyConverter
from pathlib import Path

from ..helpers.parquet import combine_parquet_dir, de_dup_parquet
from ..helpers.transformers import extract_nested_data, get_count, get_ids, process_int_columns, process_datetime_columns, process_boolean_columns
from ..logger import get_logger

curr_file = Path(__file__).resolve()
MODULE = f"{curr_file.parent.name}/{curr_file.stem}"
logger = get_logger(MODULE)

# Docs: https://www.ravelry.com/api#Pattern_full_result
def process_patterns():
    with logger.log_duration():
        _process_patterns()

def _process_patterns():
    logger.info("Processing patterns ...")

    raw_path = Path("data/raw/patterns/patterns.parquet")
    raw_file = pq.ParquetFile(raw_path)
    processed_dir = Path("data/processed/patterns")
    processed_dir.mkdir(parents=True, exist_ok=True)

    na_cols = []
    for batch in raw_file.iter_batches(batch_size=100):
        chunk = batch.to_pandas()
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
    total_records = 0
    for idx, batch in enumerate(raw_file.iter_batches(batch_size=10_000)):
        chunk = batch.to_pandas()
        logger.info(f"Processing chunk {idx+1} with {len(chunk)} rows")

        chunk = chunk.drop(columns=[col for col in drop_cols if col in chunk.columns])
        processed_chunk = process_pattern_chunk(chunk, c)

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
    chunk = process_boolean_columns(chunk, ['downloadable', 'free', 'ravelry_download'])

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
