import uuid
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from ravelry_scraper.constants import DATA_DIR
from ravelry_scraper.helpers.parquet import combine_parquet_dir, de_dup_parquet
from ravelry_scraper.helpers.transformers import (
    extract_users,
    get_ids,
    process_datetime_columns,
    process_int_columns,
)
from ravelry_scraper.logger import get_logger

curr_file = Path(__file__).resolve()
MODULE = f"{curr_file.parent.name}/{curr_file.stem}"
logger = get_logger(MODULE)

def process_designers():
    with logger.log_duration():
        _process_designers()

def _process_designers():
    logger.info("Processing designers ...")

    raw_path = Path(f"{DATA_DIR}/raw/designers.parquet")
    raw_file = pq.ParquetFile(raw_path)
    processed_dir = Path(f"{DATA_DIR}/processed/designers")
    processed_dir.mkdir(parents=True, exist_ok=True)

    na_cols = []
    for batch in raw_file.iter_batches(batch_size=100):
        chunk = batch.to_pandas()
        cols = chunk.columns.to_list()
        max_num_na = len(chunk.index) * 0.2
        na_cols = [col for col in cols if chunk[col].isna().sum() > max_num_na]
        break

    unused_cols = ['permalink', 'notes', 'notes_html']
    drop_cols = np.union1d(na_cols, unused_cols)

    logger.info(f"Dropping columns from dataset: {drop_cols.tolist()}")

    total_retained = 0
    for idx, batch in enumerate(raw_file.iter_batches(batch_size=10_000)):
        chunk = batch.to_pandas()
        logger.verbose_info(f"Processing chunk {idx+1} with {len(chunk)} rows")

        chunk = chunk.drop(columns=[col for col in drop_cols if col in chunk.columns])
        processed_chunk = process_designer_chunk(chunk)

        logger.verbose_info(f"Processed chunk {idx+1} has {len(processed_chunk)} rows")
        total_retained += len(processed_chunk)

        processed_chunk.to_parquet(
            processed_dir / f"{uuid.uuid4().hex}.parquet",
            engine='pyarrow',
            index=False,
            compression='snappy'
        )

    logger.verbose_info(f"Total chunks processed: {idx+1}")
    logger.info(f"Total records retained: {total_retained}")

    combined_file_path = combine_parquet_dir(processed_dir)
    if combined_file_path: de_dup_parquet(combined_file_path)

    logger.info("Completed processing designers.")

def process_designer_chunk(chunk):
    extract_users(chunk['users'])
    chunk['user_ids'] = chunk.apply(get_ids, axis=1, column='users')
    chunk = chunk.drop(columns=['users'])

    chunk = process_int_columns(chunk, ['id'])
    chunk = process_int_columns(chunk, [col for col in chunk.columns if col.endswith('count')])
    chunk = process_datetime_columns(chunk, ['scraped_at'])

    chunk = chunk.rename(columns={'status_name': 'status'})

    return chunk
