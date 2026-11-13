import numpy as np
import pyarrow.parquet as pq
import uuid

from pathlib import Path
from ravelry_scraper.constants import DATA_DIR

from ..helpers.parquet import combine_parquet_dir, de_dup_parquet
from ..helpers.transformers import extract_users, process_datetime_columns, process_int_columns
from ..logger import get_logger

curr_file = Path(__file__).resolve()
MODULE = f"{curr_file.parent.name}/{curr_file.stem}"
logger = get_logger(MODULE)

def process_projects():
    with logger.log_duration():
        _process_projects()

def _process_projects():
    logger.info("Processing projects ...")

    raw_path = Path(f"{DATA_DIR}/raw/projects.parquet")
    raw_file = pq.ParquetFile(raw_path)
    processed_dir = Path(f"{DATA_DIR}/processed/projects")
    processed_dir.mkdir(parents=True, exist_ok=True)

    na_cols = []
    for batch in raw_file.iter_batches(batch_size=100):
        chunk = batch.to_pandas()
        cols = chunk.columns.to_list()
        max_num_na = len(chunk.index) * 0.2
        na_cols = [col for col in cols if chunk[col].isna().sum() > max_num_na]
        break

    must_keep_cols = ['rating']
    unused_cols = ['made_for', 'first_photo', 'name', 'permalink', 'links', 'pattern_name', 'project_status_id', 'gauge_pattern', 'completed_day_set', 'started_day_set', 'size', 'tag_names']

    drop_cols = set(na_cols + unused_cols)
    drop_cols = [col for col in drop_cols if col not in must_keep_cols]

    logger.info(f"Dropping columns from dataset: {', '.join(drop_cols)}")

    total_records = 0
    for idx, batch in enumerate(raw_file.iter_batches(batch_size=10_000)):
        chunk = batch.to_pandas()
        logger.verbose_info(f"Processing chunk {idx+1} with {len(chunk)} rows")

        chunk = chunk.drop(columns=[col for col in drop_cols if col in chunk.columns])
        processed_chunk = process_project_chunk(chunk)

        logger.verbose_info(f"Processed chunk {idx+1} has {len(processed_chunk)} rows")
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

    logger.info("Completed processing projects.")

def process_project_chunk(chunk):
    extract_users(chunk['user'])
    chunk = chunk.drop(columns=['user'])

    chunk = process_int_columns(chunk, ['id', 'craft_id', 'pattern_id', 'user_id'])
    chunk = process_int_columns(chunk, [col for col in chunk.columns if col.endswith('count')])
    chunk = process_datetime_columns(chunk, ['completed', 'created_at', 'started', 'updated_at', 'scraped_at', 'project_status_changed'])

    chunk = chunk.rename(columns={'status_name': 'status'})

    return chunk
