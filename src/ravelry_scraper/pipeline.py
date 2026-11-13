import pandas as pd
import uuid

from datetime import datetime, UTC
from pathlib import Path

from .client import RavelryClient
from .exceptions import RavelryScraperError, RequestError
from .helpers.jsonl import append_to_jsonl
from .helpers.parquet import combine_parquet_dir, de_dup_parquet
from .logger import get_logger
from .state import StateManager
from .stats import StatsManager
from .transformers.projects import process_projects

MODULE = Path(__file__).stem
logger = get_logger(MODULE)

def initialize_pipeline():
    client = RavelryClient()
    state = StateManager()
    stats = StatsManager()

    return (
        client,
        state,
        stats,
    )

def run_projects_pipeline(max_pages = None):
    run_search_pipeline("projects", max_pages)
    process_projects()

def run_pipeline(
    category="patterns",
    max_count=10_000,
    batch_size=1_000,
    from_last_success=True,
):
    if not category in ["patterns", "designers", "yarns"]: return # todo

    with logger.log_duration():
        _run_pipeline(
            category,
            max_count,
            batch_size,
            from_last_success,
        )

def _run_pipeline(
    category,
    max_count,
    batch_size,
    from_last_success,
):
    client, state, stats = initialize_pipeline()

    cat_dir = Path(f"data/raw/{category}")
    cat_dir.mkdir(parents=True, exist_ok=True)

    batch_size = min(max_count, batch_size)

    consecutive_error_count = 0
    total_count = 0

    while total_count < max_count and consecutive_error_count < 100:
        records = []
        errors = []

        last_id = state.get_cursor(category)
        last_successful_id = state.get_cursor(category, "max_successful_id")
        num_ids = min(max_count - total_count, batch_size)

        id_range = range(last_successful_id + 1, last_successful_id + num_ids + 1) if from_last_success else range(last_id + 1, last_id + num_ids + 1)

        try:
            for id in id_range:
                endpoint = f"/{category}/{id}.json"

                try:
                    data = client.fetch_endpoint(endpoint)
                    first_key = next(iter(data))
                    record = data[first_key]
                    records.append(record | { 'scraped_at': datetime.now(UTC).isoformat() })

                    consecutive_error_count = 0
                    last_successful_id = max(last_successful_id, id)

                except RavelryScraperError as err:
                    consecutive_error_count += 1
                    errors.append(vars(err) | { 'timestamp': datetime.now(UTC).isoformat() })

                    if consecutive_error_count >= 100:
                        logger.info(f"Stopping scrape after {consecutive_error_count} consecutive errors.")
                        return
                    if isinstance(err, RequestError):
                        logger.error(f"Stopping scrape after request failure on endpoint: {endpoint}\nLeaving checkpoint at {last_id} for retry.")
                        return

                total_count += 1
                last_id = max(last_id, id)
        finally:
            logger.info("Saving batch results & updating checkpoint...")

            if records:
                df = pd.DataFrame(records)
                df.to_parquet(
                    cat_dir / f"{uuid.uuid4().hex}.parquet",
                    engine='pyarrow',
                    index=False,
                    compression='snappy'
                )

            if errors: append_to_jsonl(errors, Path(f"data/failed/{category}.jsonl"))

            state.update_checkpoint(category, {
                "max_successful_id": last_successful_id,
                "max_id": last_id,
            })

    logger.info("Combining parquet files...")
    combine_parquet_dir(cat_dir, cat_dir / f"{category}.parquet")

    logger.info(f"Scrape complete for {total_count} {category}.")

def run_search_pipeline(
    category="projects",
    max_pages=10_000,
):
    if category not in ["projects"]: return # todo

    with logger.log_duration():
        _run_search_pipeline(
            category,
            max_pages,
        )

def _run_search_pipeline(
    category,
    max_pages,
):
    client, _, _ = initialize_pipeline()

    cat_dir = Path(f"data/raw/{category}")
    cat_dir.mkdir(parents=True, exist_ok=True)

    endpoint = f"/{category}/search.json"
    records = []
    page_num = 1

    consecutive_error_count = 0

    while page_num <= max_pages and consecutive_error_count < 5:
        try:
            data = client.fetch_endpoint(endpoint, params={"page": page_num})
            first_key = next(iter(data))
            results = data[first_key]
            for result in results:
                records.append(result | { 'scraped_at': datetime.now(UTC).isoformat() })

            if page_num == 1:
                paginator = data['paginator']
                if 'last_page' in paginator:
                    max_pages = min(max_pages, paginator['last_page'])
            
            consecutive_error_count = 0
        except RavelryScraperError as err:
            consecutive_error_count += 1

            if page_num > paginator['last_page']:
                logger.info(f"Tried to scrape non-existent page number {page_num}: {paginator}")
                return
        finally:
            page_num += 1
            if records:
                df = pd.DataFrame(records)
                df.to_parquet(
                    cat_dir / f"{uuid.uuid4().hex}.parquet",
                    engine='pyarrow',
                    index=False,
                    compression='snappy'
                )
                records = []

    logger.info("Combining parquet files...")
    combined_file_path = combine_parquet_dir(cat_dir, cat_dir / f"{category}.parquet")
    logger.info(f"Deduping {category}...")
    de_dup_parquet(combined_file_path)

    logger.info(f"Scrape complete for {category}.")
