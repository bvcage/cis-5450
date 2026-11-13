import pandas as pd
import uuid

from datetime import datetime, UTC
from pathlib import Path

from ravelry_scraper.helpers.strings import to_plural, to_singular

from .client import RavelryClient
from .constants import DATA_DIR, SUPPORTED_PIPELINES, SUPPORTED_SEARCH_PIPELINES
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
    if not category in SUPPORTED_PIPELINES: return # todo

    logger.info(f"Scraping {to_plural(category)}")
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

    cat_dir = Path(f"{DATA_DIR}/raw/{category}")
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
                last_id = max(last_id, id)
                total_count += 1

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
                        logger.info(f"Stopping scrape after {consecutive_error_count} consecutive errors.", extra={"verbose": True})
                        break
                    if isinstance(err, RequestError):
                        logger.error(f"Stopping scrape after request failure on endpoint: {endpoint}\nLeaving checkpoint at {last_id} for retry.", extra={"verbose": True})
                        break
        finally:
            logger.info(f"Saving {len(records)} {to_singular(category)} results & updating checkpoint", extra={"verbose": True})

            if records:
                df = pd.DataFrame(records)
                df.to_parquet(
                    cat_dir / f"{uuid.uuid4().hex}.parquet",
                    engine='pyarrow',
                    index=False,
                    compression='snappy'
                )

            if errors: append_to_jsonl(errors, Path(f"{DATA_DIR}/failed/{category}.jsonl"))

            state.update_checkpoint(category, {
                "max_successful_id": last_successful_id,
                "max_id": last_id,
            })

    if records:
        logger.info("Appending files")
        combine_parquet_dir(cat_dir, cat_dir / f"{category}.parquet")
        logger.info("Deduplicating records")
        de_dup_parquet(cat_dir / f"{category}.parquet")

    logger.info(f"Scrape complete for {total_count} endpoints: {len(records)} new {to_plural(category)} added")

def run_search_pipeline(
    category="projects",
    max_pages=10_000,
):
    if category not in SUPPORTED_SEARCH_PIPELINES: return # todo

    with logger.log_duration():
        _run_search_pipeline(
            category,
            max_pages,
        )

def _run_search_pipeline(
    category,
    max_pages,
):
    logger.info(f"Scraping {to_plural(category)}")
    client, _, _ = initialize_pipeline()

    cat_dir = Path(f"{DATA_DIR}/raw/{category}")
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

    logger.info(f"Appending new {to_plural(category)}")
    combined_file_path = combine_parquet_dir(cat_dir, cat_dir / f"{category}.parquet")
    logger.info(f"Deduplicating records")
    de_dup_parquet(combined_file_path)

    logger.info(f"Scrape complete for {category}.")
