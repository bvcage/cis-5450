from datetime import datetime, UTC
from pathlib import Path
import json

from .client import RavelryClient
from .exceptions import RavelryScraperError, RequestError
from .helpers.jsonl import append_to_jsonl, deduplicate_jsonl
from .logger import get_logger
from .state import StateManager

MODULE = Path(__file__).stem


def initialize_pipeline():
    client = RavelryClient()
    state = StateManager()
    logger = get_logger(MODULE)

    return (
        client,
        state,
        logger,
    )


def run_full_pipeline():
    run_patterns_pipeline()
    de_dup_errors()
    de_dup_raw_data()


def de_dup_errors(category="patterns"):
    deduplicate_jsonl(
        source_path=Path(f"data/failed/{category}.jsonl"),
        de_dup_by="endpoint",
        keep_last_by="timestamp"
    )


def de_dup_raw_data(category="patterns"):
    deduplicate_jsonl(
        source_path=Path(f"data/raw/{category}.jsonl"),
        de_dup_by="id",
        keep_last_by="scraped_at"
    )


def run_pipeline(category="patterns", max_count=10_000, batch_size=1_000):
    start_time = datetime.now(UTC)

    client, state, logger = initialize_pipeline()

    batch_size = min(max_count, batch_size)

    consecutive_error_count = 0
    total_count = 0

    while total_count < max_count and consecutive_error_count < 100:
        records = []
        errors = []

        last_id = state.get_cursor(category)
        last_successful_id = state.get_cursor(category, "max_successful_id")
        num_ids = min(max_count - total_count, batch_size)

        try:
            for id in range(last_id + 1, last_id + num_ids + 1):
                endpoint = f"/{category}/{id}.json"

                try:
                    data = client.fetch_endpoint(endpoint)
                    first_key = next(iter(data))
                    record = data[first_key]
                    records.append(record | { 'scraped_at': datetime.now(UTC).isoformat() })

                    consecutive_error_count = 0
                    last_successful_id = id

                except RavelryScraperError as err:
                    consecutive_error_count+= 1
                    errors.append(vars(err) | { 'timestamp': datetime.now(UTC).isoformat() })

                    if isinstance(err, RequestError):
                        logger.error(f"Stopping scrape after request failure on endpoint: {endpoint}\nLeaving checkpoint at {last_id} for retry.")
                        return

                total_count += 1
                last_id = id
        finally:
            logger.info("Saving batch results & updating checkpoint...")

            if records: append_to_jsonl(records, Path(f"data/raw/{category}.jsonl"))
            if errors: append_to_jsonl(errors, Path(f"data/failed/{category}.jsonl"))

            state.update_checkpoint(category, {
                "max_successful_id": last_successful_id,
                "max_id": last_id,
            })


    end_time = datetime.now(UTC)

    logger.info(f"Scrape complete for {total_count} patterns.")
    logger.log_duration(start_time, end_time)
