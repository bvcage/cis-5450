from datetime import datetime, UTC
from pathlib import Path
import json

from .client import RavelryClient
from .exceptions import RavelryScraperError
from .jsonl_helpers import append_to_jsonl, deduplicate_jsonl
from .logger import get_logger
from .state import StateManager

module_filename = Path(__file__).stem
logger = get_logger(module_filename)


def run_full_pipeline():
    run_patterns_pipeline()
    de_dup_errors()
    de_dup_raw_data()


def de_dup_errors():
    deduplicate_jsonl(
        source_path=Path("data/failed/patterns.jsonl"),
        de_dup_by="endpoint",
        keep_last_by="timestamp"
    )


def de_dup_raw_data():
    deduplicate_jsonl(
        source_path=Path("data/raw/patterns.jsonl"),
        de_dup_by="id",
        keep_last_by="scraped_at"
    )


def run_patterns_pipeline(max_count=10_000, batch_size=1_000):
    start_time = datetime.now(UTC)

    client = RavelryClient()
    state = StateManager()

    batch_size = min(max_count, batch_size)

    consecutive_error_count = 0
    total_count = 0

    while total_count < max_count and consecutive_error_count < 100:
        patterns = []
        errors = []

        last_id = state.get_cursor("patterns")
        num_ids = min(max_count - total_count, batch_size)

        try:
            for id in range(last_id + 1, last_id + num_ids + 1):
                endpoint = f"/patterns/{id}.json"

                try:
                    data = client.fetch_endpoint(endpoint)
                    pattern = data['pattern']

                    patterns.append(pattern | { 'scraped_at': datetime.now(UTC).isoformat() })
                    consecutive_error_count = 0

                except RavelryScraperError as err:
                    consecutive_error_count+= 1
                    errors.append(vars(err) | { 'timestamp': datetime.now(UTC).isoformat() })

                finally:
                    total_count += 1
                    last_id = id
        finally:
            logger.info("Saving batch results & updating checkpoint...")

            state.update_checkpoint("patterns", last_id)

            if patterns: append_to_jsonl(patterns, Path("data/raw/patterns.jsonl"))
            if errors: append_to_jsonl(errors, Path("data/failed/patterns.jsonl"))


    end_time = datetime.now(UTC)

    logger.info(f"Scrape complete for {total_count} patterns.")
    logger.info(f"Elapsed time: {str(end_time - start_time)}")