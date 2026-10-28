from datetime import datetime, UTC
from pathlib import Path
import json

from .client import RavelryClient
from .exceptions import RavelryScraperError
from .logger import get_logger
from .state import StateManager

module_filename = Path(__file__).stem
logger = get_logger(module_filename)


def append_to_jsonl(data_records: list, target_path: str, target_filename: str):
    if not data_records: return

    output_file = Path(f"data/{target_path}/{target_filename}.jsonl")
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "a", encoding="utf-8") as file:
        for record in data_records:
            json_string = json.dumps(record)
            file.write(json_string + "\n")


def run_full_pipeline():
    # do stuff
    print('something')


def run_patterns_pipeline(max_count=10_000, batch_size=1_000):
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
            state.update_checkpoint("patterns", last_id)

            if patterns: append_to_jsonl(patterns, "raw", "patterns")
            if errors: append_to_jsonl(errors, "failed", "patterns")

            logger.info(f"Scrape complete for {total_count} patterns.")