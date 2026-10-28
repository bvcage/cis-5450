import json
import pandas as pd
from pathlib import Path


def append_to_jsonl(records: list, target: Path):
    if not records: return

    target.parent.mkdir(parents=True, exist_ok=True)

    with open(target, "a", encoding="utf-8") as file:
        for record in records:
            json_string = json.dumps(record)
            file.write(json_string + "\n")


def deduplicate_jsonl(
    source_path: Path,
    destination_path: Path = None,
    de_dup_by: str = "id",
    keep_last_by: str = "timestamp"
):
    if not source_path.is_file(): return # TODO

    df = pd.read_json(source_path, lines=True)

    df = df.sort_values(keep_last_by).drop_duplicates(subset=[de_dup_by], keep='last')

    if destination_path:
        df.to_json(destination_path, orient="records", lines=True)
    else:
        temp_path = source_path.with_suffix(".tmp")
        df.to_json(temp_path, orient="records", lines=True)
        temp_path.replace(source_path)
