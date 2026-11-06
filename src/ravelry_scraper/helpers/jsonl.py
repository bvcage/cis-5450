import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from tempfile import NamedTemporaryFile


def append_to_jsonl(records: list, target: Path):
    if not records: return

    target.parent.mkdir(parents=True, exist_ok=True)

    with open(target, "a", encoding="utf-8") as file:
        for record in records:
            json_string = json.dumps(record)
            file.write(json_string + "\n")
