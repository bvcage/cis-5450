import json
import pandas as pd
import polars as pl
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


def de_dup_jsonl(
    source_path: Path,
    destination_path: Path = None,
    de_dup_by: str = "id",
):
    if not source_path.is_file(): return # todo

    # for chunk in pd.read_json(source_path, lines=True, chunksize=1000):


    df = (
        pl.scan_ndjson(source_path)
        .unique(subset=[de_dup_by], keep="last")
        .collect(engine="streaming")
    )

    if destination_path:
        df.write_ndjson(destination_path)
    else:
        temp_path = source_path.with_suffix(".tmp")
        df.write_ndjson(temp_path)
        temp_path.replace(source_path)

