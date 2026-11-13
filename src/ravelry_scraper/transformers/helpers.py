import uuid
from pathlib import Path

import polars as pl


# TODO: Fix handling of non-directory output paths
# Needs to handle cases where output_path is a file rather than a directory / combine rather than overwrite data
def store_extracted(df: pl.LazyFrame, output_path: Path, filetype: str = "parquet") -> None:
    if not output_path.is_dir(): return # TODO

    output_path.mkdir(parents=True, exist_ok=True)

    if filetype == "parquet":
        df.sink_parquet(output_path / f"{uuid.uuid4().hex}.parquet")
    elif filetype == "jsonl":
        df.sink_ndjson(output_path / f"{uuid.uuid4().hex}.jsonl")
    else:
        raise ValueError(f"Unsupported filetype: {filetype}")
