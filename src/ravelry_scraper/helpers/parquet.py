import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from ..constants import DATETIME_COLS

def de_dup_parquet(
    source_path: Path,
    destination_path: Path = None,
    de_dup_by: str = "id",
):
    if not source_path.is_file(): return # todo
    if not source_path.suffix == ".parquet": return # todo

    overrides = dict()
    for batch in pq.ParquetFile(source_path).iter_batches(batch_size=1_000):
        chunk = batch.to_pandas()
        overrides = {col: pl.Datetime for col in DATETIME_COLS if col in chunk.columns}
        break

    df = (
        pl.scan_parquet(source_path, schema_overrides=overrides)
        .unique(subset=[de_dup_by], keep="last")
        .collect(engine="streaming")
    )

    if destination_path:
        df.write_parquet(destination_path)
    else:
        temp_path = source_path.with_suffix(".tmp")
        df.write_parquet(temp_path)
        temp_path.replace(source_path)
