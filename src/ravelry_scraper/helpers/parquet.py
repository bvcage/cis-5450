import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from pathlib import Path

from ..constants import DATETIME_COLS

def combine_parquet(
    source_dir: Path,
    destination_path: Path = None,
    delete_source: bool = True,
):
    if not source_dir.is_dir(): return # todo

    if destination_path is None:
        destination_path = source_dir.parent / f"{source_dir.name}.parquet"

    lazy_frames = [
        pl.scan_parquet(f) for f in sorted(source_dir.glob("*.parquet"))
    ]

    pl.concat(lazy_frames, how="diagonal_relaxed").sink_parquet(destination_path, compression='snappy')

    if delete_source:
        for f in source_dir.glob("*.parquet"):
            f.unlink()
            
    return destination_path

def de_dup_parquet(
    source_path: Path,
    destination_path: Path = None,
    de_dup_by: str = "id",
):
    if not source_path.is_file(): return # todo
    if not source_path.suffix == ".parquet": return # todo

    df = (
        pl.scan_parquet(source_path)
        .unique(subset=[de_dup_by], keep="last")
        .collect(engine="streaming")
    )

    if destination_path:
        df.write_parquet(destination_path, compression='snappy')
    else:
        temp_path = source_path.with_suffix(".tmp")
        df.write_parquet(temp_path, compression='snappy')
        temp_path.replace(source_path)
