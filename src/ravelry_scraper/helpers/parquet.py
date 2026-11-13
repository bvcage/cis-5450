import polars as pl

from datetime import datetime, UTC
from pathlib import Path

def combine_parquet_files(
    files: list[Path],
    destination_path: Path = None,
    delete_originals: bool = True,
):
    for file in files:
        if not file.is_file(): return # todo
        if not file.suffix == ".parquet": return # todo
    
    if destination_path is None:
        destination_path = files[0].parent / f"combined_{datetime.now(UTC).timestamp()}.parquet"

    tmp_dest_path = destination_path.with_suffix('.tmp.parquet')

    lazy_frames = [
        pl.scan_parquet(f) for f in sorted(files)
    ]

    pl.concat(lazy_frames, how="diagonal_relaxed").sink_parquet(tmp_dest_path, compression='snappy')

    if delete_originals:
        for f in files:
            f.unlink()

    tmp_dest_path.replace(destination_path)
    return destination_path

def combine_parquet_dir(
    source_dir: Path,
    destination_path: Path = None,
    delete_source: bool = True,
):
    if not source_dir.is_dir(): return # todo

    if destination_path is None:
        destination_path = source_dir.parent / f"{source_dir.name}.parquet"

    tmp_dest_path = destination_path.with_suffix('.tmp.parquet')

    files = list(source_dir.glob("*.parquet"))

    lazy_frames = [
        pl.scan_parquet(f) for f in sorted(files)
    ]

    pl.concat(lazy_frames, how="diagonal_relaxed").sink_parquet(tmp_dest_path, compression='snappy')

    if delete_source:
        for f in files:
            f.unlink()

    tmp_dest_path.replace(destination_path)
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
