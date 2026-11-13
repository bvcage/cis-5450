from datetime import UTC, datetime
from pathlib import Path

import polars as pl


def combine_parquet_files(
    files: list[Path],
    destination_path: Path | None = None,
    delete_originals: bool = True,
):
    for file in files:
        if not file.is_file(): return # TODO
        if file.suffix != ".parquet": return # TODO
    
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
    destination_path: Path | None = None,
    delete_source: bool = True,
):
    if not source_dir.is_dir(): return # TODO

    if destination_path is None:
        destination_path = source_dir.parent / f"{source_dir.name}.parquet"

    tmp_dest_path = destination_path.with_suffix('.tmp.parquet')

    files = list(source_dir.glob("*.parquet"))
    if destination_path.is_file() and destination_path.suffix == ".parquet":
        files.append(destination_path)

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
    destination_path: Path | None = None,
    de_dup_by: str = "id",
):
    if not source_path.is_file(): return # TODO
    if source_path.suffix != ".parquet": return # TODO

    temp_path = source_path.with_suffix(".tmp.parquet")
    output_path = destination_path if destination_path else temp_path

    keep_indices = (
        pl.scan_parquet(source_path)
        .select(de_dup_by)
        .with_row_index("row_nr")
        .unique(subset=[de_dup_by], keep="last")
        .select("row_nr")
        .collect()
    )

    (
        pl.scan_parquet(source_path)
        .with_row_index("row_nr")
        .join(keep_indices.lazy(), on="row_nr", how="semi")
        .drop("row_nr")
        .sink_parquet(output_path, compression="snappy")
    )

    if not destination_path:
        temp_path.replace(source_path)
