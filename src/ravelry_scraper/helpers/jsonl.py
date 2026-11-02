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


def convert_to_parquet(source_file: Path, destination: Path = None):
    if not source_file.is_file():
        raise FileNotFoundError(source_file)

    if destination == None:
        destination_file = source_file.with_suffix('.parquet')
    elif destination.is_dir():
        file_name = source_file.with_suffix('.parquet').name
        destination_file = destination / file_name
    else:
        destination_file = destination

    if destination_file.suffix != ".parquet":
        raise ValueError("Destination must have a .parquet suffix")
    if source_file.resolve() == destination_file.resolve():
        raise ValueError("Source and destination must be different files")

    schema = None
    with pd.read_json(source_file, lines=True, chunksize=10_000, dtype=False) as reader:
        for chunk in reader:
            batch_schema = pa.Table.from_pandas(chunk, preserve_index=False).schema
            batch_schema = batch_schema.remove_metadata()
            schema = batch_schema if schema is None else pa.unify_schemas(
                [schema, batch_schema], promote_options="permissive"
            )

    if schema is None or len(schema) == 0:
        raise ValueError(f"No records to convert in {source_file}")

    with NamedTemporaryFile(
        dir=destination_file.parent, suffix=".parquet", delete=False
    ) as temp_file:
        temp_path = Path(temp_file.name)

    try:
        with pq.ParquetWriter(temp_path, schema) as writer:
            with pd.read_json(
                source_file, lines=True, chunksize=10_000, dtype=False
            ) as reader:
                for chunk in reader:
                    chunk = chunk.reindex(columns=schema.names)
                    table = pa.Table.from_pandas(
                        chunk, schema=schema, preserve_index=False
                    )
                    writer.write_table(table)
        temp_path.replace(destination_file)
    finally:
        temp_path.unlink(missing_ok=True)
