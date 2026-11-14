import os
from pathlib import Path
from typing import ClassVar

import polars as pl
import polars.selectors as cs

from ravelry_scraper.helpers.strings import to_singular
from ravelry_scraper.logger import get_logger
from ravelry_scraper.transformers.helpers import extract_linked_data

curr_file = Path(__file__).resolve()
MODULE = f"{curr_file.parent.name}/{curr_file.stem}"
logger = get_logger(MODULE)

class BaseTransformer:
    # required
    source_path: Path
    output_path: Path

    # subclass overrides
    de_dup_by: str | list[str] | None = "id"
    na_max_pct: float = 0.40
    must_keep_cols: ClassVar[list[str]] = []
    unused_cols: ClassVar[list[str]] = []

    # type cast columns
    id_cols: ClassVar[list[str]] = ["id"]
    boolean_cols: ClassVar[list[str]] = []
    datetime_cols: ClassVar[list[str]] = ["scraped_at"]
    extract_cols: ClassVar[list[str]] = []
    int_cols: ClassVar[list[str]] = []

    def __init__(self, source_path: Path | str, output_path: Path | str):
        source_path = Path(source_path).resolve()
        output_path = Path(output_path).resolve()

        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.source_path = source_path
        self.output_path = output_path

    def de_dup(self, df: pl.LazyFrame) -> pl.LazyFrame:
        if not self.de_dup_by:
            return df

        subset = [self.de_dup_by] if isinstance(self.de_dup_by, str) else self.de_dup_by

        # Read only key column(s) into memory to find surviving indices
        keep_indices = (
            pl.scan_parquet(self.source_path)
            .select(subset)
            .with_row_index("row_nr")
            .unique(subset=subset, keep="last")
            .select("row_nr")
            .collect()
        )

        # Filter LazyFrame via semi-join
        return (
            df.with_row_index("row_nr")
            .join(keep_indices.lazy(), on="row_nr", how="semi")
            .drop("row_nr")
        )

    def drop_unwanted_columns(self, df: pl.LazyFrame) -> pl.LazyFrame:
        # columns where na values exceed threshold
        null_pct = df.select(pl.all().null_count() / pl.len()).collect()
        na_cols = [
            col_name for col_name in null_pct.columns
            if null_pct[col_name][0] > self.na_max_pct
        ]

        # drop na & unused cols, do not drop must_keep_cols
        df_columns = df.collect_schema().names()
        drop_cols = set(na_cols + self.unused_cols)
        drop_cols = [col for col in drop_cols if col in df_columns and col not in self.must_keep_cols]

        logger.info(f"Dropping columns from dataset: {', '.join(drop_cols)}")
        return df.drop(drop_cols)

    def process(self) -> None:
        with logger.log_duration():
            df = pl.scan_parquet(self.source_path)
            df = self.de_dup(df)
            df = self.drop_unwanted_columns(df)
            df = self.transform(df)
            self.save(df)

    def save(self, df: pl.LazyFrame) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        if self.output_path.exists():
            existing_df = pl.scan_parquet(self.output_path)
            combined_df = pl.concat([existing_df, df], how="diagonal_relaxed")
            combined_df = self.de_dup(combined_df)

            temp_path = self.output_path.with_suffix(".tmp.parquet")
            combined_df.sink_parquet(temp_path, compression='snappy')
            os.replace(temp_path, self.output_path)
        else:
            df.sink_parquet(self.output_path, compression='snappy')

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        # extract linked data
        for column in self.extract_cols:
            extract_linked_data(df, column)

            col_dtype = df.collect_schema()[column]
            if isinstance(col_dtype, (pl.List, pl.Array)):
                id_expr = (
                    pl.col(column)
                    .list.eval(pl.element().struct.field("id"))
                    .cast(pl.List(pl.Int64), strict=False)
                    .alias(f"{to_singular(column)}_ids")
                )
            else:
                id_expr = (
                    pl.col(column)
                    .struct.field("id")
                    .cast(pl.Int64, strict=False)
                    .alias(f"{to_singular(column)}_id")
                )

            df = df.with_columns(id_expr).drop(column)

        # type cast columns
        count_cols = [col for col in df.collect_schema().names() if col.endswith("count")]
        return df.with_columns(
            cs.by_name(*self.id_cols, require_all=False).cast(pl.Int64, strict=False),
            cs.by_name(count_cols, require_all=False).cast(pl.Int64, strict=False),
            cs.by_name(*self.boolean_cols, require_all=False).cast(pl.Boolean, strict=False),
            cs.by_name(*self.datetime_cols, require_all=False).str.to_datetime(time_zone="UTC", strict=False),
            cs.by_name(*self.int_cols, require_all=False).cast(pl.Int64, strict=False),  
        )
