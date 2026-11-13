from typing import ClassVar, override

import polars as pl

from .base import BaseTransformer


class YarnTransformer(BaseTransformer):
    must_keep_cols: ClassVar[list[str]] = [
        'max_gauge',
        'max_needle_size',
        'min_gauge',
        'min_needle_size',
        'rating_average',
        'rating_total',
    ]
    unused_cols: ClassVar[list[str]] = [
        'gauge_divisor',
        'grams',
        'name',
        'notes_html',
        'permalink',
        'texture',
        'yarn_attributes',
        'yarn_provenance',
    ]
    id_cols: ClassVar[list[str]] = ["id"]
    boolean_cols: ClassVar[list[str]] = ['discontinued', 'machine_washable']
    datetime_cols: ClassVar[list[str]] = ["scraped_at"]
    extract_cols: ClassVar[list[str]] = ['min_needle_size', 'max_needle_size', 'yarn_company', 'yarn_fibers', 'yarn_weight']
    int_cols: ClassVar[list[str]] = ['rating_total', 'yardage']

    @override
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        # convert photos list to photos_count
        df = df.with_columns(
            pl.col("photos")
            .list.len()
            .fill_null(0)
            .alias("photos_count")
        ).drop("photos")

        # if no range, set max equal to min
        df = df.with_columns(
            pl.col("max_gauge").fill_null(pl.col("min_gauge")),
            pl.col("max_needle_size").fill_null(pl.col("min_needle_size"))
        )

        return super().transform(df)
