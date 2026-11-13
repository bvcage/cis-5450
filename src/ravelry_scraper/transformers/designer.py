from typing import ClassVar, override

import polars as pl
import polars.selectors as cs

from .base import BaseTransformer, extract_users


class DesignerTransformer(BaseTransformer):
    must_keep_cols: ClassVar[list[str]] = []
    unused_cols: ClassVar[list[str]] = [
        "notes",
        "notes_html",
        "permalink",
    ]

    @override
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        df = super().transform(df)

        extract_users(df)
        df = df.with_columns(
            pl.col("users")
            .list.eval(pl.element().struct.field("id"))
            .alias("user_ids")
            .cast(pl.List(pl.Int64))
        ).drop("users")

        df_columns = df.collect_schema().names()

        id_cols = ["id"]
        count_cols = [col for col in df_columns if col.endswith('count')]
        datetime_cols = ["scraped_at"]

        df = df.with_columns(
            cs.by_name(*id_cols, require_all=False).cast(pl.Int64, strict=False),
            cs.by_name(*count_cols, require_all=False).cast(pl.Int64, strict=False),
            cs.by_name(*datetime_cols, require_all=False).str.to_datetime(time_zone="UTC", strict=False)
        )

        return df
