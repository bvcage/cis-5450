from typing import ClassVar, override

import polars as pl

from .base import BaseTransformer
from .extractors import extract_nested_id, extract_users


class DesignerTransformer(BaseTransformer):
    must_keep_cols: ClassVar[list[str]] = []
    unused_cols: ClassVar[list[str]] = [
        "notes",
        "notes_html",
        "permalink",
    ]
    id_cols: ClassVar[list[str]] = ["id"]
    datetime_cols: ClassVar[list[str]] = ["scraped_at"]

    @override
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        extract_users(df)
        df = extract_nested_id(df, column="users")

        return super().transform(df)
