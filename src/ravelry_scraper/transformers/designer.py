from typing import ClassVar, override

import polars as pl

from ravelry_scraper.helpers.transformers import extract_nested_id, extract_users
from ravelry_scraper.transformers.base import BaseTransformer


class DesignerTransformer(BaseTransformer):
    entity: str = "designer"
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
