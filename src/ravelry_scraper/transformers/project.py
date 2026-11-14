from typing import ClassVar, override

import polars as pl

from ravelry_scraper.transformers.extractors import extract_nested_id, extract_users

from .base import BaseTransformer


class ProjectTransformer(BaseTransformer):
    must_keep_cols: ClassVar[list[str]] = ['rating']
    unused_cols: ClassVar[list[str]] = [
        'first_photo',
        'gauge_pattern',
        'links',
        'made_for',
        'name',
        'pattern_name',
        'permalink',
        'project_status_id',
        'size',
        'tag_names',
    ]
    id_cols: ClassVar[list[str]] = ['id', 'craft_id', 'pattern_id', 'user_id']
    boolean_cols: ClassVar[list[str]] = ['completed_day_set', 'started_day_set']
    datetime_cols: ClassVar[list[str]] = [
        'completed',
        'created_at',
        'project_status_changed',
        'scraped_at',
        'started',
        'updated_at',
    ]

    @override
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        extract_users(df)
        df = extract_nested_id(df, column="user")

        return super().transform(df)
