from typing import ClassVar, override

import polars as pl
from currency_converter.currency_converter import CurrencyConverter

from ravelry_scraper.helpers.transformers import extract_count, extract_nested_id

from .base import BaseTransformer


# Docs: https://www.ravelry.com/api#Pattern_full_result
class PatternTransformer(BaseTransformer):
    must_keep_cols: ClassVar[list[str]] = [
        'currency',
        'difficulty_average',
        'difficulty_count',
        'price',
        'rating_average',
        'rating_count',
        'yardage',
        'yardage_max',
    ]
    unused_cols: ClassVar[list[str]] = [
        'currency_symbol',
        'download_location',
        'generally_available',
        'has_uk_terminology',
        'has_us_terminology',
        'notes',
        'notes_html',
        'pdf_in_library',
        'pdf_url',
        'permalink',
        'personal_attributes',
        'printings',
        'product_id',
        'sizes_available',
        'unlisted_product_ids',
        'url',
        'volumes_in_library',
        'yardage_description',
        'yarn_list_type',
    ]
    id_cols: ClassVar[list[str]] = ['id']
    boolean_cols: ClassVar[list[str]] = ['downloadable', 'free', 'ravelry_download']
    datetime_cols: ClassVar[list[str]] = ['created_at', 'published', 'scraped_at', 'updated_at']
    extract_cols: ClassVar[list[str]] = [
        'craft',
        'languages',
        'pattern_attributes',
        'pattern_categories',
        'pattern_needle_sizes',
        'pattern_type',
        'yarn_weight',
    ]
    int_cols: ClassVar[list[str]] = ['yardage', 'yardage_max']

    @override
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        # drop gauge columns
        gauge_cols = [col for col in df.collect_schema().names() if 'gauge' in col]
        df = df.drop(gauge_cols)

        # convert photos list to photos_count
        df = extract_count(df, column="photos")

        # if no range, set max equal to min
        df = df.with_columns(
            pl.col("yardage_max").fill_null(pl.col("yardage")),
        )

        # convert currencies
        df = convert_currencies(df)

        # extract yarn id from packs (special case)
        df = extract_nested_id(df, column="packs", field="yarn_id", alias="yarn_ids")

        # extract designer id from pattern_author
        df = extract_nested_id(df, column="pattern_author", field="id", alias="designer_id")

        return super().transform(df)

def convert_currencies(df: pl.LazyFrame) -> pl.LazyFrame:
    c = CurrencyConverter()
    
    # Normalize empty/whitespace currency strings to null
    df = df.with_columns(
        pl.when(pl.col("currency").str.strip_chars() == "")
        .then(None)
        .otherwise(pl.col("currency").str.strip_chars())
        .alias("currency")
    )

    # Get unique currencies list
    curr_list = (
        df.select(
            pl.col("currency").drop_nulls().unique()
        ).collect()
        .get_column("currency")
        .to_list()
    )

    # Build rate_map
    def get_usd_rate(curr: str) -> float | None:
        try:
            return c.convert(1, curr, "USD")
        except ValueError:
            return None
    rate_map: dict[str, float] = {
        curr: rate
        for curr in curr_list
        if (rate := get_usd_rate(curr)) is not None
    }

    # Map rates & convert to USD
    df = df.with_columns(
        (
            pl.col("price").cast(pl.Float64, strict=False)
            * pl.col("currency").replace_strict(rate_map, default=None, return_dtype=pl.Float64)
        )
        .round(2)
        .alias("price_usd")
    )

    return df