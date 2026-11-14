from pathlib import Path

import polars as pl

from ravelry_scraper.constants import DATA_DIR
from ravelry_scraper.helpers.strings import to_plural, to_singular
from ravelry_scraper.transformers.helpers import store_extracted

from .helpers import fraction_to_float_expr


def extract_count(
    df: pl.LazyFrame,
    column: str, 
    alias: str | None = None,
    drop: bool = True,
) -> pl.LazyFrame:
    df = df.with_columns(
        pl.col(column)
        .list.len()
        .fill_null(0)
        .alias(alias or f"{column}_count")
    )
    return df.drop(column) if drop else df

def extract_linked_data(df: pl.LazyFrame, column: str) -> None:
    col_expr = pl.col(column)

    if isinstance(df.collect_schema()[column], (pl.List, pl.Array)):
        col_expr = col_expr.explode()

    df = df.select(col_expr)
    df = df.drop_nulls()
    df = df.unnest(column)
    df = df.unique()

    if column in ["pattern_needle_sizes", "min_needle_size", "max_needle_size", "min_hook_size", "max_hook_size"]:
        exprs = [fraction_to_float_expr(pl.col(size), size) for size in ["us", "metric"]]
        df = df.with_columns(exprs)
        column = "_".join(column.split('_')[-2:])

    if column == 'pattern_categories':
        extract_nested_data(df, root_col=column, nested_col='parent')
        df = extract_nested_id(df, column="parent", alias="parent_id")

    store_extracted(df, Path(f"{DATA_DIR}/extracted/{to_plural(column)}"))

def extract_nested_data(
    df: pl.LazyFrame,
    root_col: str,
    nested_col: str,
) -> None:
    """
    Flattens a self-referential Struct column (e.g., root_col -> nested_col -> nested_col)
    into a single deduplicated LazyFrame with `{nested_col}_id` populated at each level.
    """
    extracted: list[pl.LazyFrame] = []
    columns = df.collect_schema().names()

    while nested_col in columns:
        extracted.append(
            df.with_columns(
                pl.col(nested_col).struct.field("id").alias(f"{nested_col}_id")
            ).drop(nested_col)
        )

        col_expr = pl.col(nested_col)
        if isinstance(df.collect_schema()[nested_col], (pl.List, pl.Array)):
            col_expr = col_expr.explode()

        df = (
            df.select(col_expr) 
            .drop_nulls()
            .unnest(nested_col)
            .unique()
        )
        columns = df.collect_schema().names()

    result = pl.concat(extracted, how="diagonal_relaxed").unique()
    store_extracted(result, Path(f"{DATA_DIR}/extracted/{to_plural(root_col)}"))

def extract_nested_id(
    df: pl.LazyFrame,
    column: str,
    field: str = "id",
    alias: str | None = None,
    drop: bool = True,
) -> pl.LazyFrame:
    schema = df.collect_schema()
    if column not in schema:
        return df

    col_dtype = schema[column]
    singular = to_singular(column)

    if isinstance(col_dtype, (pl.List, pl.Array)):
        target_name = alias or f"{singular}_ids"
        expr = (
            pl.col(column)
            .list.eval(pl.element().struct.field(field))
            .cast(pl.List(pl.Int64), strict=False)
            .alias(target_name)
        )
    elif isinstance(col_dtype, pl.Struct):
        target_name = alias or f"{singular}_id"
        expr = (
            pl.col(column)
            .struct.field(field)
            .cast(pl.Int64, strict=False)
            .alias(target_name)
        )
    else:
        return df

    df = df.with_columns(expr)
    return df.drop(column) if drop else df

def extract_users(df: pl.LazyFrame, column: str = "users", output_path: Path = Path(f"{DATA_DIR}/extracted/users.parquet")) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    col_expr = pl.col(column)

    if isinstance(df.collect_schema()[column], (pl.List, pl.Array)):
        col_expr = col_expr.explode()

    result = (
        df.select(col_expr)
        .drop_nulls()
        .unnest(column)
        .select(["id", "username", "profile_country_code"])
        .unique()
    )

    store_extracted(result, output_path)
