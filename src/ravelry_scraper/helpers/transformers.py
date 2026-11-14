import os
import unicodedata
from pathlib import Path

import polars as pl

from ravelry_scraper.constants import DATA_DIR
from ravelry_scraper.helpers.parquet import de_dup_parquet
from ravelry_scraper.helpers.strings import to_plural, to_singular

FRACTION_CHARS = "¼½¾⅐⅑⅒⅓⅔⅕⅖⅗⅘⅙⅚⅛⅜⅝⅞"
FRACTION_MAP = {ch: unicodedata.numeric(ch) for ch in FRACTION_CHARS}


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

# TODO: Review
def fraction_to_float_expr(expr: pl.Expr, alias_name: str | None = None) -> pl.Expr:
    s = expr.cast(pl.String)

    # Group 1: whole/decimal | Group 2: Unicode fraction | Groups 3 & 4: ASCII "num/den"
    pattern = rf"^\s*(\d+(?:\.\d+)?)?\s*(?:([{FRACTION_CHARS}])|(\d+)\s*/\s*(\d+))?\s*$"

    whole = s.str.extract(pattern, 1).cast(pl.Float64, strict=False)
    unicode_val = s.str.extract(pattern, 2).replace_strict(
        FRACTION_MAP, default=None, return_dtype=pl.Float64
    )
    num = s.str.extract(pattern, 3).cast(pl.Float64, strict=False)
    den = s.str.extract(pattern, 4).cast(pl.Float64, strict=False)

    slash_val = pl.when(den != 0).then(num / den).otherwise(None)
    frac_val = pl.coalesce([unicode_val, slash_val])

    # Only return a float if at least a whole number OR a fraction was matched
    result = (
        pl.when(whole.is_not_null() | frac_val.is_not_null())
        .then(whole.fill_null(0.0) + frac_val.fill_null(0.0))
        .otherwise(None)
    )

    return result.alias(alias_name) if alias_name else result

def store_extracted(df: pl.LazyFrame, output_path: Path, filetype: str = "parquet") -> None:
    output_path = output_path.with_suffix(f".{filetype}")

    if filetype == "parquet":
        if output_path.exists():
            existing_df = pl.scan_parquet(output_path)
            combined_df = pl.concat([existing_df, df], how="diagonal_relaxed")
    
            temp_path = output_path.with_suffix(".tmp.parquet")
            combined_df.sink_parquet(temp_path, compression='snappy')
            de_dup_parquet(temp_path)
            
            os.replace(temp_path, output_path)
        else:
            df.sink_parquet(output_path, compression='snappy')
    # elif filetype == "jsonl":
    #     df.sink_ndjson(output_path)
    else:
        raise ValueError(f"Unsupported filetype: {filetype}")
