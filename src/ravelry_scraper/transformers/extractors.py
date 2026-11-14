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
        extract_nested_data(df, category=column, root_col=column, nested_col='parent')
        df = df.with_columns(
            pl.col("parent")
            .struct.field("id")
            .alias("parent_id")
            .cast(pl.Int64)
        ).drop("parent")

    store_extracted(df, Path(f"{DATA_DIR}/extracted/{to_plural(column)}"))

# TODO: Review
def extract_nested_data(
    df: pl.LazyFrame,
    category: str,
    root_col: str,
    nested_col: str,
) -> None:
    """
    Flattens a self-referential Struct column (e.g., root_col -> nested_col -> nested_col)
    into a single deduplicated LazyFrame with `{nested_col}_id` populated at each level.
    """
    schema = df.collect_schema()
    if root_col not in schema:
        return

    current_expr = pl.col(root_col)
    current_dtype = schema[root_col]

    # Explode if the root column is a List[Struct] (e.g., pattern_categories)
    if isinstance(current_dtype, (pl.List, pl.Array)):
        df = df.explode(root_col)
        current_dtype = current_dtype.inner

    levels: list[pl.LazyFrame] = []

    # Walk down each nested struct level until `nested_col` no longer exists in the schema
    while isinstance(current_dtype, pl.Struct):
        fields = {f.name: f.dtype for f in current_dtype.fields}
        has_child = nested_col in fields and isinstance(fields[nested_col], pl.Struct)

        # Unnest the current struct level into top-level columns
        level_df = df.select(current_expr.alias("_node")).drop_nulls().unnest("_node")

        if has_child:
            child_fields = {f.name for f in fields[nested_col].fields}
            if "id" in child_fields:
                level_df = level_df.with_columns(
                    pl.col(nested_col).struct.field("id").alias(f"{nested_col}_id")
                )
            else:
                level_df = level_df.with_columns(
                    pl.lit(None, dtype=pl.Int64).alias(f"{nested_col}_id")
                )
            # Drop the nested child struct from this flattened record (matches .pop(col, None))
            level_df = level_df.drop(nested_col)
        else:
            level_df = level_df.with_columns(
                pl.lit(None, dtype=pl.Int64).alias(f"{nested_col}_id")
            )

        levels.append(level_df)

        # Advance expression and dtype to the next deeper level
        if has_child:
            current_expr = current_expr.struct.field(nested_col)
            current_dtype = fields[nested_col]
        else:
            break

    if not levels:
        return

    result = pl.concat(levels, how="diagonal_relaxed").unique()

    store_extracted(result, Path(f"{DATA_DIR}/extracted/{to_plural(category)}"))

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
