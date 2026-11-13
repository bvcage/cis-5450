from pathlib import Path

import polars as pl

from ravelry_scraper.constants import DATA_DIR
from ravelry_scraper.helpers.strings import to_plural
from ravelry_scraper.transformers.helpers import store_extracted

from .helpers import fraction_to_float_expr


def extract_linked_data(df: pl.LazyFrame, column: str) -> None:
    col_expr = pl.col(column)

    if isinstance(df.collect_schema()[column], (pl.List, pl.Array)):
        col_expr = col_expr.explode()

    df = df.select(column)
    df = df.drop_nulls()
    df = df.unnest(column)
    df = df.unique()

    if column in ["pattern_needle_sizes", "min_needle_size", "max_needle_size"]:
        field_exprs = [fraction_to_float_expr(pl.field(c), c) for c in ["us", "metric"]]
        updated_col = pl.col(column).struct.with_fields(field_exprs)
        df = df.with_columns(updated_col.alias(column))
        column = "needle_sizes"

    if column == 'pattern_categories':
        extract_nested_data(df, category=column, root_col=column, nested_col='parent')
        df = df.with_columns(
            pl.col("parent")
            .struct.field("id")
            .alias("parent_id")
            .cast(pl.List(pl.Int64))
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

def extract_users(df: pl.LazyFrame, output_path: Path = Path(f"{DATA_DIR}/extracted/users")) -> None:
    output_path.mkdir(parents=True, exist_ok=True)

    col_expr = pl.col("users")

    if isinstance(df.collect_schema()["users"], (pl.List, pl.Array)):
        col_expr = col_expr.explode()

    result = (
        df.select(col_expr)
        .drop_nulls()
        .unnest("users")
        .select(["id", "username", "profile_country_code"])
        .unique()
    )

    store_extracted(result, output_path)
