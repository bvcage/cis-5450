from pathlib import Path

import polars as pl

from ravelry_scraper.constants import DATA_DIR
from ravelry_scraper.transformers.helpers import store_extracted


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
