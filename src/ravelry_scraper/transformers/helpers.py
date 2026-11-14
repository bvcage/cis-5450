import os
import unicodedata
from pathlib import Path

import polars as pl

from ravelry_scraper.helpers.parquet import de_dup_parquet

FRACTION_CHARS = "¼½¾⅐⅑⅒⅓⅔⅕⅖⅗⅘⅙⅚⅛⅜⅝⅞"
FRACTION_MAP = {ch: unicodedata.numeric(ch) for ch in FRACTION_CHARS}

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
