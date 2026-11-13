import unicodedata
import uuid
from pathlib import Path

import polars as pl

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


# TODO: Fix handling of non-directory output paths
# Needs to handle cases where output_path is a file rather than a directory / combine rather than overwrite data
def store_extracted(df: pl.LazyFrame, output_path: Path, filetype: str = "parquet") -> None:
    if not output_path.is_dir(): return # TODO

    output_path.mkdir(parents=True, exist_ok=True)

    if filetype == "parquet":
        df.sink_parquet(output_path / f"{uuid.uuid4().hex}.parquet")
    elif filetype == "jsonl":
        df.sink_ndjson(output_path / f"{uuid.uuid4().hex}.jsonl")
    else:
        raise ValueError(f"Unsupported filetype: {filetype}")
