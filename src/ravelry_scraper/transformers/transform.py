# from ravelry_scraper.transformers.pattern import PatternTransformer
# from ravelry_scraper.transformers.project import ProjectTransformer
from pathlib import Path

from ravelry_scraper.transformers.designer import DesignerTransformer
from ravelry_scraper.transformers.yarn import YarnTransformer


def main():
    transformers = [
        # PatternTransformer("data/raw/patterns.parquet", "data/clean/patterns.parquet"),
        # ProjectTransformer("data/raw/projects.parquet", "data/clean/projects.parquet"),
        DesignerTransformer(Path("data/raw/designers.parquet"), Path("data/clean/designers.parquet")),
        YarnTransformer(Path("data/raw/yarns.parquet"), Path("data/clean/yarns.parquet")),
    ]

    for transformer in transformers:
        transformer.process()

if __name__ == "__main__":
    main()
