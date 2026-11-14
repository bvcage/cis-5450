from ravelry_scraper.transformers.designer import DesignerTransformer
from ravelry_scraper.transformers.pattern import PatternTransformer
from ravelry_scraper.transformers.project import ProjectTransformer
from ravelry_scraper.transformers.yarn import YarnTransformer


def main():
    transformers = [
        DesignerTransformer("data/raw/designers.parquet", "data/clean/designers.parquet"),
        PatternTransformer("data/raw/patterns.parquet", "data/clean/patterns.parquet"),
        ProjectTransformer("data/raw/projects.parquet", "data/clean/projects.parquet"),
        YarnTransformer("data/raw/yarns.parquet", "data/clean/yarns.parquet"),
    ]

    for transformer in transformers:
        transformer.process()

if __name__ == "__main__":
    main()
