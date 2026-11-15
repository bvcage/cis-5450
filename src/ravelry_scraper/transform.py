from ravelry_scraper.constants import DATA_DIR
from ravelry_scraper.transformers.designer import DesignerTransformer
from ravelry_scraper.transformers.pattern import PatternTransformer
from ravelry_scraper.transformers.project import ProjectTransformer
from ravelry_scraper.transformers.yarn import YarnTransformer


def main():
    transformers = [
        DesignerTransformer(f"{DATA_DIR}/raw/designers.parquet", f"{DATA_DIR}/clean/designers.parquet"),
        PatternTransformer(f"{DATA_DIR}/raw/patterns.parquet", f"{DATA_DIR}/clean/patterns.parquet"),
        ProjectTransformer(f"{DATA_DIR}/raw/projects.parquet", f"{DATA_DIR}/clean/projects.parquet"),
        YarnTransformer(f"{DATA_DIR}/raw/yarns.parquet", f"{DATA_DIR}/clean/yarns.parquet"),
    ]

    for transformer in transformers:
        transformer.process()

if __name__ == "__main__":
    main()
