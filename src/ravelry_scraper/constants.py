from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[2]

DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"

SUPPORTED_PIPELINES = ["patterns", "designers", "yarns"]
SUPPORTED_SEARCH_PIPELINES = ["projects"]
