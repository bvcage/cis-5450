from pathlib import Path
from typing import Any, Final, overload

PROJECT_ROOT: Final[Path] = Path(__file__).parents[2]

DATA_DIR: Final[Path] = PROJECT_ROOT / "data"
LOGS_DIR: Final[Path] = PROJECT_ROOT / "logs"

SUPPORTED_PIPELINES: Final[list[str]] = ["patterns", "designers", "yarns"]
SUPPORTED_SEARCH_PIPELINES: Final[list[str]] = ["projects"]

SEARCH_SORT_OPTIONS: Final[list[str]] = [
    "best",
    "completed",
    "favorites",
    "happiness",
    "helpful",
    "started",
    "updated",
]

POPULAR_PROJECT_PATTERNS: Final[list[str]] = [
    'Sophie Scarf',
    'Oslo Hat',
    'Ranunculus',
    'Step by Step Sweater',
    'Musselburgh',
    'Lakes Pullover',
    'Monday Sweater',
    'Sophie Shawl',
    'Color Affection',
    'Blouse No. 1',
    'Cumulus Blouse',
    'Featherweight Cardigan',
    'Weekend Hat',
    "I'm So Basic Socks",
    'Winters Pullover',
    'Vanilla Socks on 9" Circulars',
    'Storm Sweater',
    'Classic Ribbed Hat',
    'Clapotis',
    "The World's Simplest Mittens",
    'Penny Gloves',
    'No Frills Sweater',
    'Shalom Cardigan',
    'Baa-ble Hat',
    'Nightshift',
    'Porcelain Sweater',
    'Zakkuri Cardigan',
    'Novice Slipover',
    'Hitchhiker',
    'Baby Bear Bonnet',
    'Champagne Cardigan',
    'Hitofude Cardigan',
    'The Simple Thing',
    'Hipster Hat',
    'Flax worsted',
]
PROJECT_QUERY_OPTIONS: Final[list[str]] = POPULAR_PROJECT_PATTERNS

@overload
def get_const(const_name: str) -> Any: ...
@overload
def get_const[T](const_name: str, fallback: T) -> T: ...

def get_const[T](const_name: str, fallback: T | None = None) -> T | Any:
    return globals().get(const_name, fallback)
