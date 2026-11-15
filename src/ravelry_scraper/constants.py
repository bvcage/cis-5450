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
POPULAR_PATTERNS_BY_PROJECT_COUNT = [
    'Sophie Scarf',
    'Musselburgh',
    "Hermione's Everyday Socks",
    'Ranunculus',
    'Hitchhiker',
    'Fish Lips Kiss Heel',
    'Flax worsted',
    'Baby Surprise Jacket',
    'Oslo Hat',
    'Sockhead Slouch Hat',
    'Barley worsted',
    'Honey Cowl',
    "The World's Simplest Mittens",
    'Step by Step Sweater',
    'Clapotis',
    'Monkey Socks',
    'Vanilla Latte Socks',
    "Grandmother's Favorite The Original",
    'Fetching',
    'Turn A Square',
    'GAP-tastic Cowl',
    'Calorimetry',
    'Sock Recipe: A Good, Plain Sock',
    'Sophie Hood',
    'wurm',
]
PROJECT_QUERY_OPTIONS: Final[list[str]] = set(POPULAR_PROJECT_PATTERNS + POPULAR_PATTERNS_BY_PROJECT_COUNT)

@overload
def get_const(const_name: str) -> Any: ...
@overload
def get_const[T](const_name: str, fallback: T) -> T: ...

def get_const[T](const_name: str, fallback: T | None = None) -> T | Any:
    return globals().get(const_name, fallback)
