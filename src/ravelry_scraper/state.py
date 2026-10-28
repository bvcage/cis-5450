import json
from datetime import datetime, UTC
from pathlib import Path

from .logger import get_logger


module_filename = Path(__file__).stem
logger = get_logger(module_filename)


class StateManager:
    def __init__(self, state_filepath: str = "data/state.json"):
        self.path = Path(state_filepath)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()

    
    def _load_state(self):
        if not self.path.exists():
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as file:
                return json.load(file)
        except:
            logger.error(f"Could not load ravelry_scraper.state from file: {self.path}")
            raise


    def get_cursor(self, category: str, primary_key: str = "max_id"):
        return self.state.get(category, {}).get(primary_key, 0)

    
    def update_checkpoint(self, category: str, max_id: int):
        self.state[category] = {
            "max_id": max_id,
            "updated_at": datetime.now(UTC).isoformat(),
        }

        temp_path = self.path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as file:
            json.dump(self.state, file, indent=4)
        temp_path.replace(self.path)