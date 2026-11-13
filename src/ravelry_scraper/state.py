import json
from datetime import UTC, datetime
from pathlib import Path

from ravelry_scraper.constants import DATA_DIR
from ravelry_scraper.logger import get_logger

MODULE = Path(__file__).stem
logger = get_logger(MODULE)

class StateManager:
    def __init__(self, state_filepath: str = f"{DATA_DIR}/state.json"):
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

    def update_checkpoint(self, category: str, state_changes: dict, replace=False):
        if replace:
            self.state[category] = state_changes | { "updated_at": datetime.now(UTC).isoformat() }
        else:
            existing_state = self.state.get(category, {})
            self.state[category] = existing_state | state_changes | { "updated_at": datetime.now(UTC).isoformat() }

        temp_path = self.path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as file:
            json.dump(self.state, file, indent=4)
        temp_path.replace(self.path)
