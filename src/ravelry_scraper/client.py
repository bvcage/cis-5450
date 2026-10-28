from dotenv import load_dotenv
import httpx
import os
from pathlib import Path

from .exceptions import InvalidPayloadError
from .logger import get_logger

load_dotenv()

module_filename = Path(__file__).stem
logger = get_logger(module_filename)

class RavelryClient:
    def __init__(self):
        self.base_url = "https://api.ravelry.com"
        self.auth = (os.getenv("RAVELRY_USER"), os.getenv("RAVELRY_PASSWORD"))
        self.client = httpx.Client(
            base_url=self.base_url,
            auth=self.auth,
            timeout=10.0
        )


    def fetch_endpoint(self, endpoint: str, params: dict = None) -> dict:
        try:
            response = self.client.get(endpoint, params=params)
            response.raise_for_status()

            try:
                data = response.json()
                return data
            except JSONDecodeError:
                logger.warning("Response OK but not JSON")
                raise InvalidPayloadError(f"Expected JSON but received raw body from endpoint: {endpoint}")

        except httpx.HTTPStatusError as err:
            logger.error(f"HTTP error [{err.response.status_code}] on endpoint: {endpoint}")
            raise HTTPStatusError(code, f"Server returned error code for endpoint: {endpoint}")
        
        except err:
            logger.error(f"UNKNOWN ERROR on endpoint: {endpoint}")
            raise err
