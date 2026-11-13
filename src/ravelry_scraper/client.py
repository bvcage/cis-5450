import os
from json import JSONDecodeError
from pathlib import Path

import httpx
from dotenv import load_dotenv

from ravelry_scraper.exceptions import (
    HTTPStatusError,
    InvalidPayloadError,
    RequestError,
)
from ravelry_scraper.logger import get_logger

load_dotenv()

MODULE = Path(__file__).stem
logger = get_logger(MODULE)

class RavelryClient:
    def __init__(self):
        self.base_url = "https://api.ravelry.com"
        self.auth = (os.getenv("RAVELRY_USER"), os.getenv("RAVELRY_PASSWORD"))
        self.client = httpx.Client(
            base_url=self.base_url,
            auth=self.auth,
            timeout=10.0
        )

    def fetch_endpoint(self, endpoint: str, params: dict | None = None) -> dict:
        try:
            response = self.client.get(endpoint, params=params)
            response.raise_for_status()

            try:
                data = response.json()
                logger.verbose_info(f"Successfully retrieved JSON from endpoint: {str(response.url).replace(self.base_url, "")}")

                return data
            except JSONDecodeError:
                logger.warning(
                    "Response OK but not JSON",
                    extra={"verbose": True}
                )
                raise InvalidPayloadError(f"Expected JSON but received raw body from endpoint: {endpoint}")

        except httpx.HTTPStatusError as err:
            logger.error(
                f"HTTP error [{err.response.status_code}] on endpoint: {endpoint}",
                extra={"verbose": err.response.status_code != 400}
            )
            raise HTTPStatusError(
                endpoint=endpoint,
                message=f"Server returned error code for endpoint: {endpoint}",
                status_code=err.response.status_code,
            ) from err

        except httpx.RequestError as err:
            logger.exception(
                "Request failed [%s] on endpoint: %s",
                type(err).__name__,
                endpoint,
            )
            raise RequestError(
                endpoint=endpoint,
                cause=err
            ) from err

        except Exception as err:
            logger.error(f"UNKNOWN ERROR on endpoint: {endpoint}\n{err!s}")
            raise
