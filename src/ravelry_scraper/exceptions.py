class RavelryScraperError(Exception):
    pass

class InvalidPayloadError(RavelryScraperError):
    pass

class HTTPStatusError(RavelryScraperError):
    def __init__(self, status_code: int, message: str):
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code