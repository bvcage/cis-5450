class RavelryScraperError(Exception):
    pass

class InvalidPayloadError(RavelryScraperError):
    pass

class HTTPStatusError(RavelryScraperError):
    def __init__(self, endpoint: str, status_code: int, message: str):
        super().__init__(f"HTTP {status_code}: {message}")
        self.endpoint = endpoint
        self.status_code = status_code

class RequestError(RavelryScraperError):
    def __init__(self, endpoint: str, cause: Exception):
        super().__init__(str(cause))
        self.endpoint = endpoint
        self.error_type = type(cause).__name__
        self.message = str(cause)
