import logging
import sys

from contextlib import contextmanager
from datetime import datetime, UTC
from pathlib import Path
from ravelry_scraper.constants import LOGS_DIR
from typing import Literal

LogMode = Literal["verbose", "essential"]

class RavelryLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        # Merge adapter's default extra with any per-call extra={"verbose": True}
        extra = self.extra.copy()
        if "extra" in kwargs:
            extra.update(kwargs["extra"])
        kwargs["extra"] = extra
        return msg, kwargs

    def verbose_info(self, msg: str, *args, **kwargs):
        extra = kwargs.setdefault("extra", {})
        extra["verbose"] = True
        self.info(msg, *args, **kwargs)

    @contextmanager
    def log_duration(self, time_format = '%Y-%m-%d @ %I:%M:%S %p'):
        start_time = datetime.now(UTC)
        yield
        end_time = datetime.now(UTC)

        self.info(f"Began at: {start_time.astimezone().strftime(time_format)}", extra={"verbose": True})
        self.info(f"Ended at: {end_time.astimezone().strftime(time_format)}", extra={"verbose": True})
        self.info(f"Elapsed time: {str(end_time - start_time)}")

class EssentialLogFilter(logging.Filter):
    """Hide DEBUG logs and any logs marked as verbose=True."""
    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno < logging.INFO:
            return False
        if getattr(record, "verbose", False):
            return False
        return True

class DailyFileHandler(logging.FileHandler):
    def __init__(self, log_dir: Path, encoding: str ="utf-8"):
        self.log_dir = log_dir
        self.current_date = self._get_local_date()
        log_file = self.log_dir / f"{self.current_date}.log"
        super().__init__(log_file, encoding=encoding)

    @staticmethod
    def _get_local_date():
        return datetime.now().strftime('%Y-%m-%d')

    def emit(self, record: logging.LogRecord):
        new_date = self._get_local_date()
        if new_date != self.current_date:
            self.current_date = new_date
            self.close()
            self.baseFilename = str(
                (self.log_dir / f"{self.current_date}.log").resolve()
            )
            self.stream = self._open()
        super().emit(record)

def is_notebook() -> bool:
    return "ipykernel" in sys.modules

def get_logger(
    module_name: str,
    log_mode: LogMode | None = None
) -> logging.Logger:
    logger = logging.getLogger(module_name)

    if log_mode is None:
        log_mode = "essential" if is_notebook() else "verbose"

    if logger.handlers:
        logger.handlers.clear()

    logger.setLevel(logging.DEBUG)
    logger.propagate = False 

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Live terminal logging
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    if log_mode == "essential":
        stream_handler.addFilter(EssentialLogFilter())
    logger.addHandler(stream_handler)

    if log_mode == "essential":
        return RavelryLogger(logger, {})

    # Persisted logging
    LOGS_DIR.mkdir(exist_ok=True)
    file_handler = DailyFileHandler(LOGS_DIR, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return RavelryLogger(logger, {})
