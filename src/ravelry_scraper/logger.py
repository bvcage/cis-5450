import logging
from contextlib import contextmanager
from datetime import datetime, UTC
from pathlib import Path

class RavelryLogger(logging.LoggerAdapter):
    @contextmanager
    def log_duration(self, time_format = '%Y-%m-%d @ %I:%M:%S %p'):
        start_time = datetime.now(UTC)
        yield
        end_time = datetime.now(UTC)

        self.info(f"Began at: {start_time.astimezone().strftime(time_format)}")
        self.info(f"Ended at: {end_time.astimezone().strftime(time_format)}")
        self.info(f"Elapsed time: {str(end_time - start_time)}")

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

def get_logger(module_name: str) -> logging.Logger:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logger = logging.getLogger(module_name)

    if logger.hasHandlers():
        return RavelryLogger(logger, {})

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Persisted logging
    file_handler = DailyFileHandler(log_dir, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Live terminal logging
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return RavelryLogger(logger, {})
