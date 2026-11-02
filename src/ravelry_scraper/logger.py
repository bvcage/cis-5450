import logging
from datetime import datetime, UTC
from pathlib import Path


class RavelryLogger(logging.LoggerAdapter):
    def log_duration(self, start_time: datetime, end_time: datetime, time_format = '%Y-%m-%d @ %I:%M:%S %p'):
        self.info(f"Began at: {start_time.astimezone().strftime(time_format)}")
        self.info(f"Ended at: {end_time.astimezone().strftime(time_format)}")
        self.info(f"Elapsed time: {str(end_time - start_time)}")


def get_logger(module_name: str) -> logging.Logger:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"{datetime.now(UTC).strftime('%Y-%m-%d')}.log"

    logger = logging.getLogger(module_name)

    if logger.hasHandlers():
        return RavelryLogger(logger, {})

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Persisted logging
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Live terminal logging
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return RavelryLogger(logger, {})
