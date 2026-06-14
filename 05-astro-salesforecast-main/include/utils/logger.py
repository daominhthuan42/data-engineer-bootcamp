# logger.py
import sys
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
from colorlog import ColoredFormatter

class LoggerConfig:
    """
    Logger configuration utility for applications.
    """

    @staticmethod
    def setup_logger(name: str = "spark", level: int = logging.DEBUG, 
                     log_dir: str | None = None) -> logging.Logger:
        """
        Create and configure logger with console and file handlers.

        Parameters
        ----------
        name : str, optional
            Logger name. Default is "spark".

        level : int, optional
            Logging level. Default is logging.DEBUG.

        log_dir : str, optional
            Directory to store log files.

        Returns
        -------
        logging.Logger
            Configured logger instance.
        """

        logger = logging.getLogger(name)

        # If logger already configured → reuse
        if logger.handlers:
            return logger

        logger.setLevel(level)

        # Disable log propagation to prevent duplicate logs
        logger.propagate = False

        # Remove existing handlers (avoid duplicated output)
        if logger.hasHandlers():
            logger.handlers.clear()

        # Colored Console Format
        color_formatter = ColoredFormatter(
            "%(log_color)s%(asctime)s | "
            "%(levelname)-8s | "
            # "%(cyan)s%(name)s:%(funcName)s%(reset)s - "
            "%(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "white",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
            secondary_log_colors={},
            style="%"
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(color_formatter)
        logger.addHandler(console_handler)

        # File Handler (No color)
        if log_dir:
            log_dir = Path(log_dir)
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / f"{name}.log"

            file_formatter = logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )

            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=500 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8"
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

        logger.info(f"Logging configured: level={logging.getLevelName(level)}, format=colored")

        return logger

PROJECT_ROOT = Path(__file__).resolve().parents[2]
logger = LoggerConfig.setup_logger(
    name="data_engineering",
    log_dir=str(PROJECT_ROOT / "logs")
)