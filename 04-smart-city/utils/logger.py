# logger.py
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from colorlog import ColoredFormatter

class LoggerConfig:
    """
    Centralized logger configuration utility.

    Features
    --------
    - Colored console logging
    - Rotating file logging
    - UTF-8 support
    - Shared reusable logger
    - Duplicate handler protection
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
        console_formatter = ColoredFormatter(
            "%(log_color)s"
            "%(asctime)s | "
            "%(levelname)-8s | "
            "%(name)s:%(funcName)s:%(lineno)d | "
            "%(message)s",

            datefmt="%Y-%m-%d %H:%M:%S",

            log_colors={
                "DEBUG": "white",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            }
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # File Handler (No color)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
            # Generate log file name by date
            # today = datetime.now().strftime("%Y%m%d%H%M%S")

            log_file = os.path.join(log_dir, f"{name}.log")
            file_formatter = logging.Formatter(
                "%(asctime)s | "
                "%(levelname)-8s | "
                "%(name)s:%(funcName)s:%(lineno)d | "
                "%(message)s",

                datefmt="%Y-%m-%d %H:%M:%S"
            )

            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=20 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8"
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

        logger.info(
            f"Logging configured: level={logging.getLevelName(level)}, format=colored"
        )

        return logger

# Shared Global Logger
logger = LoggerConfig.setup_logger(
    name="SmartCity",
    level=logging.INFO,
    log_dir="log"
)
