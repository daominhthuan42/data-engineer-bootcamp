# logger.py
import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from colorlog import ColoredFormatter
from config.runtime_context import RuntimeContext

class LoggerFactory:
    """
    Logger configuration utility for applications.
    """

    @staticmethod
    def _resolve_run_id(run_id: str | None = None) -> str:
        """
        Resolve run_id with priority:
        1. Passed parameter
        2. ENV variable
        3. Auto-generate
        """
        if run_id:
            return run_id

        return RuntimeContext.get_run_id()

    @staticmethod
    def setup_logger(name: str = "etl", level: int = logging.DEBUG, 
                     log_dir: str | None = None, run_id: str | None = None) -> logging.Logger:
        """
        Configure a reusable ETL logger with console and optional file output.

        - 1 run = 1 log file (etl_<run_id>.log)
        - Avoids duplicate handlers
        - Safe to call multiple times

        Parameters
        ----------
        name : str
            Logger name (default: "etl")
        level : int
            Logging level
        log_dir : str | None
            Directory for log file (None = console only)
        run_id : str | None
            Run identifier (param → ENV `RUN_ID` → auto timestamp)

        Returns
        -------
        logging.Logger
        """

        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.propagate = False

        # ===== Resolve run_id =====
        run_id = LoggerFactory._resolve_run_id(run_id)

        # ===== Resolve log file (shared) =====
        log_file = None
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, f"etl_{run_id}.log")

        # ===== Formatter =====
        color_formatter = ColoredFormatter(
            "%(log_color)s%(asctime)s | %(levelname)-8s | %(name)s | %(filename)s:%(lineno)d | %(message)s",
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

        file_formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(filename)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # ===== Prevent duplicate FILE handler =====
        if log_file:
            for h in logger.handlers:
                if isinstance(h, RotatingFileHandler) and h.baseFilename == log_file:
                    return logger

        # ===== Clear old handlers (safe reset) =====
        if logger.hasHandlers():
            logger.handlers.clear()

        # ===== Console handler =====
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(color_formatter)
        logger.addHandler(console_handler)

        # ===== File handler =====
        if log_file:
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=500 * 1024 * 1024,  # 500MB
                backupCount=5,
                encoding="utf-8"
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

        logger.info(f"Logger initialized | level={logging.getLevelName(level)} | file={log_file}")

        return logger
