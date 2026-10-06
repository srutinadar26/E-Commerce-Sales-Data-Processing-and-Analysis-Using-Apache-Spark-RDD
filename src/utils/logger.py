"""
utils/logger.py
---------------
Centralised logging configuration for the project.
All modules import `get_logger(__name__)` to get a properly
configured logger that writes to both the console and a
rotating log file.
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

import yaml


def load_config(config_path: str = None) -> dict:
    """Load the YAML configuration file."""
    if config_path is None:
        # Walk up from src/utils/ --> src/ --> project root, then find config/config.yaml
        base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base, "config", "config.yaml")
    with open(config_path, "r") as fh:
        return yaml.safe_load(fh)


def get_logger(name: str, config_path: str = None) -> logging.Logger:
    """
    Return a logger that writes to:
      • STDOUT (INFO level)
      • logs/ecommerce_spark_<date>.log (DEBUG level, rotating)

    Parameters
    ----------
    name : str
        Typically ``__name__`` of the calling module.
    config_path : str, optional
        Override path to config.yaml.

    Returns
    -------
    logging.Logger
    """
    try:
        cfg = load_config(config_path)
        log_level_str = cfg.get("app", {}).get("log_level", "INFO")
        logs_dir = cfg.get("paths", {}).get("logs_dir", "logs")
    except Exception:
        log_level_str = "INFO"
        logs_dir = "logs"

    log_level = getattr(logging, log_level_str.upper(), logging.INFO)

    logger = logging.getLogger(name)
    if logger.handlers:
        # Already configured – avoid duplicate handlers
        return logger

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # -- Console handler ------------------------------------------------------
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # -- File handler (rotating) -----------------------------------------------
    try:
        # Resolve logs directory relative to project root (src/utils --> src --> root)
        base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        abs_logs_dir = os.path.join(base, logs_dir)
        os.makedirs(abs_logs_dir, exist_ok=True)
        log_filename = os.path.join(
            abs_logs_dir,
            f"ecommerce_spark_{datetime.now().strftime('%Y%m%d')}.log",
        )
        file_handler = RotatingFileHandler(
            log_filename, maxBytes=10 * 1024 * 1024, backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as exc:
        logger.warning("Could not create file handler: %s", exc)

    return logger
