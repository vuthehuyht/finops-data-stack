"""
Standardized logging utilities for FinOps Data Stack.
"""

import logging
import sys


def setup_logger(name: str) -> logging.Logger:
    """Initialize a standardized logging configuration for python modules.

    Args:
        name (str): Name of the logger.

    Returns:
        logging.Logger: The configured Logger.
    """
    logger = logging.getLogger(name)

    # Avoid duplicate handlers if already configured
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Standard Output stream handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
