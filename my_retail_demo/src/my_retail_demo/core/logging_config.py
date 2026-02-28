"""Centralized logging configuration."""

import logging
import sys
from typing import Optional


def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    include_timestamp: bool = True,
    include_module: bool = True,
) -> logging.Logger:
    """Configure root logger with consistent formatting."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    format_parts = []
    if include_timestamp:
        format_parts.append("%(asctime)s")
    if include_module:
        format_parts.append("%(name)s")
    format_parts.extend(["%(levelname)s", "%(message)s"])

    if format_string is None:
        format_string = " - ".join(format_parts)

    logging.basicConfig(
        level=numeric_level,
        format=format_string,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    return logging.getLogger(__name__)


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Get a named logger."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger
