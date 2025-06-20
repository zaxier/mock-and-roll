#!/usr/bin/env python3
"""
Centralized logging configuration for all demo scripts.
Provides consistent logging setup across the entire project.
"""

import logging
import sys
from typing import Optional


def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    include_timestamp: bool = True,
    include_module: bool = True
) -> logging.Logger:
    """
    Configure and return a logger with consistent formatting.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Custom format string (optional)
        include_timestamp: Whether to include timestamp in logs
        include_module: Whether to include module name in logs
        
    Returns:
        Configured logger instance
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Default format components
    format_parts = []
    
    if include_timestamp:
        format_parts.append("%(asctime)s")
    
    if include_module:
        format_parts.append("%(name)s")
    
    format_parts.extend(["%(levelname)s", "%(message)s"])
    
    # Use custom format or build default
    if format_string is None:
        format_string = " - ".join(format_parts)
    
    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        format=format_string,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True  # Override any existing configuration
    )
    
    return logging.getLogger(__name__)


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Get a logger with the specified name and level.
    
    Args:
        name: Logger name (typically __name__)
        level: Logging level
        
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger


# Pre-configured logger for immediate use
default_logger = setup_logging()