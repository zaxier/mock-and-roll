"""
Centralized CLI argument parsing for demo pipelines.

This module provides standardized command line argument parsing for all demo pipelines,
ensuring consistency and DRY principles across the framework.
"""

import argparse
from typing import Dict, Any, List, Tuple, Optional

from core.logging_config import get_logger

logger = get_logger(__name__)


def parse_demo_args(
    description: str = "Demo Pipeline", 
    custom_args: Optional[List[Tuple[str, type, str]]] = None
) -> Dict[str, Any]:
    """
    Parse command line arguments for demo pipelines with standardized options.
    
    This function provides a consistent CLI interface across all demos with support
    for the most common configuration overrides. Custom arguments can be added
    for demo-specific needs.
    
    Args:
        description: Description for the argument parser help text
        custom_args: List of tuples (arg_name, arg_type, help_text) for additional arguments
        
    Returns:
        Dictionary of parsed CLI arguments with None values filtered out
        
    Example:
        ```python
        from core.cli import parse_demo_args
        
        # Standard usage
        cli_overrides = parse_demo_args("Sales Demo Pipeline")
        
        # With custom arguments
        custom_args = [("--batch-size", int, "Override batch size")]
        cli_overrides = parse_demo_args("Custom Demo", custom_args)
        ```
    """
    parser = argparse.ArgumentParser(description=description)
    
    # Standard demo arguments that map to configuration overrides
    parser.add_argument(
        "--schema",
        type=str,
        help="Override the Databricks schema name"
    )
    parser.add_argument(
        "--catalog",
        type=str,
        help="Override the Databricks catalog name"
    )
    parser.add_argument(
        "--volume",
        type=str,
        help="Override the Databricks volume name"
    )
    parser.add_argument(
        "--records",
        type=int,
        help="Override the number of records to generate"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Override the logging level"
    )
    
    # Add any custom arguments if provided
    if custom_args:
        for arg_name, arg_type, help_text in custom_args:
            parser.add_argument(arg_name, type=arg_type, help=help_text)
    
    # Parse arguments
    args = parser.parse_args()
    
    # Convert to dictionary and filter out None values
    cli_overrides = {k: v for k, v in vars(args).items() if v is not None}
    
    # Log any overrides provided
    if cli_overrides:
        logger.debug(f"CLI overrides provided: {cli_overrides}")
    
    return cli_overrides


def get_demo_args_with_config(
    description: str = "Demo Pipeline",
    custom_args: Optional[List[Tuple[str, type, str]]] = None
) -> Tuple[Dict[str, Any], Any]:
    """
    Parse CLI arguments and load configuration with overrides applied.
    
    This is a convenience function that combines CLI parsing with configuration
    loading, applying CLI overrides with highest precedence.
    
    Args:
        description: Description for the argument parser help text
        custom_args: List of tuples (arg_name, arg_type, help_text) for additional arguments
        
    Returns:
        Tuple of (cli_overrides_dict, config_object)
        
    Example:
        ```python
        from core.cli import get_demo_args_with_config
        
        cli_overrides, config = get_demo_args_with_config("Sales Demo Pipeline")
        ```
    """
    from config import get_config
    
    # Parse CLI arguments
    cli_overrides = parse_demo_args(description, custom_args)
    
    # Load configuration with CLI overrides
    config = get_config(cli_overrides=cli_overrides)
    
    return cli_overrides, config