"""
ETL Package for B3 IBOVESPA Data Pipeline

This package contains modules for extracting, transforming, and loading
B3 stock market data.
"""

from .extract import B3DataExtractor

__version__ = "1.0.0"
__author__ = "FIAP ETL Team"

__all__ = [
    "B3DataExtractor"
] 