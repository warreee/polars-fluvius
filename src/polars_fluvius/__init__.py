"""Polars IO sources for Fluvius Energy API.

This package provides Polars IO sources for reading energy and mandate data
from the Fluvius Energy API directly into Polars DataFrames / LazyFrames.

Example:
    ```python
    import polars_fluvius as pf

    # Read mandates
    mandates_lf = pf.scan_mandates(status="Approved")
    mandates_df = mandates_lf.collect()

    # Read energy data
    energy_lf = pf.scan_energy(
        ean="541234567890123456",
        period_type="readTime",
        granularity="daily",
        from_date="2024-01-01",
        to_date="2024-01-31",
    )
    energy_df = energy_lf.collect()
    ```
"""

from __future__ import annotations

from .datasources import FluviusEnergyDataSource, FluviusMandatesDataSource
from .schemas import ENERGY_SCHEMA, MANDATES_SCHEMA

__version__ = "0.2.1"
__all__ = [
    "FluviusEnergyDataSource",
    "FluviusMandatesDataSource",
    "ENERGY_SCHEMA",
    "MANDATES_SCHEMA",
    "scan_energy",
    "scan_mandates",
]


def scan_energy(**options: str):
    """Scan Fluvius energy data as a Polars LazyFrame.

    Required options:
        - ean: GSRN EAN-code that identifies the installation
        - period_type: Type of period ("readTime" or "insertTime")

    See ``FluviusEnergyDataSource`` for the full list of options.
    """
    return FluviusEnergyDataSource(options).scan()


def scan_mandates(**options: str):
    """Scan Fluvius mandates as a Polars LazyFrame.

    See ``FluviusMandatesDataSource`` for the full list of options.
    """
    return FluviusMandatesDataSource(options).scan()