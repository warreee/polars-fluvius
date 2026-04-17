"""Polars schema definitions for Fluvius data sources."""

from .energy_schema import ENERGY_SCHEMA
from .mandates_schema import MANDATES_SCHEMA

__all__ = ["ENERGY_SCHEMA", "MANDATES_SCHEMA"]