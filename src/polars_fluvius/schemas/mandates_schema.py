"""Polars schema for mandates data."""

from __future__ import annotations

import polars as pl

MANDATES_SCHEMA: pl.Schema = pl.Schema(
    {
        "reference_number": pl.String,
        "status": pl.String,
        "ean": pl.String,
        "energy_type": pl.String,
        "data_period_from": pl.Datetime("us", "UTC"),
        "data_period_to": pl.Datetime("us", "UTC"),
        "data_service_type": pl.String,
        "mandate_expiration_date": pl.Datetime("us", "UTC"),
        "renewal_status": pl.String,
    }
)