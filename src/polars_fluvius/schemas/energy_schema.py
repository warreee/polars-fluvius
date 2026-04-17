"""Polars schema for normalized energy measurement data."""

from __future__ import annotations

import polars as pl

ENERGY_SCHEMA: pl.Schema = pl.Schema(
    {
        "ean": pl.String,
        "energy_type": pl.String,
        "metering_type": pl.String,
        "source": pl.String,
        "meter_id": pl.String,
        "seq_number": pl.String,
        "sub_headpoint_ean": pl.String,
        "sub_headpoint_type": pl.String,
        "sub_headpoint_seq_number": pl.String,
        "vreg_id": pl.String,
        "production_installation_type": pl.String,
        "granularity": pl.String,
        "start": pl.Datetime("us", "UTC"),
        "end": pl.Datetime("us", "UTC"),
        "direction": pl.String,
        "register_type": pl.String,
        "value": pl.Float64,
        "unit": pl.String,
        "validation_state": pl.String,
        "gas_conversion_factor": pl.String,
    }
)