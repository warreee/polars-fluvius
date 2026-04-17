"""Fluvius Energy data source for Polars."""

from __future__ import annotations

import polars as pl

from ..readers.energy_reader import FluviusEnergyReader
from ..schemas.energy_schema import ENERGY_SCHEMA


class FluviusEnergyDataSource:
    """Polars data source for reading Fluvius energy measurements.

    This data source allows you to read energy measurement data from the
    Fluvius Energy API directly into a Polars DataFrame / LazyFrame.

    Required Options:
        - ean: GSRN EAN-code that identifies the installation
        - period_type: Type of period ("readTime" or "insertTime")

    Optional Options:
        Credential options (if not using environment variables):
            - subscription_key: Azure API Management subscription key
            - client_id: Azure AD application (client) ID
            - tenant_id: Azure AD tenant ID
            - scope: OAuth2 scope
            - data_access_contract_number: Data access contract number
            - certificate_thumbprint: Certificate thumbprint (for cert auth)
            - private_key: Private key in PEM format (for cert auth)
            - client_secret: Client secret (for secret auth)
            - credentials_prefix: Environment variable prefix (default: "FLUVIUS")

        Environment options:
            - environment: "sandbox" (default) or "production"

        Filter options:
            - reference_number: Custom reference number
            - granularity: Granularity filter (e.g., "daily", "hourly_quarterhourly")
            - complex_energy_types: Types of complex energy (e.g., "active,reactive")
            - from_date: Start date (ISO format, e.g., "2024-01-01")
            - to_date: End date (ISO format, e.g., "2024-01-31")

    Example:
        ```python
        import polars_fluvius as pf

        lf = pf.scan_energy(
            ean="541234567890123456",
            period_type="readTime",
            granularity="daily",
            from_date="2024-01-01",
            to_date="2024-01-31",
        )
        df = lf.collect()
        ```
    """

    def __init__(self, options: dict[str, str]) -> None:
        self.options = options

    @classmethod
    def name(cls) -> str:
        """Return the short name of this data source."""
        return "fluvius.energy"

    def schema(self) -> pl.Schema:
        """Return the schema for energy data."""
        return ENERGY_SCHEMA

    def reader(self, schema: pl.Schema) -> FluviusEnergyReader:
        """Return a reader for energy data.

        Args:
            schema: The schema to use (typically the default ENERGY_SCHEMA).

        Returns:
            A FluviusEnergyReader instance.
        """
        return FluviusEnergyReader(schema, self.options)

    def scan(self) -> pl.LazyFrame:
        """Return a LazyFrame for the configured energy data."""
        return self.reader(self.schema()).lazy_frame()

    def read(self) -> pl.DataFrame:
        """Return an eager DataFrame for the configured energy data."""
        return self.reader(self.schema()).to_dataframe()