"""Fluvius Mandates data source for Polars."""

from __future__ import annotations

import polars as pl

from ..readers.mandates_reader import FluviusMandatesReader
from ..schemas.mandates_schema import MANDATES_SCHEMA


class FluviusMandatesDataSource:
    """Polars data source for reading Fluvius mandates.

    Options:
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
            - reference_number: Filter by custom reference number
            - ean: Filter by GSRN EAN-code
            - data_service_types: Comma-separated list of data service types
            - energy_type: "E" (electricity) or "G" (gas)
            - status: Mandate status (Requested, Approved, Rejected, Finished)
            - mandate_expiration_date: Filter by expiration date (ISO format)
            - renewal_status: ToBeRenewed, RenewalRequested, or Expired
            - last_updated_from: Start of last updated filter (ISO format)
            - last_updated_to: End of last updated filter (ISO format)

    Example:
        ```python
        import polars_fluvius as pf

        lf = pf.scan_mandates(status="Approved", energy_type="E")
        df = lf.collect()
        ```
    """

    def __init__(self, options: dict[str, str]) -> None:
        self.options = options

    @classmethod
    def name(cls) -> str:
        """Return the short name of this data source."""
        return "fluvius.mandates"

    def schema(self) -> pl.Schema:
        """Return the schema for mandates data."""
        return MANDATES_SCHEMA

    def reader(self, schema: pl.Schema) -> FluviusMandatesReader:
        """Return a reader for mandates data."""
        return FluviusMandatesReader(schema, self.options)

    def scan(self) -> pl.LazyFrame:
        """Return a LazyFrame for the configured mandates query."""
        return self.reader(self.schema()).lazy_frame()

    def read(self) -> pl.DataFrame:
        """Return an eager DataFrame for the configured mandates query."""
        return self.reader(self.schema()).to_dataframe()