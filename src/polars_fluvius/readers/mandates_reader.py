"""Mandates data source reader."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Iterator

import polars as pl

from fluvius_energy_api import FluviusEnergyClient
from fluvius_energy_api.models.enums import (
    DataServiceType,
    EnergyType,
    MandateRenewalStatus,
    MandateStatus,
)

from ..converters.mandates_converter import MandateTuple, convert_mandate
from ..utils.credentials import get_credentials, get_environment

if TYPE_CHECKING:
    pass


class FluviusMandatesReader:
    """Reader for Fluvius mandates data."""

    def __init__(self, schema: pl.Schema, options: dict[str, str]) -> None:
        """Initialize the mandates reader.

        Args:
            schema: The Polars schema for mandates.
            options: Options passed to the data source.
        """
        self._schema = schema
        self._options = options

    def partitions(self) -> list[int]:
        """Return a single partition for mandates."""
        return [0]

    def read(self, partition: int) -> Iterator[MandateTuple]:
        """Read mandates from the Fluvius API.

        Args:
            partition: The partition to read (unused, single partition).

        Yields:
            Tuples representing mandate rows.
        """
        credentials = get_credentials(self._options)
        environment = get_environment(self._options)

        reference_number = self._options.get("reference_number")
        ean = self._options.get("ean")

        data_service_types_str = self._options.get("data_service_types")
        data_service_types: list[DataServiceType] | None = None
        if data_service_types_str:
            data_service_types = [
                DataServiceType(t.strip()) for t in data_service_types_str.split(",")
            ]

        energy_type_str = self._options.get("energy_type")
        energy_type: EnergyType | None = None
        if energy_type_str:
            energy_type = EnergyType(energy_type_str)

        status_str = self._options.get("status")
        status: MandateStatus | None = None
        if status_str:
            status = MandateStatus(status_str)

        mandate_expiration_date_str = self._options.get("mandate_expiration_date")
        mandate_expiration_date: datetime | None = None
        if mandate_expiration_date_str:
            mandate_expiration_date = datetime.fromisoformat(
                mandate_expiration_date_str.replace("Z", "+00:00")
            )

        renewal_status_str = self._options.get("renewal_status")
        renewal_status: MandateRenewalStatus | None = None
        if renewal_status_str:
            renewal_status = MandateRenewalStatus(renewal_status_str)

        last_updated_from_str = self._options.get("last_updated_from")
        last_updated_from: datetime | None = None
        if last_updated_from_str:
            last_updated_from = datetime.fromisoformat(
                last_updated_from_str.replace("Z", "+00:00")
            )

        last_updated_to_str = self._options.get("last_updated_to")
        last_updated_to: datetime | None = None
        if last_updated_to_str:
            last_updated_to = datetime.fromisoformat(last_updated_to_str.replace("Z", "+00:00"))

        with FluviusEnergyClient(credentials=credentials, environment=environment) as client:
            response = client.get_mandates(
                reference_number=reference_number,
                ean=ean,
                data_service_types=data_service_types,
                energy_type=energy_type,
                status=status,
                mandate_expiration_date=mandate_expiration_date,
                renewal_status=renewal_status,
                last_updated_from=last_updated_from,
                last_updated_to=last_updated_to,
            )

            if response.data and response.data.mandates:
                for mandate in response.data.mandates:
                    yield convert_mandate(mandate)

    def to_dataframe(self) -> pl.DataFrame:
        """Read all rows and return them as an eager Polars DataFrame."""
        rows = list(self.read(0))
        return pl.DataFrame(rows, schema=self._schema, orient="row")

    def lazy_frame(self) -> pl.LazyFrame:
        """Return a LazyFrame backed by a Polars IO source plugin."""
        from polars.io.plugins import register_io_source

        schema = self._schema

        def _source(
            with_columns: list[str] | None,
            predicate: pl.Expr | None,
            n_rows: int | None,
            batch_size: int | None,
        ) -> Iterator[pl.DataFrame]:
            df = self.to_dataframe()
            if with_columns is not None:
                df = df.select(with_columns)
            if predicate is not None:
                df = df.filter(predicate)
            if n_rows is not None:
                df = df.head(n_rows)
            yield df

        return register_io_source(_source, schema=schema)