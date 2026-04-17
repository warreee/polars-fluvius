"""Energy data source reader."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Iterator

import polars as pl

from fluvius_energy_api import FluviusEnergyClient
from fluvius_energy_api.exceptions import NotFoundError
from fluvius_energy_api.models.enums import PeriodType

from ..converters.energy_converter import EnergyTuple, convert_energy_response
from ..utils.credentials import get_credentials, get_environment

if TYPE_CHECKING:
    pass


class FluviusEnergyReader:
    """Reader for Fluvius energy data."""

    def __init__(self, schema: pl.Schema, options: dict[str, str]) -> None:
        """Initialize the energy reader.

        Args:
            schema: The Polars schema for energy data.
            options: Options passed to the data source.
        """
        self._schema = schema
        self._options = options

    def partitions(self) -> list[int]:
        """Return a single partition for energy data."""
        return [0]

    def read(self, partition: int) -> Iterator[EnergyTuple]:
        """Read energy data from the Fluvius API.

        Args:
            partition: The partition to read (unused, single partition).

        Yields:
            Tuples representing energy measurement rows.

        Raises:
            ValueError: If required options (ean, period_type) are missing.
        """
        ean = self._options.get("ean")
        if not ean:
            raise ValueError("Option 'ean' is required for fluvius.energy data source")

        period_type_str = self._options.get("period_type")
        if not period_type_str:
            raise ValueError("Option 'period_type' is required for fluvius.energy data source")

        period_type = PeriodType(period_type_str)

        reference_number = self._options.get("reference_number")
        granularity = self._options.get("granularity")
        complex_energy_types = self._options.get("complex_energy_types")

        from_date_str = self._options.get("from_date")
        from_date: datetime | None = None
        if from_date_str:
            from_date = datetime.fromisoformat(from_date_str.replace("Z", "+00:00"))

        to_date_str = self._options.get("to_date")
        to_date: datetime | None = None
        if to_date_str:
            to_date = datetime.fromisoformat(to_date_str.replace("Z", "+00:00"))

        credentials = get_credentials(self._options)
        environment = get_environment(self._options)

        with FluviusEnergyClient(credentials=credentials, environment=environment) as client:
            try:
                response = client.get_energy(
                    ean=ean,
                    period_type=period_type,
                    reference_number=reference_number,
                    granularity=granularity,
                    complex_energy_types=complex_energy_types,
                    from_date=from_date,
                    to_date=to_date,
                )
            except NotFoundError:
                return

            rows = convert_energy_response(response)
            yield from rows

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