"""Tests for the mandates data source."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl

from polars_fluvius.converters.mandates_converter import convert_mandate
from polars_fluvius.datasources.mandates import FluviusMandatesDataSource
from polars_fluvius.schemas.mandates_schema import MANDATES_SCHEMA


class TestMandatesSchema:
    """Tests for the mandates schema."""

    def test_schema_has_correct_fields(self) -> None:
        """Verify the schema has all expected fields."""
        field_names = list(MANDATES_SCHEMA.names())

        expected_fields = [
            "reference_number",
            "status",
            "ean",
            "energy_type",
            "data_period_from",
            "data_period_to",
            "data_service_type",
            "mandate_expiration_date",
            "renewal_status",
        ]

        assert field_names == expected_fields

    def test_schema_field_types(self) -> None:
        """Verify field types are correct."""
        assert MANDATES_SCHEMA["reference_number"] == pl.String
        assert MANDATES_SCHEMA["status"] == pl.String
        assert MANDATES_SCHEMA["ean"] == pl.String
        assert MANDATES_SCHEMA["data_period_from"] == pl.Datetime("us", "UTC")
        assert MANDATES_SCHEMA["mandate_expiration_date"] == pl.Datetime("us", "UTC")


class TestMandatesConverter:
    """Tests for the mandates converter."""

    def test_convert_mandate(self, mock_mandate: MagicMock) -> None:
        """Test converting a mandate to a tuple."""
        result = convert_mandate(mock_mandate)

        assert result[0] == "test-ref-001"  # reference_number
        assert result[1] == "Approved"  # status
        assert result[2] == "541234567890123456"  # ean
        assert result[3] == "E"  # energy_type
        assert result[4] == datetime(2024, 1, 1, tzinfo=timezone.utc)  # data_period_from
        assert result[5] == datetime(2024, 12, 31, tzinfo=timezone.utc)  # data_period_to
        assert result[6] == "VH_dag"  # data_service_type
        assert result[7] == datetime(2025, 1, 1, tzinfo=timezone.utc)  # mandate_expiration_date
        assert result[8] == "ToBeRenewed"  # renewal_status

    def test_convert_mandate_with_nulls(self) -> None:
        """Test converting a mandate with null values."""
        mandate = MagicMock()
        mandate.reference_number = None
        mandate.status = None
        mandate.ean = "541234567890123456"
        mandate.energy_type = None
        mandate.data_period_from = None
        mandate.data_period_to = None
        mandate.data_service_type = None
        mandate.mandate_expiration_date = None
        mandate.renewal_status = None

        result = convert_mandate(mandate)

        assert result[0] is None
        assert result[1] is None
        assert result[2] == "541234567890123456"
        assert result[3] is None


class TestFluviusMandatesDataSource:
    """Tests for the FluviusMandatesDataSource class."""

    def test_name(self) -> None:
        """Test the data source name."""
        assert FluviusMandatesDataSource.name() == "fluvius.mandates"

    def test_schema(self) -> None:
        """Test the schema method returns the correct schema."""
        ds = FluviusMandatesDataSource({})
        assert ds.schema() == MANDATES_SCHEMA

    @patch("polars_fluvius.readers.mandates_reader.FluviusEnergyClient")
    @patch("polars_fluvius.readers.mandates_reader.get_credentials")
    @patch("polars_fluvius.readers.mandates_reader.get_environment")
    def test_reader_returns_mandates(
        self,
        mock_get_env: MagicMock,
        mock_get_creds: MagicMock,
        mock_client_class: MagicMock,
        mock_mandates_response: MagicMock,
    ) -> None:
        """Test the reader yields mandate tuples."""
        from fluvius_energy_api.client import Environment

        mock_get_creds.return_value = MagicMock()
        mock_get_env.return_value = Environment.SANDBOX

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get_mandates.return_value = mock_mandates_response
        mock_client_class.return_value = mock_client

        ds = FluviusMandatesDataSource({"status": "Approved"})
        reader = ds.reader(MANDATES_SCHEMA)

        partitions = reader.partitions()
        assert len(partitions) == 1

        results = list(reader.read(partitions[0]))
        assert len(results) == 1
        assert results[0][1] == "Approved"  # status

    @patch("polars_fluvius.readers.mandates_reader.FluviusEnergyClient")
    @patch("polars_fluvius.readers.mandates_reader.get_credentials")
    @patch("polars_fluvius.readers.mandates_reader.get_environment")
    def test_reader_with_no_mandates(
        self,
        mock_get_env: MagicMock,
        mock_get_creds: MagicMock,
        mock_client_class: MagicMock,
    ) -> None:
        """Test the reader handles empty response."""
        from fluvius_energy_api.client import Environment

        mock_get_creds.return_value = MagicMock()
        mock_get_env.return_value = Environment.SANDBOX

        mock_response = MagicMock()
        mock_response.data = MagicMock()
        mock_response.data.mandates = []

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get_mandates.return_value = mock_response
        mock_client_class.return_value = mock_client

        ds = FluviusMandatesDataSource({})
        reader = ds.reader(MANDATES_SCHEMA)

        partitions = reader.partitions()
        results = list(reader.read(partitions[0]))
        assert len(results) == 0

    @patch("polars_fluvius.readers.mandates_reader.FluviusEnergyClient")
    @patch("polars_fluvius.readers.mandates_reader.get_credentials")
    @patch("polars_fluvius.readers.mandates_reader.get_environment")
    def test_scan_returns_lazyframe(
        self,
        mock_get_env: MagicMock,
        mock_get_creds: MagicMock,
        mock_client_class: MagicMock,
        mock_mandates_response: MagicMock,
    ) -> None:
        """Test that scan() returns a Polars LazyFrame collectable to a DataFrame."""
        from fluvius_energy_api.client import Environment

        mock_get_creds.return_value = MagicMock()
        mock_get_env.return_value = Environment.SANDBOX

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get_mandates.return_value = mock_mandates_response
        mock_client_class.return_value = mock_client

        ds = FluviusMandatesDataSource({"status": "Approved"})
        lf = ds.scan()
        assert isinstance(lf, pl.LazyFrame)

        df = lf.collect()
        assert df.height == 1
        assert df["status"][0] == "Approved"
