"""Tests for the energy data source."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from fluvius_energy_api import GetEnergyResponseApiDataResponse

from polars_fluvius.converters.energy_converter import convert_energy_response
from polars_fluvius.datasources.energy import FluviusEnergyDataSource
from polars_fluvius.schemas.energy_schema import ENERGY_SCHEMA


class TestEnergySchema:
    """Tests for the normalized energy schema."""

    def test_schema_has_expected_fields(self) -> None:
        """Verify the schema has all 20 normalized fields."""
        field_names = list(ENERGY_SCHEMA.names())

        expected = [
            "ean",
            "energy_type",
            "metering_type",
            "source",
            "meter_id",
            "seq_number",
            "sub_headpoint_ean",
            "sub_headpoint_type",
            "sub_headpoint_seq_number",
            "vreg_id",
            "production_installation_type",
            "granularity",
            "start",
            "end",
            "direction",
            "register_type",
            "value",
            "unit",
            "validation_state",
            "gas_conversion_factor",
        ]
        assert field_names == expected

    def test_schema_field_count(self) -> None:
        """Verify the total number of fields is 20."""
        assert len(ENERGY_SCHEMA) == 20


class TestEnergyConverter:
    """Tests for the energy converter."""

    def test_convert_energy_response(self, mock_energy_response) -> None:
        """Test converting an energy response to tuples."""
        results = convert_energy_response(mock_energy_response)

        assert len(results) == 1
        row = results[0]
        assert len(row) == 20

        assert row[0] == "541234567890123456"  # ean
        assert row[1] == "E"  # energy_type
        assert row[2] == "metering-on-headpoint"  # metering_type
        assert row[3] == "headpoint"  # source
        assert row[4] is None  # meter_id
        assert row[5] is None  # seq_number
        assert row[11] == "daily"  # granularity
        assert row[12] == datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)  # start
        assert row[13] == datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)  # end
        assert row[14] == "offtake"  # direction
        assert row[15] == "total"  # register_type
        assert row[16] == 123.45  # value
        assert row[17] == "kWh"  # unit
        assert row[18] == "VAL"  # validation_state

    def test_convert_empty_response(self) -> None:
        """Test converting an empty response."""
        response = GetEnergyResponseApiDataResponse(data=None)
        results = convert_energy_response(response)
        assert len(results) == 0

    def test_convert_response_no_headpoint(self) -> None:
        """Test converting a response with no headpoint."""
        response = GetEnergyResponseApiDataResponse.model_validate(
            {"data": {"headpoint": None}}
        )
        results = convert_energy_response(response)
        assert len(results) == 0


class TestFluviusEnergyDataSource:
    """Tests for the FluviusEnergyDataSource class."""

    def test_name(self) -> None:
        """Test the data source name."""
        assert FluviusEnergyDataSource.name() == "fluvius.energy"

    def test_schema(self) -> None:
        """Test the schema method returns the correct schema."""
        ds = FluviusEnergyDataSource({})
        assert ds.schema() == ENERGY_SCHEMA

    @patch("polars_fluvius.readers.energy_reader.FluviusEnergyClient")
    @patch("polars_fluvius.readers.energy_reader.get_credentials")
    @patch("polars_fluvius.readers.energy_reader.get_environment")
    def test_reader_requires_ean(
        self,
        mock_get_env: MagicMock,
        mock_get_creds: MagicMock,
        mock_client_class: MagicMock,
    ) -> None:
        """Test the reader raises error if ean is missing."""
        ds = FluviusEnergyDataSource({"period_type": "readTime"})
        reader = ds.reader(ENERGY_SCHEMA)
        partitions = reader.partitions()

        with pytest.raises(ValueError, match="'ean' is required"):
            list(reader.read(partitions[0]))

    @patch("polars_fluvius.readers.energy_reader.FluviusEnergyClient")
    @patch("polars_fluvius.readers.energy_reader.get_credentials")
    @patch("polars_fluvius.readers.energy_reader.get_environment")
    def test_reader_requires_period_type(
        self,
        mock_get_env: MagicMock,
        mock_get_creds: MagicMock,
        mock_client_class: MagicMock,
    ) -> None:
        """Test the reader raises error if period_type is missing."""
        ds = FluviusEnergyDataSource({"ean": "541234567890123456"})
        reader = ds.reader(ENERGY_SCHEMA)
        partitions = reader.partitions()

        with pytest.raises(ValueError, match="'period_type' is required"):
            list(reader.read(partitions[0]))

    @patch("polars_fluvius.readers.energy_reader.FluviusEnergyClient")
    @patch("polars_fluvius.readers.energy_reader.get_credentials")
    @patch("polars_fluvius.readers.energy_reader.get_environment")
    def test_reader_returns_energy_data(
        self,
        mock_get_env: MagicMock,
        mock_get_creds: MagicMock,
        mock_client_class: MagicMock,
        mock_energy_response,
    ) -> None:
        """Test the reader yields energy tuples."""
        from fluvius_energy_api.client import Environment

        mock_get_creds.return_value = MagicMock()
        mock_get_env.return_value = Environment.SANDBOX

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get_energy.return_value = mock_energy_response
        mock_client_class.return_value = mock_client

        ds = FluviusEnergyDataSource(
            {
                "ean": "541234567890123456",
                "period_type": "readTime",
                "granularity": "daily",
            }
        )
        reader = ds.reader(ENERGY_SCHEMA)

        partitions = reader.partitions()
        assert len(partitions) == 1

        results = list(reader.read(partitions[0]))
        assert len(results) == 1
        row = results[0]
        assert len(row) == 20
        assert row[0] == "541234567890123456"  # ean
        assert row[14] == "offtake"  # direction
        assert row[15] == "total"  # register_type
        assert row[16] == 123.45  # value

    @patch("polars_fluvius.readers.energy_reader.FluviusEnergyClient")
    @patch("polars_fluvius.readers.energy_reader.get_credentials")
    @patch("polars_fluvius.readers.energy_reader.get_environment")
    def test_scan_returns_lazyframe(
        self,
        mock_get_env: MagicMock,
        mock_get_creds: MagicMock,
        mock_client_class: MagicMock,
        mock_energy_response,
    ) -> None:
        """Test that scan() returns a Polars LazyFrame collectable to a DataFrame."""
        import polars as pl
        from fluvius_energy_api.client import Environment

        mock_get_creds.return_value = MagicMock()
        mock_get_env.return_value = Environment.SANDBOX

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get_energy.return_value = mock_energy_response
        mock_client_class.return_value = mock_client

        ds = FluviusEnergyDataSource(
            {"ean": "541234567890123456", "period_type": "readTime"}
        )
        lf = ds.scan()
        assert isinstance(lf, pl.LazyFrame)

        df = lf.collect()
        assert df.height == 1
        assert df["ean"][0] == "541234567890123456"
        assert df["direction"][0] == "offtake"
        assert df["value"][0] == 123.45


class TestMeteringOnMeter:
    """Tests for metering-on-meter installations."""

    def test_convert_metering_on_meter_response(self) -> None:
        """Test converting a metering-on-meter response."""
        response = GetEnergyResponseApiDataResponse.model_validate(
            {
                "data": {
                    "headpoint": {
                        "$type": "metering-on-meter",
                        "ean": "541234567890123456",
                        "energyType": "E",
                        "physicalMeters": [
                            {
                                "seqNumber": "001",
                                "meterID": "M12345",
                                "dailyEnergy": [
                                    {
                                        "start": "2024-01-01T00:00:00Z",
                                        "end": "2024-01-02T00:00:00Z",
                                        "measurements": [
                                            {
                                                "offtake": {
                                                    "total": {
                                                        "value": 100.0,
                                                        "unit": "kWh",
                                                        "validationState": "VAL",
                                                    }
                                                }
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                }
            }
        )

        results = convert_energy_response(response)

        assert len(results) == 1
        row = results[0]
        assert len(row) == 20
        assert row[0] == "541234567890123456"  # ean
        assert row[2] == "metering-on-meter"  # metering_type
        assert row[3] == "meter"  # source
        assert row[4] == "M12345"  # meter_id
        assert row[5] == "001"  # seq_number
        assert row[14] == "offtake"  # direction
        assert row[15] == "total"  # register_type
        assert row[16] == 100.0  # value
