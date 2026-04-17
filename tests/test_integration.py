"""Integration tests against the real Fluvius sandbox API.

These tests require valid sandbox credentials in environment variables:
- FLUVIUS_SUBSCRIPTION_KEY
- FLUVIUS_SANDBOX_CLIENT_ID
- FLUVIUS_TENANT_ID
- FLUVIUS_SCOPE
- FLUVIUS_DATA_ACCESS_CONTRACT_NUMBER
- FLUVIUS_SANDBOX_CLIENT_SECRET
"""

from __future__ import annotations

import os
import uuid

import polars as pl
import pytest

from fluvius_energy_api import FluviusCredentials, FluviusEnergyClient
from fluvius_energy_api.client import Environment
from fluvius_energy_api.models.enums import DataServiceType, MandateStatus

import polars_fluvius as pf

# Test EAN for sandbox - must match pattern: 5414[4-5][0-1][0-2][0-2][0-2]000000[0-9][0-9][0-9]
TEST_EAN = "541450000000000001"


@pytest.fixture(scope="module")
def has_credentials() -> bool:
    """Check if sandbox credentials are available."""
    required_vars = [
        "FLUVIUS_SUBSCRIPTION_KEY",
        "FLUVIUS_SANDBOX_CLIENT_ID",
        "FLUVIUS_TENANT_ID",
        "FLUVIUS_SCOPE",
        "FLUVIUS_DATA_ACCESS_CONTRACT_NUMBER",
        "FLUVIUS_SANDBOX_CLIENT_SECRET",
    ]
    return all(os.environ.get(var) for var in required_vars)


@pytest.fixture(scope="module")
def sandbox_credentials() -> FluviusCredentials:
    """Create sandbox credentials using client_secret auth (not certificate)."""
    return FluviusCredentials(
        subscription_key=os.environ["FLUVIUS_SUBSCRIPTION_KEY"],
        client_id=os.environ["FLUVIUS_SANDBOX_CLIENT_ID"],
        tenant_id=os.environ["FLUVIUS_TENANT_ID"],
        scope=os.environ["FLUVIUS_SCOPE"],
        data_access_contract_number=os.environ["FLUVIUS_DATA_ACCESS_CONTRACT_NUMBER"],
        client_secret=os.environ["FLUVIUS_SANDBOX_CLIENT_SECRET"],
    )


@pytest.fixture(scope="module")
def options() -> dict[str, str]:
    """Return options for sandbox with client_secret auth."""
    return {
        "subscription_key": os.environ["FLUVIUS_SUBSCRIPTION_KEY"],
        "client_id": os.environ["FLUVIUS_SANDBOX_CLIENT_ID"],
        "tenant_id": os.environ["FLUVIUS_TENANT_ID"],
        "scope": os.environ["FLUVIUS_SCOPE"],
        "data_access_contract_number": os.environ["FLUVIUS_DATA_ACCESS_CONTRACT_NUMBER"],
        "client_secret": os.environ["FLUVIUS_SANDBOX_CLIENT_SECRET"],
        "environment": "sandbox",
    }


@pytest.fixture(scope="module")
def mock_mandate_reference(
    has_credentials: bool, sandbox_credentials: FluviusCredentials
) -> str | None:
    """Create a mock mandate in the sandbox and return its reference number."""
    if not has_credentials:
        return None

    reference = f"polars-fluvius-test-{uuid.uuid4().hex[:8]}"

    with FluviusEnergyClient(
        credentials=sandbox_credentials, environment=Environment.SANDBOX
    ) as client:
        client.create_mock_mandate(
            reference_number=reference,
            ean=TEST_EAN,
            data_service_type=DataServiceType.VH_DAG,
            data_period_from="2024-01-01T00:00:00Z",
            data_period_to="2024-12-31T23:59:59Z",
            status=MandateStatus.APPROVED,
        )

    print(f"\nCreated mock mandate with reference: {reference}")
    return reference


class TestMandatesIntegration:
    """Integration tests for mandates data source."""

    def test_read_all_mandates(
        self,
        has_credentials: bool,
        mock_mandate_reference: str | None,
        options: dict[str, str],
    ) -> None:
        """Test reading all mandates from sandbox."""
        if not has_credentials:
            pytest.skip("Sandbox credentials not available")

        df = pf.scan_mandates(**options).collect()

        assert "reference_number" in df.columns
        assert "status" in df.columns
        assert "ean" in df.columns
        assert "energy_type" in df.columns

        count = df.height
        print(f"\nFound {count} mandates")
        assert count > 0, "Expected at least one mandate (the mock we created)"
        print(df)

    def test_read_approved_mandates(
        self,
        has_credentials: bool,
        mock_mandate_reference: str | None,
        options: dict[str, str],
    ) -> None:
        """Test reading only approved mandates."""
        if not has_credentials:
            pytest.skip("Sandbox credentials not available")

        df = pf.scan_mandates(**{**options, "status": "Approved"}).collect()

        count = df.height
        print(f"\nFound {count} approved mandates")
        assert count > 0, "Expected at least one approved mandate"
        print(df)

        for status in df["status"].to_list():
            assert status == "Approved", f"Expected Approved but got {status}"

    def test_read_mandate_by_reference(
        self,
        has_credentials: bool,
        mock_mandate_reference: str | None,
        options: dict[str, str],
    ) -> None:
        """Test reading a specific mandate by reference number."""
        if not has_credentials:
            pytest.skip("Sandbox credentials not available")

        df = pf.scan_mandates(
            **{**options, "reference_number": mock_mandate_reference}
        ).collect()

        count = df.height
        print(f"\nFound {count} mandates with reference {mock_mandate_reference}")
        assert count == 1, f"Expected exactly 1 mandate, got {count}"

        row = df.row(0, named=True)
        assert row["reference_number"] == mock_mandate_reference
        assert row["ean"] == TEST_EAN
        assert row["status"] == "Approved"


class TestEnergyIntegration:
    """Integration tests for energy data source."""

    def test_read_energy_data(
        self,
        has_credentials: bool,
        mock_mandate_reference: str | None,
        options: dict[str, str],
    ) -> None:
        """Test reading energy data from sandbox."""
        if not has_credentials:
            pytest.skip("Sandbox credentials not available")

        print(f"\nUsing EAN: {TEST_EAN}, reference: {mock_mandate_reference}")

        energy_df = pf.scan_energy(
            **{
                **options,
                "ean": TEST_EAN,
                "reference_number": mock_mandate_reference,
                "period_type": "readTime",
                "granularity": "daily",
                "from_date": "2024-01-01",
                "to_date": "2024-01-10",
            }
        ).collect()

        for col in (
            "ean",
            "energy_type",
            "start",
            "direction",
            "register_type",
            "value",
        ):
            assert col in energy_df.columns

        count = energy_df.height
        print(f"\nFound {count} energy measurements")

        if count > 0:
            print(
                energy_df.select(
                    "ean",
                    "energy_type",
                    "start",
                    "end",
                    "granularity",
                    "direction",
                    "register_type",
                    "value",
                    "unit",
                )
            )

    def test_read_energy_with_date_range(
        self,
        has_credentials: bool,
        mock_mandate_reference: str | None,
        options: dict[str, str],
    ) -> None:
        """Test reading energy data with a date range filter."""
        if not has_credentials:
            pytest.skip("Sandbox credentials not available")

        energy_df = pf.scan_energy(
            **{
                **options,
                "ean": TEST_EAN,
                "reference_number": mock_mandate_reference,
                "period_type": "readTime",
                "granularity": "daily",
                "from_date": "2024-01-01",
                "to_date": "2024-01-15",
            }
        ).collect()

        count = energy_df.height
        print(f"\nFound {count} energy measurements for Jan 2024")

        if count > 0:
            print(
                energy_df.select(
                    "ean", "start", "end", "direction", "register_type", "value"
                ).head(10)
            )


class TestDataAnalysis:
    """Test data analysis capabilities."""

    def test_aggregate_energy_data(
        self,
        has_credentials: bool,
        mock_mandate_reference: str | None,
        options: dict[str, str],
    ) -> None:
        """Test aggregating energy measurements."""
        if not has_credentials:
            pytest.skip("Sandbox credentials not available")

        energy_df = pf.scan_energy(
            **{
                **options,
                "ean": TEST_EAN,
                "reference_number": mock_mandate_reference,
                "period_type": "readTime",
                "granularity": "daily",
                "from_date": "2024-01-01",
                "to_date": "2024-01-31",
            }
        ).collect()

        if energy_df.height == 0:
            pytest.skip("No energy data available for this EAN")

        offtake_day = energy_df.filter(
            (pl.col("direction") == "offtake") & (pl.col("register_type") == "day")
        )

        stats = offtake_day.select(
            pl.len().alias("count"),
            pl.col("value").sum().alias("total_day_consumption"),
            pl.col("value").mean().alias("avg_day_consumption"),
            pl.col("value").min().alias("min_day_consumption"),
            pl.col("value").max().alias("max_day_consumption"),
        )

        print("\nEnergy consumption statistics (offtake/day):")
        print(stats)

        assert energy_df.height > 0
