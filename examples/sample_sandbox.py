#!/usr/bin/env python3
"""Sandbox usage example for polars-fluvius.

This example demonstrates how to use the Fluvius IO sources to read energy
and mandate data into Polars DataFrames using the sandbox environment.

Sandbox uses client secret authentication (simpler than certificate-based).

Required environment variables:
    # Common credentials
    export FLUVIUS_SUBSCRIPTION_KEY="your-subscription-key"
    export FLUVIUS_TENANT_ID="your-tenant-id"
    export FLUVIUS_SCOPE="api://your-scope/.default"
    export FLUVIUS_DATA_ACCESS_CONTRACT_NUMBER="your-contract-number"

    # Sandbox-specific (uses client_secret instead of certificate)
    export FLUVIUS_SANDBOX_CLIENT_ID="your-sandbox-client-id"
    export FLUVIUS_SANDBOX_CLIENT_SECRET="your-sandbox-client-secret"
"""

import os
import uuid

import polars as pl

import polars_fluvius as pf

# Sandbox test EAN - must match pattern: 5414[4-5][0-1][0-2][0-2][0-2]000000[0-9][0-9][0-9]
TEST_EAN = "541450000000000001"


def get_sandbox_options() -> dict[str, str]:
    """Get options for sandbox environment."""
    return {
        "subscription_key": os.environ["FLUVIUS_SUBSCRIPTION_KEY"],
        "client_id": os.environ["FLUVIUS_SANDBOX_CLIENT_ID"],
        "tenant_id": os.environ["FLUVIUS_TENANT_ID"],
        "scope": os.environ["FLUVIUS_SCOPE"],
        "data_access_contract_number": os.environ["FLUVIUS_DATA_ACCESS_CONTRACT_NUMBER"],
        "client_secret": os.environ["FLUVIUS_SANDBOX_CLIENT_SECRET"],
        "environment": "sandbox",
    }


def create_mock_mandate(reference: str) -> None:
    """Create a mock mandate in the sandbox for testing."""
    from fluvius_energy_api import FluviusCredentials, FluviusEnergyClient
    from fluvius_energy_api.client import Environment
    from fluvius_energy_api.models.enums import DataServiceType, MandateStatus

    credentials = FluviusCredentials(
        subscription_key=os.environ["FLUVIUS_SUBSCRIPTION_KEY"],
        client_id=os.environ["FLUVIUS_SANDBOX_CLIENT_ID"],
        tenant_id=os.environ["FLUVIUS_TENANT_ID"],
        scope=os.environ["FLUVIUS_SCOPE"],
        data_access_contract_number=os.environ["FLUVIUS_DATA_ACCESS_CONTRACT_NUMBER"],
        client_secret=os.environ["FLUVIUS_SANDBOX_CLIENT_SECRET"],
    )

    with FluviusEnergyClient(credentials=credentials, environment=Environment.SANDBOX) as client:
        client.create_mock_mandate(
            reference_number=reference,
            ean=TEST_EAN,
            data_service_type=DataServiceType.VH_DAG,
            data_period_from="2024-01-01T00:00:00Z",
            data_period_to="2026-12-31T23:59:59Z",
            status=MandateStatus.APPROVED,
        )
    print(f"Created mock mandate with reference: {reference}")


def main() -> None:
    """Run the sandbox example."""
    print("=" * 60)
    print("Polars Fluvius - Sandbox Example")
    print("=" * 60)

    reference = f"polars-example-{uuid.uuid4().hex[:8]}"

    print("\n1. Creating mock mandate...")
    create_mock_mandate(reference)

    options = get_sandbox_options()

    print("\n" + "=" * 60)
    print("2. Reading all mandates...")
    print("=" * 60)

    mandates_df = pf.scan_mandates(**options).collect()
    print(f"Found {mandates_df.height} mandates")
    print(
        mandates_df.select(
            "reference_number",
            "ean",
            "status",
            "energy_type",
            "data_service_type",
        )
    )

    print("=" * 60)
    print("3. Reading approved mandates...")
    print("=" * 60)

    approved_df = pf.scan_mandates(**{**options, "status": "Approved"}).collect()
    print(f"Found {approved_df.height} approved mandates")
    print(approved_df)

    print("=" * 60)
    print("4. Reading energy data...")
    print("=" * 60)

    energy_df = pf.scan_energy(
        **{
            **options,
            "ean": TEST_EAN,
            "reference_number": reference,
            "period_type": "readTime",
            "granularity": "daily",
            "from_date": "2024-01-01",
            "to_date": "2024-01-10",
        }
    ).collect()

    print(f"Found {energy_df.height} energy measurements")
    print(energy_df)

    print("=" * 60)
    print("5. Showing reactive energy measurements...")
    print("=" * 60)

    print(
        energy_df.filter(pl.col("register_type").is_in(["reactive", "inductive", "capacitive"]))
        .select("ean", "start", "direction", "register_type", "value", "unit")
    )

    print("=" * 60)
    print("6. Analyzing energy consumption...")
    print("=" * 60)

    if energy_df.height > 0:
        offtake_day = energy_df.filter(
            (pl.col("direction") == "offtake") & (pl.col("register_type") == "day")
        )
        stats = offtake_day.select(
            pl.len().alias("measurements"),
            pl.col("value").sum().alias("total_day_consumption"),
            pl.col("value").mean().alias("avg_day_consumption"),
            pl.col("value").min().alias("min_day_consumption"),
            pl.col("value").max().alias("max_day_consumption"),
        )
        print(stats)

    print("=" * 60)
    print("Sandbox example complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()