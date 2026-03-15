# io_config.py

from pathlib import Path


class IOConfig:
    """
    Centralized IO configuration.

    Paths are resolved relative to the project root,
    independent of the current working directory.
    """

    # Resolve project root dynamically
    # io_config.py -> src/ -> project_root/
    PROJECT_ROOT = Path(__file__).resolve().parents[1]

    # Local filesystem paths
    DATASET_DIR = (PROJECT_ROOT / "fixtures").as_posix()
    LOG_DIR     = (PROJECT_ROOT / "logs").as_posix()

    # Unity Catalog volume paths
    COMMON        = "/Volumes/databricks_toturial/source/source_data/"
    CP_PATH       = "/Volumes/databricks_toturial/bronze/checkpoints/"

    # Bronze tables
    BRONZE_COMMON      = "databricks_toturial.bronze"
    BRONZE_CUSTOMERS   = "databricks_toturial.bronze.customers"
    BRONZE_TRIPS       = "databricks_toturial.bronze.trips"
    BRONZE_DRIVERS     = "databricks_toturial.bronze.drivers"
    BRONZE_PAYMENTS    = "databricks_toturial.bronze.payments"
    BRONZE_VEHICLES    = "databricks_toturial.bronze.vehicles"
    BRONZE_LOCATIONS   = "databricks_toturial.bronze.locations"

    # Silver tables
    SILVER_CUSTOMER    = "databricks_toturial.silver.customers"
    SILVER_TRIPS       = "databricks_toturial.silver.trips"
    SILVER_DRIVERS     = "databricks_toturial.silver.drivers"
    SILVER_PAYMENTS    = "databricks_toturial.silver.payments"
    SILVER_VEHICLES    = "databricks_toturial.silver.vehicles"
    SILVER_LOCATIONS   = "databricks_toturial.silver.locations"

    # Gold tables
    GOLD_CUSTOMER      = "databricks_toturial.gold.dim_customers"
    GOLD_TRIPS         = "databricks_toturial.gold.fact_trips"
    GOLD_DRIVERS       = "databricks_toturial.gold.dim_drivers"
    GOLD_PAYMENTS      = "databricks_toturial.gold.dim_payments"
    GOLD_VEHICLES      = "databricks_toturial.gold.dim_vehicles"
    GOLD_LOCATIONS     = "databricks_toturial.gold.dim_locations"
