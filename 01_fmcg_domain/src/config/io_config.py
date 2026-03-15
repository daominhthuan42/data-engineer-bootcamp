# io_config.py
from pathlib import Path

class IOConfig:
    """
    Centralized IO configuration.
    """

    # Resolve project root based on file location
    PROJECT_ROOT = Path(__file__).resolve().parents[2]

    # Local filesystem paths
    DATASET_DIR = (PROJECT_ROOT / "fixtures").as_posix()
    LOG_DIR = (PROJECT_ROOT / "logs").as_posix()

    # Unity Catalog volume paths
    COMMON = "/Volumes/fmcg_domain/source/source_data/"
    CP_PATH_BRONZE = "/Volumes/fmcg_domain/bronze/checkpoints/"
    CP_PATH_GOLD = "/Volumes/fmcg_domain/gold/checkpoints/"

    # Gold tables
    GOLD_COMMON = "fmcg_domain.gold"
    GOLD_CUSTOMER = "fmcg_domain.gold.dim_customers"
    GOLD_TRIPS = "fmcg_domain.gold.fact_trips"
    GOLD_DRIVERS = "fmcg_domain.gold.dim_drivers"
    GOLD_PAYMENTS = "fmcg_domain.gold.dim_payments"
    GOLD_VEHICLES = "fmcg_domain.gold.dim_vehicles"
    GOLD_LOCATIONS = "fmcg_domain.gold.dim_locations"
