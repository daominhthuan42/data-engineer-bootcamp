# io_config.py
from pathlib import Path

"""
Centralized IO configuration.
"""

# Resolve project root based on file location
PROJECT_ROOT        = Path(__file__).resolve().parents[2]

# Local filesystem paths
DATASET_DIR         = (PROJECT_ROOT / "fixtures").as_posix()
LOG_DIR             = (PROJECT_ROOT / "logs").as_posix()

# Unity Catalog volume paths
COMMON              = "/Volumes/fmcg_domain/source/source_data/"
CP_PATH_BRONZE      = "/Volumes/fmcg_domain/bronze/checkpoints/"
CP_PATH_GOLD        = "/Volumes/fmcg_domain/gold/checkpoints/"

# Bronze tables
BRONZE_COMMON       = "fmcg_domain.bronze"

# Silver tables
SILVER_COMMON       = "fmcg_domain.silver"
SILVER_CUSTOMER     = "fmcg_domain.silver.dim_customers"
SILVER_PRODUCTS     = "fmcg_domain.silver.dim_products"
SILVER_GROSS_PRICE  = "fmcg_domain.silver.dim_gross_price"
SILVER_ORDERS       = "fmcg_domain.silver.fact_orders"

# Gold tables
GOLD_COMMON         = "fmcg_domain.gold"
GOLD_CUSTOMER       = "fmcg_domain.gold.dim_customers"
GOLD_PRODUCTS       = "fmcg_domain.gold.dim_products"
GOLD_GROSS_PRICE    = "fmcg_domain.gold.dim_gross_price"
GOLD_ORDERS         = "fmcg_domain.gold.fact_orders"

GOLD_SB_CUSTOMER    = "fmcg_domain.gold.sb_dim_customers"
GOLD_SB_PRODUCTS    = "fmcg_domain.gold.sb_dim_products"
GOLD_SB_GROSS_PRICE = "fmcg_domain.gold.sb_dim_gross_price"
GOLD_SB_ORDERS      = "fmcg_domain.gold.sb_fact_orders"


# S3 base
S3_BASE_PATH        = f"s3://00-sportsbar-dp"
