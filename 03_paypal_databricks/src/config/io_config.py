# io_config.py
from pathlib import Path

"""
Centralized IO configuration.
"""

# Resolve project root based on file location
PROJECT_ROOT                        = Path(__file__).resolve().parents[2]

# Local filesystem paths
DATASET_DIR                         = (PROJECT_ROOT / "fixtures").as_posix()
LOG_DIR                             = (PROJECT_ROOT / "logs").as_posix()

SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# Unity Catalog volume paths
COMMON                              = "/Volumes/paypal_analytic/source/source_data/"
CP_PATH_BRONZE                      = "/Volumes/paypal_analytic/bronze/checkpoints/"

# Bronze tables
BRONZE_COMMON                       = "paypal_analytic.bronze"
BRONZE_TRANSACTIONS                 = "paypal_analytic.bronze.transactions"
BRONZE_DISPUTE_TRANSACTIONS         = "paypal_analytic.bronze.dispute_transactions"

# Silver schema
SILVER_PATH_COMMON                       = "paypal_analytic.silver"
SILVER_PATH_DISPUTED_PP01_TRANSACTIONS   = "paypal_analytic.silver.disputed_pp01_transactions"
SILVER_PATH_DISPUTED_PP01_PAYER          = "paypal_analytic.silver.disputed_pp01_payer"
SILVER_PATH_DISPUTED_PP01_SHIPPING       = "paypal_analytic.silver.disputed_pp01_shipping"
SILVER_PATH_DISPUTED_PP01_CART           = "paypal_analytic.silver.disputed_pp01_cart"
SILVER_PATH_DISPUTED_PP01_INCENTIVE      = "paypal_analytic.silver.disputed_pp01_incentive"

SILVER_TABLE_DISPUTED_PP01_TRANSACTIONS   = "disputed_pp01_transactions"
SILVER_TABLE_DISPUTED_PP01_PAYER          = "disputed_pp01_payer"
SILVER_TABLE_DISPUTED_PP01_SHIPPING       = "disputed_pp01_shipping"
SILVER_TABLE_DISPUTED_PP01_CART           = "disputed_pp01_cart"
SILVER_TABLE_DISPUTED_PP01_INCENTIVE      = "disputed_pp01_incentive"

SILVER_PATH_DISPUTED_PP02_DISPUTES        = "paypal_analytic.silver.disputed_pp02_disputes"
SILVER_PATH_DISPUTED_PP02_DISPUTES_TRANSACTIONS        = "paypal_analytic.silver.disputed_pp02_disputed_transactions"
SILVER_PATH_DISPUTED_PP02_DISPUTES_OUTCOME        = "paypal_analytic.silver.disputed_pp02_disputed_outcome"

SILVER_TABLE_DISPUTED_PP02_DISPUTES       = "disputed_pp02_disputes"
SILVER_TABLE_DISPUTED_PP02_DISPUTES_TRANSACTIONS       = "disputed_pp02_disputed_transactions"
SILVER_TABLE_DISPUTED_PP02_DISPUTES_OUTCOME       = "disputed_pp02_disputed_outcome"

# S3 base
S3_BASE_PATH                        = f"s3://01-paypal-analytics"
