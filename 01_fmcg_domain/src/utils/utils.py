# utils.py
import logging
from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from delta.tables import DeltaTable

def dedup(df: DataFrame, dedup_cols: List, cdc: str, logger: logging.Logger) -> DataFrame:
    """
    Remove duplicate records and retain the latest record for each key group.

    This function performs the following steps:
    - Generate a deduplication key by concatenating the specified columns
    - Partition the dataset by the generated deduplication key
    - Apply a window function to rank records using the CDC column in descending order
    - Retain only the most recent record (row_number = 1) within each partition
    - Remove temporary helper columns used during the deduplication process

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame containing raw records.

    dedup_cols : List
        List of column names used to construct the deduplication key.

    cdc : str
        Column used for ordering records to determine the most recent entry
        (e.g., last_updated_timestamp or ingestion_timestamp).

    logger : logging.Logger
        Application logger used to record execution information.

    Returns
    -------
    DataFrame
        Deduplicated Spark DataFrame containing only the latest record per key.
    """

    logger.info("Starting deduplication")
    logger.info("Dedup columns: %s | CDC column: %s", dedup_cols, cdc)

    input_count = df.count()
    logger.info("Input row count: %s", input_count)

    # Create a temporary deduplication key by concatenating the dedup columns
    df = df.withColumn("dedupKey", concat(*dedup_cols))

    # Assign row numbers within each dedupKey partition
    # Ordering is based on CDC column (latest record first)
    df = df.withColumn("dedupCounts", row_number() \
                            .over(Window.partitionBy("dedupKey") \
                            .orderBy(col(cdc).desc()))
                        )

    # Keep only the latest record per dedupKey (row_number = 1)
    df = df.filter(df.dedupCounts == 1)

    # Count output rows after deduplication
    output_count = df.count()
    logger.info("Output row count after dedup: %s", output_count)

    # Log number of removed duplicate records
    logger.info("Removed duplicate rows: %s", input_count - output_count)

    # Drop temporary helper columns used for deduplication
    df = df.drop("dedupKey", "dedupCounts")

    logger.info("Deduplication completed")

    return df

def process_timestamp(df: DataFrame) -> None:
    """
    Add processing timestamp to track when the record is transformed.

    This column is used for auditing, debugging, and data lineage
    to identify when the data was processed in the pipeline.
    """

    return (df.withColumn("process_timestamp", current_timestamp()))

def upsert(spark: SparkSession, df: DataFrame, key_cols: List, table: str, cdc: str, name_catalog: str, 
           name_schema: str, logger: logging) -> None:
    """
    Perform a CDC-based UPSERT (MERGE) operation into a Delta Lake table.

    This function merges records from a source Spark DataFrame into a
    target Delta table using the specified key columns. Existing records
    are updated only when the source CDC column value is newer than or
    equal to the target. New records are inserted when no match is found.

    The merge logic follows these rules:

    - MATCHED rows:
        Update all columns if src.<cdc> >= trg.<cdc>

    - NOT MATCHED rows:
        Insert all columns

    All operations are logged using the provided logger. Any exception
    occurring during execution is logged with full stack trace and
    re-raised to the caller.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Active Spark session used to access catalog metadata and execute
        the Delta Lake merge operation.

    df : pyspark.sql.DataFrame
        Source DataFrame containing new or updated records.

    key_cols : List[str]
        List of column names used as merge keys. These columns must
        uniquely identify records in the target table.

    table : str
        Target Delta table name (without catalog and schema).

    cdc : str
        Name of the CDC (Change Data Capture) column used to determine
        record freshness (e.g., updated_at, last_modified_ts).

    name_catalog : str
        Catalog name containing the target Delta table.

    name_schema : str
        Schema (database) name containing the target Delta table.

    logger : logging.Logger
        Configured logger instance used for logging execution status
        and error details.

    Returns
    -------
    None
        This function does not return a value. It raises an exception
        if the merge operation fails.

    Raises
    ------
    Exception
        Propagates any exception raised during Delta merge execution
        after logging the error details.

    Examples
    --------
    >>> upsert(
    ...     spark=spark,
    ...     df=source_df,
    ...     key_cols=["id", "email"],
    ...     table="customer",
    ...     cdc="updated_at",
    ...     name_catalog="main",
    ...     name_schema="silver",
    ...     logger=logger
    ... )
    """

    target_table = f"{name_catalog}.{name_schema}.{table}"
    try:
        logger.info(f"Starting UPSERT into {target_table}")

        # Validate Delta format
        detail = spark.sql(f"DESCRIBE DETAIL {target_table}").collect()[0]
        if detail.format != "delta":
            raise ValueError(f"{target_table} exists but is not a Delta table")

        # Validate columns
        source_cols = set(df.columns)
        missing_keys = [c for c in key_cols if c not in source_cols]
        if missing_keys:
            raise ValueError(f"Missing key columns in source DataFrame: {missing_keys}")

        if cdc not in source_cols:
            raise ValueError(f"CDC column '{cdc}' not found in source DataFrame")

        # Build merge condition using key columns
        # Example: src.id = trg.id AND src.email = trg.email
        merge_condition = " AND ".join([f"src.{i} = trg.{i}" for i in key_cols])

        # Load target Delta table
        dlt_obj = DeltaTable.forName(df.sparkSession, target_table)

        # Merge source DataFrame into target table
        # - Update all columns when matched
        # - Insert all columns when not matched
        dlt_obj.alias("trg").merge(df.alias("src"), merge_condition) \
                            .whenMatchedUpdateAll(condition = f"src.{cdc} >= trg.{cdc}") \
                            .whenNotMatchedInsertAll() \
                            .execute()
        logger.info(f"UPSERT completed successfully: {target_table}")
    except Exception as e:
        logger.exception(f"UPSERT FAILED on {table}: {str(e)}")
        raise
