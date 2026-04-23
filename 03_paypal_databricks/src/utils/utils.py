# utils.py
import logging
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from delta.tables import DeltaTable

def parse_timestamp(col: Column, fmt: Optional[str] = "yyyy-MM-dd HH:mm:ss", 
                    remove_utc: bool = True) -> Column:
    """
    Safely parse a timestamp column with optional UTC suffix removal.

    Parameters
    ----------
    col : Column
        Input column

    fmt : str, optional
        Timestamp format (default: yyyy-MM-dd HH:mm:ss)

    remove_utc : bool
        Whether to remove ' UTC' suffix

    Returns
    -------
    Column
        Parsed timestamp column (NULL if parsing fails)
    """

    if remove_utc:
        col = F.regexp_replace(col, " UTC", "")

    # 2024-03-17T19:51:15.791Z
    # 2024-03-07 04:04:26.743750 UTC

    return F.try_to_timestamp(F.substring(col, 1, 19), F.lit(fmt))

def parse_timestamp2(col: Column) -> Column:
    """
    Parse multiple timestamp formats using try_to_timestamp (Spark older version).
    """

    return F.coalesce(
        # ISO format with milliseconds + Z
        F.try_to_timestamp(col, F.lit("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")),

        # ISO without milliseconds
        F.try_to_timestamp(col, F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'")),

        # Format with UTC suffix
        F.try_to_timestamp(
            F.regexp_replace(col, " UTC", ""),
            F.lit("yyyy-MM-dd HH:mm:ss.SSSSSS")
        ),

        # ISO format
        F.to_date(F.try_to_timestamp(col, F.lit("yyyy-MM-dd"))),

        # US format (3/7/2024)
        F.to_date(F.try_to_timestamp(col, F.lit("M/d/yyyy"))),

        # padded format
        F.to_date(F.try_to_timestamp(col, F.lit("MM/dd/yyyy"))),

        # Fallback format
        F.try_to_timestamp(col, F.lit("yyyy-MM-dd HH:mm:ss"))
    )

def parse_decimal(col: Column, precision: int = 18, scale: int = 2) -> Column:
    """
    Safely cast a column to decimal.

    Parameters
    ----------
    col : Column
        Input column

    precision : int
        Total number of digits

    scale : int
        Number of decimal places

    Returns
    -------
    Column
        Decimal column (NULL if casting fails)
    """

    return col.cast(f"decimal({precision},{scale})")

def parse_json_field(col: Column, path: str) -> Column:
    """
    Extract a field from a JSON string column.

    Parameters
    ----------
    col : Column
        JSON string column

    path : str
        JSON path (e.g., '$.currency_code')

    Returns
    -------
    Column
        Extracted field as string (NULL if not found)
    """

    return F.get_json_object(col, path)

def dedup(
    df: DataFrame,
    dedup_cols: List[str],
    order_cols: List[str],
    order_desc: Optional[List[bool]] = None,
    nulls_last: bool = True,
    logger: Optional[logging.Logger] = None
) -> DataFrame:
    """
    Deduplicate a Spark DataFrame by business key and retain a single record per group
    based on configurable multi-column ordering.

    This function applies a window function (ROW_NUMBER) over partitions defined by
    `dedup_cols`, and orders rows within each partition using `order_cols` with
    customizable sort direction and NULL handling. Only the top-ranked row
    (row_number = 1) is kept for each partition.

    Behavior:
    - Partitions data by `dedup_cols` (business key)
    - Orders rows within each partition using `order_cols`
    - Supports per-column sort direction via `order_desc`
    - Controls NULL placement using `nulls_last`
    - Returns one record per key (latest or highest priority)

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame.

    dedup_cols : List[str]
        List of columns used as the deduplication key (equivalent to PARTITION BY).

    order_cols : List[str]
        List of columns used to determine record priority (equivalent to ORDER BY).
        Priority is evaluated from left to right.

    order_desc : Optional[List[bool]], default = None
        List of booleans indicating sort direction for each column in `order_cols`.
        - True  → DESC
        - False → ASC
        If None, all columns are sorted in DESC order.

    nulls_last : bool, default = True
        Controls NULL placement in ordering:
        - True  → NULL values are placed last
        - False → NULL values are placed first

    logger : Optional[logging.Logger], default = None
        Logger instance for recording execution details such as input/output counts
        and configuration parameters.

    Returns
    -------
    DataFrame
        Deduplicated DataFrame containing one record per unique key defined by `dedup_cols`.

    Notes
    -----
    - Equivalent SQL pattern:

        ROW_NUMBER() OVER (
            PARTITION BY <dedup_cols>
            ORDER BY <order_cols>
        ) = 1

    - It is recommended to include a tie-breaker column (e.g., ingestion timestamp)
    in `order_cols` to ensure deterministic results when ordering columns contain duplicates.

    - Using `nulls_last=True` is recommended for "latest record" use cases to avoid
    NULL values being incorrectly selected as highest priority.

    - This function does not modify input schema except for temporary internal columns,
    which are removed before returning the result.
    """

    if logger:
        logger.info("Starting deduplication")
        logger.info("Dedup columns: %s", dedup_cols)
        logger.info("Order columns: %s", order_cols)

    # =========================
    # Validation
    # =========================
    if not dedup_cols:
        raise ValueError("dedup_cols must not be empty")

    if not order_cols:
        raise ValueError("order_cols must not be empty")

    if order_desc is None:
        order_desc = [True] * len(order_cols)

    if len(order_cols) != len(order_desc):
        raise ValueError("order_cols and order_desc must have same length")

    if logger:
        logger.info("Order direction (desc): %s", order_desc)
        logger.info("Nulls last: %s", nulls_last)

    # =========================
    # Input count
    # =========================
    if logger:
        input_count = df.count()
        logger.info("Input row count: %s", input_count)

    # =========================
    # Build order expressions
    # =========================
    order_exprs = []
    for col_name, desc in zip(order_cols, order_desc):
        c = F.col(col_name)

        if desc:
            expr = c.desc_nulls_last() if nulls_last else c.desc_nulls_first()
        else:
            expr = c.asc_nulls_last() if nulls_last else c.asc_nulls_first()

        order_exprs.append(expr)

    # =========================
    # Window
    # =========================
    window_spec = Window.partitionBy(*dedup_cols).orderBy(*order_exprs)

    # =========================
    # Dedup
    # =========================
    df_out = (
        df.withColumn("_rn", F.row_number().over(window_spec))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )

    # =========================
    # Output count
    # =========================
    if logger:
        output_count = df_out.count()
        logger.info("Output row count after dedup: %s", output_count)
        logger.info("Removed duplicate rows: %s", input_count - output_count)
        logger.info("Deduplication completed")

    return df_out

def process_timestamp(df: DataFrame) -> None:
    """
    Add processing timestamp to track when the record is transformed.

    This column is used for auditing, debugging, and data lineage
    to identify when the data was processed in the pipeline.
    """

    return (df.withColumn("process_timestamp", F.current_timestamp()))

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
