from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import logging
from tabulate import tabulate

def check_null(df: DataFrame, logger: logging.Logger) -> DataFrame:
    """
    Display and log missing value statistics for a given dataset.

    This function checks for missing values (including blank strings),
    summarizes missing counts and percentages per feature, and reports
    the results using logging and formatted tables when available.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataset to be inspected for missing values.

    logger : logging.Logger
        Logger instance for recording inspection results.

    Returns
    -------
    None
        Prints missing value summary and logs overall statistics.
    """

    total_rows = df.count()

    # Build expressions to count NULL and blank values
    exprs = [
        F.sum(
            F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)
        ).alias(c)
        for c in df.columns
    ]

    null_counts = df.select(exprs)

    # Convert to pandas for easier formatting
    pdf = null_counts.toPandas().T.reset_index()
    pdf.columns = ["Features", "Missing_Count"]

    # Keep only columns with missing values
    pdf = pdf[pdf["Missing_Count"] > 0]

    # Calculate percentage
    pdf["Missing_%"] = (pdf["Missing_Count"] / total_rows * 100).round(2)

    # Sort
    pdf = pdf.sort_values("Missing_Count", ascending=False)
    total_missing = pdf["Missing_Count"].sum()

    if total_missing == 0:
        logger.info(f"No missing values detected in {total_rows:,} rows.")
        return

    try:
        table_str = tabulate(
            pdf,
            headers="keys",
            tablefmt="pretty",
            showindex=False
        )
        logger.warning("\n" + table_str)

    except ImportError:
        logger.warning(pdf.to_string(index=False))

    logger.warning(
        f"Total missing values: {total_missing:,} out of {total_rows:,} rows."
    )

def check_duplicate(df: DataFrame, logger: logging.Logger) -> None:
    """
    Check and log duplicate rows in a dataset.

    This function identifies fully duplicated rows in the input DataFrame
    and reports the results using the configured logger.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataset to be checked for duplicate rows.

    logger : logging.Logger
        Logger instance for recording inspection results.

    Returns
    -------
    None
        Logs duplicate statistics to the console or log file.
    """

    total_rows = df.count()
    distinct_rows = df.distinct().count()

    duplicates_count = total_rows - distinct_rows
    if duplicates_count == 0:
        logger.info(f"No duplicate rows found out of {total_rows:,} total rows.")
    else:
        logger.warning(f"{duplicates_count:,} duplicate rows detected ({duplicates_count/total_rows:.2%}).")
        logger.warning(f"Rows affected: {duplicates_count:,} out of {total_rows:,}.")

def checking_outlier(df: DataFrame, dataset_name: str, list_feature: list[str], 
                     logger: logging.Logger) -> None:
    """
    Detect and report outliers using the IQR (Interquartile Range) method.

    This function identifies outliers for specified numerical features
    based on the 1.5 × IQR rule, logs detailed statistics, and returns
    a summary table for reporting and monitoring purposes.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataset to be inspected for outliers.

    dataset_name : str
        Name of the dataset (used for logging context).

    list_feature : list
        List of numerical feature names to be checked.

    logger : logging.Logger, optional
        Logger instance for recording inspection results.

    Returns
    -------
    None
    """

    logger.info(f"Checking outliers for {dataset_name}...")
    logger.info("-" * 80)

    outlier_info = []

    for feature in list_feature:

        # Compute quartiles
        q1, q3 = df.approxQuantile(feature, [0.25, 0.75], 0.01)
        iqr = q3 - q1

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        # Detect outliers
        outlier_count = df.filter(
            (F.col(feature) < lower_bound) |
            (F.col(feature) > upper_bound)
        ).count()

        if outlier_count == 0:
            logger.debug(f"[{dataset_name}] {feature}: No outliers detected")
        else:
            logger.warning(
                f"[{dataset_name}] {feature}: {outlier_count:,} outliers detected "
                f"(lower={lower_bound:.3f}, upper={upper_bound:.3f})"
            )

            outlier_info.append({
                "Feature": feature,
                "Outlier_Count": outlier_count
            })

    if not outlier_info:
        logger.info(f"[{dataset_name}] No outliers detected across all features.")
    else:
        logger.info(
            f"[{dataset_name}] Outliers detected in {len(outlier_info)} features."
        )

    logger.info("-" * 80)
    logger.info("\s" + outlier_info)

def validate_not_null(df: DataFrame, columns: list[str], 
                      logger: logging.Logger) -> DataFrame:
    """
    Identify rows that contain NULL values in specified columns.

    This function builds a condition that checks whether any of the
    given columns contains a NULL value. Rows that satisfy this condition
    are returned for further inspection or data quality validation.

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame to be validated.

    columns : list[str]
        List of column names that must not contain NULL values.

    logger : logging.Logger, optional
        Logger instance used to record validation results.

    Returns
    -------
    DataFrame
        A Spark DataFrame containing rows where at least one
        of the specified columns has a NULL value.
    """

    logger.info("Starting NULL validation check...")
    logger.info(f"Columns to validate: {columns}")

    condition = None

    # Build OR condition to detect NULL values across all specified columns
    for c in columns:
        if condition is None:
            # First condition initialization
            condition = F.col(c).isNull()
        else:
            # Combine conditions using OR
            condition = condition | F.col(c).isNull()

    # Filter rows that contain NULL values
    null_rows = df.filter(condition)

    # Count number of affected rows for logging
    null_count = null_rows.count()

    if null_count == 0:
        logger.info("No NULL values detected in specified columns.")
    else:
        logger.warning(f"{null_count:,} rows contain NULL values in the specified columns.")

    return null_rows
