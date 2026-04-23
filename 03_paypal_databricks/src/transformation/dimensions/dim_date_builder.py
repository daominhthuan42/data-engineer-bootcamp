import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_dim_date(
    spark: SparkSession,
    start_date: str,
    end_date: str,
    table_name: str,
    logger: logging.Logger
) -> None:
    """
    Create and populate a Date Dimension table.

    This function generates a continuous range of dates between the
    specified start and end dates and derives commonly used calendar
    attributes such as year, month, quarter, week, and day information.

    The resulting dataset is written to a Delta table.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    start_date : str
        Start date of the calendar range in format 'yyyy-MM-dd'.

    end_date : str
        End date of the calendar range in format 'yyyy-MM-dd'.

    table_name : str
        Fully qualified Delta table name (e.g. catalog.schema.table).

    logger : logging.Logger
        Logger instance used for pipeline logging.

    Returns
    -------
    None

    Notes
    -----
    - The table is written using Delta Lake format.
    - Existing data will be overwritten.
    - Date key is generated as an integer in format YYYYMMDD.
    """

    try:
        logger.info(f"Generating dim_date from {start_date} to {end_date}")

        # Generate a continuous sequence of dates using Spark SQL
        # sequence() creates an array of dates
        # explode() converts the array into rows
        df = spark.sql(f"""
            SELECT explode(
                sequence(
                    to_date('{start_date}'),
                    to_date('{end_date}'),
                    interval 1 month
                )
            ) AS date
        """)

        # Derive calendar attributes for the date dimension
        dim_date = (
            df
            .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))
            .withColumn("year", year(col("date")))
            .withColumn("month", month(col("date")))
            .withColumn("day", dayofmonth(col("date")))
            .withColumn("quarter", quarter(col("date")))
            .withColumn("week_of_year", weekofyear(col("date")))
            .withColumn("day_of_week", dayofweek(col("date")))
            .withColumn("day_name", date_format(col("date"), "EEEE"))
            .withColumn("month_name", date_format(col("date"), "MMMM"))
            .withColumn("month_short_name", date_format(col("date"), "MMM"))
            .withColumn("quarter", concat(lit("Q"), quarter("date")))
            .withColumn("year_quarter", concat(col("year"), lit("-Q"), quarter("date")))
        )

        logger.info("Calendar attributes successfully generated")

        # Write dimension table to Delta Lake
        # Overwrite mode ensures a clean rebuild of the table
        dim_date.write.format("delta").mode("overwrite").saveAsTable(table_name)

        logger.info(f"dim_date successfully written to table: {table_name}")

    except Exception as e:
        logger.exception(f"Failed to create dim_date table: {table_name} | Error: {str(e)}")
        raise
