# spark_config.py

import logging
import os
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count

class SparkConfig:
    """
    Create and manage Spark sessions connected to Databricks Serverless.

    Authentication is handled through Databricks CLI configuration.
    The default profile is read from DATABRICKS_CONFIG_PROFILE.
    """

    DEFAULT_PROFILE = "ThuanDao"

    @staticmethod
    def create_spark(
        app_name: str = "RetailAnalytics",
        logger: logging.Logger | None = None,
    ) -> SparkSession:
        """
        Create a Spark session connected to Databricks Serverless.

        Parameters
        ----------
        app_name : str
            Application name for identification.
        logger : logging.Logger | None
            Optional logger.

        Returns
        -------
        SparkSession
            Databricks Connect Spark session.

        Raises
        ------
        RuntimeError
            If the Databricks connection cannot be established.
        """

        # Use the provided logger or create a default one
        log = logger or logging.getLogger(__name__)

        try:
            # Get the Databricks CLI profile for authentication
            profile = os.getenv("DATABRICKS_CONFIG_PROFILE", SparkConfig.DEFAULT_PROFILE)

            # Create a Spark session using Databricks Connect and Serverless
            spark = (
                DatabricksSession.builder
                .profile(profile)
                .serverless()
                .getOrCreate()
            )

            # Log connection status and Spark version
            log.info("Connected to Databricks Serverless.")
            log.info("Spark version: %s", spark.version)

            return spark
            # try:
            #     builder = DatabricksSession.builder

            #     host = os.getenv("DATABRICKS_HOST")
            #     token = os.getenv("DATABRICKS_TOKEN")
            #     if host and token:
            #         builder = builder.remote(host=host, token=token)

            #     # Create Spark session via Databricks Connect
            #     spark = builder.getOrCreate()
            #     logger.info("Connected to Databricks via Spark Connect.")
            #     return spark
            # except Exception as e:
            #     logger.error("Databricks connection failed.", exc_info=e)
            #     raise RuntimeError(f"Databricks connection failed: {e}")

        except Exception as exc:
            # Log the error and raise a descriptive exception
            log.exception("Failed to connect to Databricks Serverless.")
            raise RuntimeError(f"Databricks connection failed: {exc}") from exc

    @staticmethod
    def stop_spark(
        spark: SparkSession | None,
        logger: logging.Logger | None = None,
    ) -> None:
        """Stop the Spark session safely."""

        # Use the provided logger or create a default one
        log = logger or logging.getLogger(__name__)

        if spark is not None:
            spark.stop()
            log.info("Spark session stopped.")

    @staticmethod
    def spark_info(df: DataFrame, logger: logging.Logger | None = None, 
                   run_count: bool = True) -> None:
        """
        Log basic profiling information for a Spark DataFrame.

        This method provides lightweight data diagnostics
        for validation and quality assessment.

        Reported Information
        --------------------
        - Total row count (optional)
        - Total column count
        - DataFrame schema
        - Non-null count per column

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            Input Spark DataFrame to be analyzed.

        logger : logging.Logger
            Logger instance for writing profiling output.

        run_count : bool, optional
            Whether to compute total row count.

            Note: This operation triggers a full scan
            and may be expensive on large datasets.

            Default is True.

        Returns
        -------
        None
        """

        # Use the provided logger or create a default one
        log = logger or logging.getLogger(__name__)

        # Log total number of rows (optional due to performance cost)
        if run_count:
            log.info(f"Rows    : {df.count()}")

        # Log total number of columns
        log.info(f"Columns : {len(df.columns)}")

        # Print DataFrame schema
        df.printSchema()

        # Show non-null count for each column
        df.select([
            count(col(c)).alias(c) for c in df.columns
        ]).show(vertical=True)
