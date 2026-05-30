# spark_config.py
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count
from config.settings import CONFIGURATION

class SparkConfig:
    """
    Utility class for creating and managing SparkSession.

    This class centralizes Spark configuration and initialization
    logic for local Spark environments with Delta Lake support.
    """

    # Core Config
    MASTER = os.getenv("SPARK_MASTER", "local[*]")

    # Spark Config
    SPARK_CONF = {
        "spark.sql.shuffle.partitions": "200",
        # Kafka
        "spark.jars.packages": ",".join([
            # Spark Kafka Connector
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2",
            # Kafka dependency
            "org.apache.kafka:kafka-clients:3.5.1",
            # AWS S3 support
            "org.apache.hadoop:hadoop-aws:3.3.4",
            # AWS SDK
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ]),
        # Streaming
        "spark.streaming.stopGracefullyOnShutdown": "true",
        # S3A Filesystem
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        # AWS Credentials
        "spark.hadoop.fs.s3a.access.key": CONFIGURATION.get("AWS_ACCESS_KEY"),
        "spark.hadoop.fs.s3a.secret.key": CONFIGURATION.get("AWS_SECRET_KEY"),
        # Credentials Provider
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    }

    @staticmethod
    def create_spark(
        logger: logging.Logger,
        app_name: str = "SmartCitySreaming"
    ) -> SparkSession:
        """
        Create and return a local SparkSession with Delta Lake support.
        """

        try:
            builder = (
                SparkSession.builder
                .appName(app_name)
                .master(SparkConfig.MASTER)
            )

            for k, v in SparkConfig.SPARK_CONF.items():
                builder = builder.config(k, v)

            spark = builder.getOrCreate()

            logger.info("Running Spark in local mode.")

            spark.sparkContext.setLogLevel("WARN")

            return spark

        except Exception as e:
            logger.error("Failed to initialize Spark session.", exc_info=e)

            raise RuntimeError(f"Spark initialization failed: {e}")

    @staticmethod
    def stop_spark(
        spark: SparkSession,
        logger: logging.Logger
    ) -> None:
        """
        Stop Spark session safely.
        """

        if spark is not None:
            spark.stop()
        logger.info("Spark session stopped.")

    @staticmethod
    def spark_info(
        df: DataFrame,
        logger: logging.Logger,
        run_count: bool = True
    ) -> None:
        """
        Log basic profiling information for a Spark DataFrame.
        """

        if run_count and logger:
            logger.info(f"Rows    : {df.count()}")

        logger.info(f"Columns : {len(df.columns)}")

        df.printSchema()

        df.select([
            count(col(c)).alias(c) for c in df.columns
        ]).show(vertical=True)
