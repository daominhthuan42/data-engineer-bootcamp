# platform.py
import os
import io
import logging
from pathlib import Path
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from spark_config import SparkConfig
from pyspark.sql import SparkSession
from logger import LoggerConfig
from bronze_ingestion import BronzeIngestion
from silver_transformation import SilverTransformation
from gold_transformation import GoldTransformation
from io_config import IOConfig

class PlatformApp:
    """
    Main application for Platform.

    Responsibilities
    -----------------
    - Initialize logging system
    - Initialize Spark (Databricks Connect / Local)
    - Create and manage Unity Catalog objects
    - Create schemas and volumes
    - Clean up resources on shutdown

    This class represents the main entry point of the platform.
    """

    DEFAULT_CATALOG     = "databricks_toturial"
    DEFAULT_SCHEMAS     = ["bronze", "silver", "gold"]
    DEFAULT_VOLUMES     = ["checkpoints"]
    DEFAULT_ENTITIES    = ["customers", "trips", "locations", "payments", "vehicles", "drivers"]

    def __init__(self, spark: SparkSession, logger: logging, catalog_name: str):
        """
        Initialize application components:
        - Logger
        - Spark session
        - Default catalog name
        """
        # Init logger
        if logger is None:
            self.logger = LoggerConfig().setup_logger(log_dir=IOConfig.LOG_DIR)
        self.logger = logger
        self.logger.info("Initializing Data Platform...")

        # Init Spark
        if spark is None:
            self.spark = SparkConfig.create_spark(logger=self.logger)
        self.spark = spark
        self.logger.info("Spark session initialized")

        # Store current catalog name
        if catalog_name is None:
            self.catalog_name = self.DEFAULT_CATALOG
        self.catalog_name = catalog_name

    def create_catalog(self, name_catalog: str) -> None:
        """
        Create Unity Catalog and default schemas if not exists.

        Parameters
        ----------
        name_catalog : str,
            Custom catalog name.
            If provided, it overrides the default catalog.
        """

        try:
            # Override catalog name if provided
            if name_catalog:
                self.catalog_name = name_catalog

            self.logger.info(f"Creating catalog: {self.catalog_name}")

            # Create catalog
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog_name}")
            self.logger.info(f"CATALOG {self.catalog_name} created successfully.")

            # Set current catalog context
            self.spark.sql(f"USE CATALOG {self.catalog_name}")

            # Create default schemas
            for schema in self.DEFAULT_SCHEMAS:
                self.logger.info(f"Creating schema: {schema}")
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                self.logger.info(f"SCHEMA {schema} created successfully.")
            self.logger.info("Catalog setup completed.")
        except Exception as e:
            self.logger.error("Failed to create catalog.", exc_info=True)
            raise

    def create_volume(self, schema_name: str, volume_name: str) -> None:
        """
        Create a volume inside a specific schema.

        Volumes are mainly used for:
        - Streaming checkpoints
        - State management
        - Intermediate storage

        Parameters
        ----------
        schema_name : str
            Target schema name (e.g., bronze)

        volume_name : str
            Name of the volume to be created
        """

        try:
            # Build fully-qualified volume name
            full_name = (f"{self.catalog_name}.{schema_name}.{volume_name}")
            self.logger.info(f"Creating volume: {full_name}")
            # Create volume
            self.spark.sql(f"CREATE VOLUME IF NOT EXISTS {full_name}")
            self.logger.info(f"Volume {volume_name} created successfully.")
        except Exception:
            self.logger.error("Failed to create volume.", exc_info=True )
            raise

    def drop_catalog(self, name_catalog: str) -> None:
        """
        Drop a Spark catalog if it exists.

        Permanently removes the catalog and all its objects
        when cascade is enabled.

        Parameters
        ----------
        name_catalog : str
            Name of the catalog to drop.

        """
        # Override catalog name if provided
        if name_catalog is None:
            name_catalog = self.catalog_name
        try:
            self.logger.info(f"Dropping catalog: {name_catalog}")
            # Drop catalog
            self.spark.sql(f"DROP CATALOG IF EXISTS {name_catalog} CASCADE")
            self.logger.info("Catalog dropped successfully.")
        except Exception:
            self.logger.error("Failed to drop catalog.", exc_info=True)
            raise

    def upload_file_to_bronze(self, path_data: str, path_cp: str, 
                              name_catalog: str, file_format: str = "csv", header: bool = True) -> None:
        """
        Ingest local CSV files into Bronze Delta tables using Structured Streaming.

        This method performs incremental ingestion with streaming guarantees.
        It first infers schema using batch read, then applies the schema
        for streaming ingestion to ensure consistency.

        Workflow
        --------
        1. Read files in batch mode to infer schema
        2. Validate input directory is not empty
        3. Use Structured Streaming for incremental ingestion
        4. Write data to Bronze Delta tables
        5. Persist checkpoints for fault tolerance

        Parameters
        ----------
        path_data : str
            Path to source directory containing CSV files.

        path_cp : str
            Base path for streaming checkpoints (one per entity).

        name_catalog : str,
            Custom catalog name.
            If provided, it overrides the default catalog.

        file_format : str, default "csv"
            Input file format (currently supports only CSV).

        header : bool, default True
            Whether input CSV files contain headers.
        """
        if file_format.lower() != "csv":
            self.logger.warning(f"Unsupported file format: {file_format}")
            return

        if name_catalog:
            self.catalog_name = name_catalog

        for entity in self.DEFAULT_ENTITIES:
            self.logger.info(f"Starting Bronze ingestion for entity: {entity.title()}")
            path_data_entity = Path(os.path.join(path_data, entity)).as_posix()
            path_cp_entity = Path(os.path.join(path_cp, entity)).as_posix()
            try:
                # Structured Streaming with CSV does NOT support schema inference.
                # Therefore, we perform a one-time batch read to infer the schema.
                # This schema will be reused for the streaming read.
                self.logger.info(f"Uploading file: {path_data_entity}")
                bronze_df_batch = (
                        self.spark.read
                        .option("header", header)
                        .option("inferSchema", True)
                        .csv(path_data_entity)
                )

                # Skip ingestion if directory is empty
                if bronze_df_batch.limit(1).count() == 0:
                    self.logger.warning(f"{entity.title()}: Source directory is empty. Skipping ingestion.")
                    continue

                # Extract inferred schema from batch DataFrame
                schema_entity = bronze_df_batch.schema
                self.logger.info(f"{entity.title()}: Schema inferred successfully")

                # Spark continuously monitors the directory for new files.
                # When new files arrive, they are processed as micro-batches.
                # The schema is explicitly provided to ensure consistency.
                bronze_df_batch = (
                    self.spark.readStream.format("csv")
                    .option("header", header)
                    .schema(schema_entity)
                    .load(path_data_entity)
                )

                # Streaming Write to Bronze Delta Table
                # format("delta"):
                #   - Delta Lake is the recommended storage format for Bronze layer
                #
                # outputMode("append"):
                #   - Only new records are appended to the table
                #
                # checkpointLocation:
                #   - Stores streaming state and progress
                #   - Ensures fault tolerance and prevents duplicate ingestion
                #
                # trigger(once=True):
                #   - Executes the streaming query once and then stops
                #   - Behaves like a batch job with streaming guarantees
                #   - Ideal for incremental Bronze ingestion
                #
                # toTable():
                #   - Writes directly to a managed Delta table
                query = (
                    bronze_df_batch.writeStream
                    .format("delta")
                    .outputMode("append")
                    .option("checkpointLocation", path_cp_entity)
                    .trigger(once=True)
                    .toTable(f"{self.catalog_name}.bronze.{entity}")
                )

                # Wait until the streaming query finishes
                query.awaitTermination()

                self.logger.info(f"Completed Bronze ingestion for entity: {entity.title()}")
                self.spark.read.table(f"{IOConfig.BRONZE_COMMON}.{entity}").show(n=10, truncate=False)
                self.logger.info("-" * 80)
            except Exception as e:
                self.logger.exception(f"Bronze ingestion FAILED for entity: {entity.title()} | Error: {str(e)}")
                raise

    @staticmethod
    def parse_remote_time(remote_time_str: str) -> int:
        """
        Convert RFC 1123 datetime string to epoch milliseconds.
        """
        dt = datetime.strptime(remote_time_str,  "%a, %d %b %Y %H:%M:%S %Z")

        # Force UTC timezone
        dt = dt.replace(tzinfo=timezone.utc)

        # Convert to milliseconds
        return int(dt.timestamp() * 1000)

    def upload_local_to_uc_volume(self, local_base: str, catalog: str, 
                                  schema: str, volume: str) -> None:
        """
        Synchronize local CSV files to a Unity Catalog Volume.

        This method scans a local directory, detects new or modified CSV files,
        and uploads them to the specified Unity Catalog Volume while preserving
        the folder structure.

        Files that are unchanged (based on size and modified time)
        are skipped to avoid redundant uploads.

        Parameters
        ----------
        local_base : str,
            Root directory containing source CSV files.

        catalog : str,
            Target Unity Catalog name.

        schema : str,
            Target schema name.

        volume : str,
            Target volume name.
        """

        # Override default catalog if provided
        if catalog:
            self.catalog_name = catalog

        # Set catalog context
        self.spark.sql(f"USE CATALOG {self.catalog_name}")

        # Ensure schema exists
        self.logger.info(f"Creating schema: {schema}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # Ensure volume exists
        full_name = (f"{self.catalog_name}.{schema}.{volume}")
        self.spark.sql(f"CREATE VOLUME IF NOT EXISTS {full_name}")

        # Initialize Databricks workspace client (auto-auth)
        w = WorkspaceClient()

        # Base path of the target Unity Catalog volume
        base_volume = f"/Volumes/{catalog}/{schema}/{volume}"

        self.logger.info("Starting upload to Unity Catalog Volume...")

        # Walk through all subdirectories and files in local_base
        for root, _, files in os.walk(local_base):
            for file in files:
                # Process only CSV files
                if file.lower().endswith(".csv"):
                    local_file = os.path.join(root, file)
                    rel = os.path.relpath(local_file, local_base)
                    volume_file = f"{base_volume}/{rel}".replace("\\", "/")

                    # Collect local file metadata
                    local_size = os.path.getsize(local_file)
                    local_mtime = int(os.path.getmtime(local_file) * 1000)

                    upload_required = True

                    # Check remote metadata to determine whether upload is needed
                    try:
                        meta = w.files.get_metadata(volume_file)
                        remote_size = meta.content_length
                        remote_mtime = self.parse_remote_time(meta.last_modified)

                        # Skip upload if file is unchanged
                        if (local_size == remote_size and local_mtime <= remote_mtime):
                            upload_required = False
                            self.logger.info(f"Skip (unchanged): {volume_file}")
                    except NotFound:
                        self.logger.info(f"Remote file not found. Upload required: {volume_file}")
                        upload_required = True
                    except Exception as e:
                        # File does not exist remotely → must upload
                        self.logger.error(f"Unexpected error: {e}")
                        raise

                    # Skip unchanged files
                    if not  upload_required:
                        continue

                    # Read file as a seekable byte stream (required by SDK)
                    with open(local_file, "rb") as f:
                        content = io.BytesIO(f.read())

                    # Upload file to Unity Catalog Volume (auto chunking + retry)
                    w.files.upload(file_path=volume_file, contents=content, overwrite=True)
                    self.logger.info(f"Uploaded: {volume_file}")

    def stop(self) -> None:
        """
        Gracefully stop Spark session and release resources.
        """
        self.logger.info("Stopping Spark session...")
        self.spark.stop()
        self.logger.info("Spark stopped.")
