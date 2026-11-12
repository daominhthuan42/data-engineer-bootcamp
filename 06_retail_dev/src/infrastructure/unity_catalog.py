# unity_catalog.py
import logging
import re
from pyspark.sql import SparkSession

DEFAULT_SCHEMAS     = ["bronze", "silver", "gold"]
DEFAULT_VOLUMES     = ["checkpoints"]

def create_catalog(spark: SparkSession, logger: logging, name_catalog: str) -> None:
    """
    Create Unity Catalog and default schemas if not exists.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    logger : logging.Logger
        Logger instance.

    name_catalog : str,
        Custom catalog name.
        If provided, it overrides the default catalog.
    """

    try:
        logger.info(f"Creating catalog: {name_catalog}")

        # Create catalog
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {name_catalog}")
        logger.info(f"CATALOG {name_catalog} created successfully.")

        # Set current catalog context
        spark.sql(f"USE CATALOG {name_catalog}")

        # Create default schemas
        for schema in DEFAULT_SCHEMAS:
            logger.info(f"Creating schema: {schema}")
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            logger.info("Schema is ready: %s", schema)
        logger.info("Unity Catalog setup completed successfully: %s", name_catalog)
    except Exception as e:
        logger.exception("Failed to set up Unity Catalog: %s", name_catalog)
        raise

def create_volume(spark: SparkSession, logger: logging.Logger, schema_name: str,
                  volume_name: str, name_catalog: str) -> None:
    """
    Create a Unity Catalog Volume inside a specific schema
    if it does not exist.

    Volumes are mainly used for:
    - Storing streaming checkpoints.
    - Managing streaming state.
    - Storing intermediate data.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    logger : logging.Logger
        Logger instance.

    schema_name : str
        Target schema name (e.g., bronze, silver, gold).

    volume_name : str
        Name of the volume to be created (e.g., checkpoints).

    name_catalog : str
        Target catalog name where the schema and volume
        will be created.

    Returns
    -------
    None
        This function does not return any value.
    """

    # Validate identifiers
    identifiers = {
        "catalog": name_catalog,
        "schema": schema_name,
        "volume": volume_name
    }

    for key, value in identifiers.items():
        if not value or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError(f"Invalid {key} name: {value!r}")

    try:
        # Build fully-qualified volume name
        full_name = (f"`{name_catalog}`.`{schema_name}`.`{volume_name}`")

        logger.info("Creating volume: %s", full_name)

        # Create volume if it does not exist
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {full_name}")
        logger.info("Volume is ready: %s", full_name)

    except Exception:
        logger.exception("Failed to create volume: %s", full_name)
        raise

def drop_catalog(spark: SparkSession, logger: logging.Logger, name_catalog: str) -> None:
    """
    Drop a Unity Catalog and all its associated objects if it exists.

    This operation permanently removes the specified catalog,
    including all schemas, tables, volumes, and other objects
    contained within it, when CASCADE is specified.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    logger : logging.Logger
        Logger instance.

    name_catalog : str
        Name of the catalog to be dropped.

    Returns
    -------
    None
        This function does not return any value.

    Raises
    ------
    ValueError
        If the catalog name is invalid.

    Exception
        If the catalog cannot be dropped.
    """

    # Validate catalog name
    if not name_catalog or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name_catalog):
        raise ValueError(f"Invalid catalog name: {name_catalog!r}")

    try:
        logger.warning("Dropping catalog and all associated objects: %s", name_catalog)

        # Drop catalog and all its objects
        spark.sql(f"DROP CATALOG IF EXISTS `{name_catalog}` CASCADE")
        logger.info("Catalog and all associated objects dropped successfully: %s", name_catalog)

    except Exception:
        logger.exception("Failed to drop catalog: %s", name_catalog)
        raise