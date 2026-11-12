import os
import re
import logging
from pathlib import Path
from email.utils import parsedate_to_datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from pyspark.sql import SparkSession


def _parse_remote_time(remote_time_str: str) -> int:
    """
    Convert an RFC 1123 datetime string to epoch milliseconds.

    Parameters
    ----------
    remote_time_str : str
        Remote file modification time in RFC 1123 format.

    Returns
    -------
    int
        Timestamp in milliseconds since the Unix epoch.
    """
    dt = parsedate_to_datetime(remote_time_str)

    # Ensure the datetime is timezone-aware
    if dt.tzinfo is None:
        from datetime import timezone

        dt = dt.replace(tzinfo=timezone.utc)

    return int(dt.timestamp() * 1000)


def upload_local_to_uc_volume(
    spark: SparkSession,
    logger: logging.Logger,
    local_base: str,
    catalog_name: str,
    schema: str,
    volume: str,
    profile: str = "ThuanDao",
) -> None:
    """
    Synchronize local CSV files to a Unity Catalog Volume.

    This function recursively scans a local directory, identifies CSV
    files, and uploads new or modified files to the specified Unity
    Catalog Volume while preserving the original folder structure.

    Remote file metadata (file size and last modified time) is used
    as a heuristic to determine whether an upload is required.

    Note
    ----
    Remote last modified time represents the upload/update time on
    Databricks, not the original local file modification time.
    Therefore, this metadata comparison cannot guarantee detection
    of all content changes.

    The target catalog must already exist. The target schema and
    volume are created automatically if they do not exist.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    logger : logging.Logger
        Logger instance for recording processing information.

    local_base : str
        Root directory containing source CSV files.

    catalog_name : str
        Target Unity Catalog name.

    schema : str
        Target schema name.

    volume : str
        Target volume name.

    profile : str, optional
        Databricks authentication profile.
        Defaults to "ThuanDao".

    Returns
    -------
    None
        This function does not return any value.

    Raises
    ------
    ValueError
        If an input path or Unity Catalog identifier is invalid.

    FileNotFoundError
        If the local source directory does not exist.

    Exception
        If the Unity Catalog setup or file upload fails.
    """

    # Validate Unity Catalog identifiers
    identifiers = {"catalog": catalog_name, "schema": schema, "volume": volume}

    for key, value in identifiers.items():
        if not value or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError(f"Invalid {key} name: {value!r}")

    # Validate local source directory
    local_base = Path(local_base).resolve()

    if not local_base.is_dir():
        raise FileNotFoundError(f"Local source directory not found: {local_base}")

    try:
        # Set catalog context
        spark.sql(f"USE CATALOG `{catalog_name}`")

        # Create schema if it does not exist
        logger.info("Creating schema: %s", schema)

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")

        # Create volume if it does not exist
        full_name = f"`{catalog_name}`.`{schema}`.`{volume}`"

        logger.info("Creating volume: %s", full_name)

        spark.sql(f"CREATE VOLUME IF NOT EXISTS {full_name}")

        # Initialize Databricks Workspace Client
        w = WorkspaceClient(profile=profile)

        # Define target Unity Catalog Volume path
        base_volume = f"/Volumes/{catalog_name}/{schema}/{volume}"

        logger.info("Starting upload to Unity Catalog Volume: %s", base_volume)

        total_uploaded = 0
        total_skipped = 0

        # Recursively scan local directory
        for root, _, files in os.walk(local_base):
            for file in files:
                # Process CSV files only
                if not file.lower().endswith(".csv"):
                    continue

                local_file = Path(root) / file

                # Preserve the relative folder structure
                rel = local_file.relative_to(local_base).as_posix()

                volume_file = f"{base_volume}/{rel}"

                # Get local file metadata
                local_stat = local_file.stat()
                local_size = local_stat.st_size
                local_mtime = local_stat.st_mtime_ns // 1_000_000

                upload_required = True

                # Check remote file metadata
                try:
                    meta = w.files.get_metadata(volume_file)

                    remote_size = meta.content_length
                    remote_mtime = _parse_remote_time(meta.last_modified)

                    # Skip unchanged files based on metadata heuristic
                    if local_size == remote_size and local_mtime <= remote_mtime:
                        upload_required = False
                        total_skipped += 1
                        logger.info("Skip (unchanged): %s", volume_file)

                except NotFound:
                    logger.info("Remote file not found. Upload required: %s", volume_file)

                except Exception:
                    logger.exception("Failed to retrieve remote metadata: %s", volume_file)
                    raise

                # Skip files that do not require uploading
                if not upload_required:
                    continue

                # Upload file directly without loading it entirely into RAM
                with open(local_file, "rb") as content:
                    w.files.upload(file_path=volume_file, contents=content, overwrite=True)

                total_uploaded += 1

                logger.info("Uploaded successfully: %s", volume_file)

        logger.info("Upload completed. Uploaded: %d | Skipped: %d", total_uploaded, total_skipped)

    except Exception:
        logger.exception("Failed to synchronize local data to Unity Catalog Volume.")
        raise
