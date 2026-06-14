"""
S3 artifact verification utilities for MLflow.

This module provides helper functions to verify whether MLflow artifacts
for a specific run are successfully stored in MinIO-compatible S3 storage.
"""

import os
from typing import Any, Dict, List, Optional
import boto3
import mlflow
from botocore.client import Config
from utils.logger import logger


def verify_s3_artifacts(
    run_id: str,
    expected_artifacts: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Verify that MLflow artifacts for a run are stored in MinIO S3.

    Parameters
    ----------
    run_id : str
        MLflow run ID whose artifacts should be verified.

    expected_artifacts : Optional[List[str]], default=None
        List of expected artifact names or partial paths to check.
        If provided, each expected artifact must be found in the S3
        object list.

    Returns
    -------
    Dict[str, Any]
        Dictionary containing verification results, including:

        - success : Whether verification passed.
        - artifact_uri : MLflow artifact URI for the run.
        - s3_artifacts : List of artifact paths found in S3.
        - missing_artifacts : Expected artifacts not found in S3.
        - errors : Error messages collected during verification.
    """
    results = {
        "success": False,
        "artifact_uri": "",
        "s3_artifacts": [],
        "missing_artifacts": [],
        "errors": []
    }

    try:
        # Retrieve the MLflow run metadata to locate its artifact URI
        client = mlflow.tracking.MlflowClient()
        run = client.get_run(run_id)
        artifact_uri = run.info.artifact_uri
        results["artifact_uri"] = artifact_uri

        # This utility only verifies artifacts stored in S3-compatible storage
        if not artifact_uri.startswith("s3://"):
            results["errors"].append(
                f"Artifact URI is not S3: {artifact_uri}"
            )
            return results

        # Parse artifact URI.
        # Example:
        #   s3://mlflow-artifacts/1/abc123/artifacts
        #   -> bucket = "mlflow-artifacts"
        #   -> prefix = "1/abc123/artifacts"
        parts = artifact_uri.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        # Create a MinIO-compatible S3 client using environment variables,
        # with local development defaults as fallback values
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv(
                "MLFLOW_S3_ENDPOINT_URL",
                "http://minio:9000"
            ),
            aws_access_key_id=os.getenv(
                "AWS_ACCESS_KEY_ID",
                "minioadmin"
            ),
            aws_secret_access_key=os.getenv(
                "AWS_SECRET_ACCESS_KEY",
                "minioadmin"
            ),
            config=Config(signature_version="s3v4"),
            region_name="us-east-1"
        )

        # List all S3 objects under the MLflow artifact prefix
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )

        if "Contents" in response:
            s3_objects = []

            for obj in response["Contents"]:
                # Convert full S3 object key into a path relative to the
                # MLflow artifact root for easier comparison and logging
                relative_path = obj["Key"].replace(prefix, "").lstrip("/")

                # Skip the artifact root itself if it appears as an object
                if relative_path:
                    s3_objects.append(relative_path)

            results["s3_artifacts"] = s3_objects

            # Verify expected artifacts using partial matching so callers
            # can pass names such as "model.pkl" instead of full S3 paths
            if expected_artifacts:
                for expected in expected_artifacts:
                    found = any(
                        expected in artifact
                        for artifact in s3_objects
                    )

                    if not found:
                        results["missing_artifacts"].append(expected)

            # Verification passes only when artifacts exist and all expected
            # artifacts are present
            results["success"] = (
                len(s3_objects) > 0
                and len(results["missing_artifacts"]) == 0
            )

            logger.info(
                f"Found {len(s3_objects)} artifacts in S3 for run {run_id}"
            )

            # Log only the first few artifacts to avoid noisy logs
            logger.info(
                f"Artifacts: {', '.join(s3_objects[:5])}..."
            )

        else:
            results["errors"].append("No artifacts found in S3")

    except Exception as e:
        # Capture errors instead of raising them so this utility can be used
        # safely in validation or monitoring workflows
        results["errors"].append(str(e))
        logger.error(f"Error verifying S3 artifacts: {e}")

    return results


def log_s3_verification_results(results: Dict[str, Any]) -> None:
    """
    Log S3 artifact verification results.

    Parameters
    ----------
    results : Dict[str, Any]
        Verification result dictionary returned by
        ``verify_s3_artifacts``.

    Returns
    -------
    None
    """
    if results["success"]:
        logger.info("✓ S3 artifact verification PASSED")
        logger.info(f"  - Artifact URI: {results['artifact_uri']}")
        logger.info(
            f"  - Total artifacts: {len(results['s3_artifacts'])}"
        )
    else:
        logger.error("✗ S3 artifact verification FAILED")
        logger.error(f"  - Artifact URI: {results['artifact_uri']}")

        # Log all captured verification or connectivity errors
        for error in results["errors"]:
            logger.error(f"  - Error: {error}")

        # Log missing expected artifacts separately for faster debugging
        if results["missing_artifacts"]:
            logger.error(
                "  - Missing artifacts: "
                f"{', '.join(results['missing_artifacts'])}"
            )
