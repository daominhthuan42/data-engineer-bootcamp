"""
MLflow S3 utilities to ensure artifacts are stored in MinIO
"""

import os
import mlflow
import boto3
from botocore.client import Config
import shutil
from typing import Optional
from .service_discovery import get_minio_endpoint
from utils.logger import logger

class MLflowS3Manager:
    """
    Manager class to ensure MLflow artifacts are stored in S3/MinIO
    """

    def __init__(self):
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=get_minio_endpoint(),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            config=Config(signature_version="s3v4"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )
        self.bucket_name = "mlflow-artifacts"

    def upload_artifact_to_s3(self, local_path: str, run_id: str, 
                              artifact_path: Optional[str] = None) -> str:
        """
        Upload a local artifact file to S3 storage.

        The artifact is stored using an MLflow-compatible directory
        structure based on the provided run ID.

        Parameters
        ----------
        local_path : str
            Path to the local file to be uploaded.

        run_id : str
            MLflow run identifier associated with the artifact.

        artifact_path : str, optional
            Logical artifact subdirectory within the run's artifact
            hierarchy.

        Returns
        -------
        str
            S3 object key of the uploaded artifact.

        Raises
        ------
        Exception
            Raised when the upload operation fails.
        """
        try:
            # Construct S3 key
            if artifact_path:
                s3_key = f"{run_id[:2]}/{run_id[2:4]}/{run_id}/artifacts/{artifact_path}/{os.path.basename(local_path)}"
            else:
                s3_key = f"{run_id[:2]}/{run_id[2:4]}/{run_id}/artifacts/{os.path.basename(local_path)}"
            
            # Upload to S3
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            logger.info(f"Uploaded {local_path} to s3://{self.bucket_name}/{s3_key}")
            
            return s3_key
            
        except Exception as e:
            logger.error(f"Failed to upload artifact to S3: {e}")
            raise

    def log_artifact_with_s3(self, local_path: str, 
                             artifact_path: Optional[str] = None) -> None:
        """
        Log an artifact to MLflow and upload it to S3 storage.

        This method first registers the artifact with the active MLflow
        run and then uploads the same file to the configured S3/MinIO
        bucket to ensure long-term artifact persistence.

        Parameters
        ----------
        local_path : str
            Path to the local artifact file.

        artifact_path : str, optional
            Logical artifact subdirectory within the MLflow artifact
            hierarchy.

        Returns
        -------
        None
        """
        # Log artifact to the active MLflow run
        if artifact_path:
            mlflow.log_artifact(local_path, artifact_path)
        else:
            mlflow.log_artifact(local_path)
        
        # Upload artifact to S3/MinIO storage
        run = mlflow.active_run()
        if run:
            self.upload_artifact_to_s3(local_path, run.info.run_id, artifact_path)

    def sync_mlflow_artifacts_to_s3(self, run_id: str) -> None:
        """
        Sync all artifacts from an MLflow run to S3 storage.

        This method downloads all artifacts from a given MLflow run to a
        temporary local directory, then uploads each artifact to the
        configured S3/MinIO bucket using an MLflow-compatible path structure.

        Parameters
        ----------
        run_id : str
            MLflow run identifier whose artifacts should be synchronized.

        Returns
        -------
        None

        Raises
        ------
        Exception
            Raised when artifact download, upload, or cleanup fails.
        """
        try:
            # MLflow client used to access run artifacts
            client = mlflow.tracking.MlflowClient()

            # Prepare temporary local directory for artifact download
            local_dir = f"/tmp/mlflow_sync/{run_id}"
            if os.path.exists(local_dir):
                shutil.rmtree(local_dir)

            # Download all artifacts from the MLflow run
            artifacts_dir = client.download_artifacts(run_id, "", dst_path=local_dir)

            # Upload each downloaded artifact file to S3/MinIO
            for root, dirs, files in os.walk(artifacts_dir):
                for file in files:
                    local_file = os.path.join(root, file)
                    # Calculate relative path
                    relative_path = os.path.relpath(local_file, artifacts_dir)
                    
                    # Upload to S3
                    s3_key = f"{run_id[:2]}/{run_id[2:4]}/{run_id}/artifacts/{relative_path}"
                    self.s3_client.upload_file(local_file, self.bucket_name, s3_key)
                    logger.info(f"Synced {relative_path} to S3")

            # Clean up temporary downloaded artifacts
            shutil.rmtree(local_dir)
            logger.info(f"Successfully synced all artifacts for run {run_id} to S3")

        except Exception as e:
            logger.error(f"Failed to sync artifacts to S3: {e}")
            raise

    def list_s3_artifacts(self, run_id: str) -> list:
        """
        List all artifact objects stored in S3 for a given MLflow run.

        This method searches the configured S3/MinIO bucket using the
        MLflow artifact path convention and returns all matching object keys.

        Parameters
        ----------
        run_id : str
            MLflow run identifier.

        Returns
        -------
        list[str]
            List of S3 object keys associated with the specified run.
            Returns an empty list if no artifacts are found or an error occurs.
        """
        try:
            # Build MLflow artifact prefix
            # Example:
            # ab/cd/abcdef123456/
            prefix = f"{run_id[:2]}/{run_id[2:4]}/{run_id}/"

            # Query objects under the run-specific prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            # Extract object keys from the response
            if "Contents" in response:
                return [obj["Key"] for obj in response["Contents"]]
            else:
                return []
        except Exception as e:
            logger.error(f"Failed to list S3 artifacts: {e}")
            return []
