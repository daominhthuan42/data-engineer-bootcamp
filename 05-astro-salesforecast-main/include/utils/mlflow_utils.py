import os
import mlflow
import mlflow.pyfunc
from mlflow.tracking import MlflowClient
from typing import Dict, Any, Optional
import yaml
import pandas as pd
from datetime import datetime
import joblib
from .service_discovery import get_mlflow_endpoint, get_minio_endpoint
from utils.logger import logger

class MLflowManager:
    """
    Manage MLflow experiment tracking, model logging, model registry,
    and artifact synchronization.

    This class centralizes MLflow operations used in the ML pipeline,
    including starting runs, logging parameters/metrics/models,
    registering models, transitioning model stages, and loading models
    for inference.
    """

    def __init__(self, config_path: str = "/usr/local/airflow/include/config/ml_config.yaml"):
        """
        Initialize MLflow manager configuration.

        Parameters
        ----------
        config_path : str, default="/usr/local/airflow/include/config/ml_config.yaml"
            Path to the ML configuration file containing MLflow settings.
        """

        # Load ML pipeline configuration
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        
        mlflow_config = self.config["mlflow"]

        # Resolve MLflow tracking URI using service discovery
        self.tracking_uri = get_mlflow_endpoint()

        # MLflow experiment and registry configuration
        self.experiment_name = mlflow_config['experiment_name']
        self.registry_name = mlflow_config['registry_name']

        # Configure MLflow tracking server
        mlflow.set_tracking_uri(self.tracking_uri)

        # Set or create MLflow experiment
        # Fallback to localhost when service discovery endpoint fails
        try:
            mlflow.set_experiment(self.experiment_name)
        except Exception as e:
            logger.warning(f"Failed to set experiment {self.experiment_name}: {e}")
            # Try with localhost if initial connection failed
            if 'mlflow' in self.tracking_uri:
                self.tracking_uri = "http://localhost:5001"
                mlflow.set_tracking_uri(self.tracking_uri)
                os.environ['MLFLOW_TRACKING_URI'] = self.tracking_uri
                logger.info(f"Retrying with localhost: {self.tracking_uri}")
                try:
                    mlflow.set_experiment(self.experiment_name)
                except Exception as e2:
                    logger.error(f"Failed to connect to MLflow: {e2}")
        
        # Configure S3 endpoint for MinIO using service discovery
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = get_minio_endpoint()
        os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
        os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
        
        self.client = MlflowClient(tracking_uri=self.tracking_uri)

    def start_run(self, run_name: Optional[str] = None, tags: Optional[Dict[str, str]] = None) -> str:
        """
        Start a new MLflow run.

        If no run name is provided, a timestamp-based name will be generated
        automatically.

        Parameters
        ----------
        run_name : Optional[str], default=None
            Name of the MLflow run. If None, a default name in the format
            'run_YYYYMMDD_HHMMSS' will be used.

        tags : Optional[Dict[str, str]], default=None
            Key-value metadata tags associated with the run.

        Returns
        -------
        str
            The unique MLflow run ID.
        """
        if run_name is None:
            run_name = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        run = mlflow.start_run(run_name=run_name, tags=tags)
        logger.info(f"Started MLflow run: {run.info.run_id}")
        return run.info.run_id

    def log_params(self, params: Dict[str, Any]):
        """
        Log model parameters to the active MLflow run.

        Parameters
        ----------
        params : Dict[str, Any]
            Dictionary containing parameter names and values.

        Returns
        -------
        None
        """
        for key, value in params.items():
            mlflow.log_param(key, value)
    
    def log_metrics(self, metrics: Dict[str, float], step: Optional[int] = None):
        """
        Log evaluation metrics to the active MLflow run.

        Parameters
        ----------
        metrics : Dict[str, float]
            Dictionary containing metric names and values.

        step : Optional[int], default=None
            Training step, epoch number, iteration number,
            or cross-validation fold index.

        Returns
        -------
        None
        """
        for key, value in metrics.items():
            mlflow.log_metric(key, value, step=step)

    def log_model(self, model, model_name: str, input_example: Optional[pd.DataFrame] = None,
                  signature: Optional[Any] = None, registered_model_name: Optional[str] = None) -> None:
        """
        Log a trained model to MLflow.

        The model is serialized using joblib and stored as an MLflow artifact.
        Additional metadata, including model type, framework, class name, and
        timestamp, is also logged for traceability.

        This implementation provides a fallback mechanism for environments where
        native MLflow model logging may not be available or compatible.

        Parameters
        ----------
        model : Any
            Trained model object to be logged.

        model_name : str
            Logical name of the model used for artifact organization.

        input_example : Optional[pd.DataFrame], default=None
            Example input data for the model. Reserved for future support of
            MLflow model signatures and schema validation.

        signature : Optional[Any], default=None
            MLflow model signature describing input and output schemas.
            Currently not used.

        registered_model_name : Optional[str], default=None
            Name of the model in the MLflow Model Registry.
            Currently not used.

        Returns
        -------
        None

        Notes
        -----
        - The model is stored as a serialized artifact using joblib.
        - Model metadata is saved as a YAML artifact.
        - Errors during model logging are captured and logged without
        interrupting the active MLflow run.
        """
        try:
            # Save model to a temporary file first
            import tempfile
            with tempfile.TemporaryDirectory() as tmpdir:
                model_path = os.path.join(tmpdir, f"{model_name}_model.pkl")
                joblib.dump(model, model_path)
                
                # Log as artifact
                mlflow.log_artifact(model_path, artifact_path=f"models/{model_name}")
                logger.info(f"Successfully saved {model_name} model as artifact")
                
                # Also save metadata
                metadata = {
                    "model_type": model_name,
                    "framework": type(model).__module__,
                    "class": type(model).__name__,
                    "timestamp": datetime.now().isoformat()
                }
                metadata_path = os.path.join(tmpdir, f"{model_name}_metadata.yaml")
                with open(metadata_path, 'w') as f:
                    yaml.dump(metadata, f)
                mlflow.log_artifact(metadata_path, artifact_path=f"models/{model_name}")

        except Exception as e:
            logger.error(f"Failed to log model {model_name}: {e}")
            # Don't fail the entire run, just log the error

    def log_artifacts(self, artifact_path: str):
        """
        Log all files within a directory as MLflow artifacts.

        Parameters
        ----------
        artifact_path : str
            Path to the local directory containing artifacts to be logged.

        Returns
        -------
        None
        """
        mlflow.log_artifacts(artifact_path)

    def log_figure(self, figure, artifact_file: str):
        """
        Log a visualization figure as an MLflow artifact.

        Parameters
        ----------
        figure : Any
            Figure object to be logged. Typically a Matplotlib figure.

        artifact_file : str
            Destination file name within the MLflow artifact store.

        Returns
        -------
        None
        """
        mlflow.log_figure(figure, artifact_file)

    def end_run(self, status: str = "FINISHED"):
        """
        End the active MLflow run.

        After the run is successfully completed, all generated artifacts
        are synchronized to S3 storage for long-term persistence and
        centralized access.

        Parameters
        ----------
        status : str, default="FINISHED"
            Final run status. Common values include:

            - "FINISHED"
            - "FAILED"
            - "KILLED"

        Returns
        -------
        None

        Notes
        -----
        - The active run ID is captured before ending the run.
        - Artifact synchronization to S3 is performed only when the run
        completes successfully with status "FINISHED".
        - Failures during artifact synchronization are logged as warnings
        and do not affect the completed MLflow run.
        """
        # Get run ID before ending
        run = mlflow.active_run()
        run_id = run.info.run_id if run else None

        mlflow.end_run(status=status)
        logger.info("Ended MLflow run")

        # Sync artifacts to S3 after run ends
        if run_id and status == "FINISHED":
            try:
                from utils.mlflow_s3_utils import MLflowS3Manager
                s3_manager = MLflowS3Manager()
                s3_manager.sync_mlflow_artifacts_to_s3(run_id)
                logger.info(f"Synced artifacts to S3 for run {run_id}")
            except Exception as e:
                logger.warning(f"Failed to sync artifacts to S3: {e}")

    def get_best_model(self, metric: str = "rmse", ascending: bool = True) -> Dict[str, Any]:
        """
        Retrieve the best MLflow run from the configured experiment based
        on a specified evaluation metric.

        The method searches all runs within the experiment, sorts them by
        the provided metric, and returns information from the top-ranked run.

        Parameters
        ----------
        metric : str, default="rmse"
            Metric name used to rank runs. The metric must exist in the
            MLflow tracking data.

        ascending : bool, default=True
            Sort order for the metric.

            - True: Lower values are considered better (e.g., RMSE, MAE, Loss).
            - False: Higher values are considered better (e.g., Accuracy, F1, R²).

        Returns
        -------
        Dict[str, Any]
            Dictionary containing information about the best run, including:

            - run_id: Unique MLflow run identifier.
            - metrics: Logged metrics associated with the run.
            - params: Logged parameters associated with the run.

        Raises
        ------
        ValueError
            If no runs are found in the configured experiment.

        Notes
        -----
        Common metric ordering recommendations:

        - RMSE, MAE, MSE, Loss  -> ascending=True
        - Accuracy, Precision,
        Recall, F1 Score, R²  -> ascending=False
        """

        # Retrieve experiment metadata using the configured experiment name
        experiment = mlflow.get_experiment_by_name(self.experiment_name)

        # Query MLflow runs ordered by the specified metric
        # Example:
        #   ascending=True  -> RMSE ASC  (lowest RMSE wins)
        #   ascending=False -> F1 DESC   (highest F1 wins)
        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=[f"metrics.{metric} {'ASC' if ascending else 'DESC'}"],
            max_results=1
        )

        # No runs available in the experiment
        if len(runs) == 0:
            raise ValueError("No runs found in the experiment")

        # First row contains the best run after sorting
        best_run = runs.iloc[0]
        return {
            # Unique MLflow run identifier
            "run_id": best_run["run_id"],

            # Extract all logged metrics and remove the 'metrics.' prefix
            "metrics": {
                col.replace("metrics.", ""): val
                for col, val in best_run.items()
                if col.startswith("metrics.")
            },

            # Extract all logged parameters and remove the 'params.' prefix
            "params": {
                col.replace("params.", ""): val
                for col, val in best_run.items()
                if col.startswith("params.")
            }
        }

    def load_model(self, model_uri: str):
        """
        Load a model from MLflow.

        The method first attempts to load the model using the MLflow
        PyFunc interface. If that fails, it falls back to downloading
        and deserializing a model artifact stored as a Joblib file.

        Parameters
        ----------
        model_uri : str
            MLflow model URI.

            Examples:
            - runs:/<run_id>/model
            - models:/<model_name>/Production
            - models:/<model_name>/Latest

        Returns
        -------
        Any
            Loaded model instance.

        Raises
        ------
        ValueError
            If the model cannot be loaded from the provided URI.

        Notes
        -----
        The fallback mechanism is intended for models that were logged
        as artifacts (e.g., Joblib pickle files) rather than registered
        using MLflow's native model logging APIs.
        """
        try:
            # Attempt to load the model using MLflow's standard PyFunc loader
            return mlflow.pyfunc.load_model(model_uri)
        except:
            # Fallback for models saved as Joblib artifacts instead of
            # native MLflow models
            if "runs:/" in model_uri:
                # Extract run ID and artifact path from the MLflow URI
                # Example:
                # runs:/123abc/xgboost
                #   -> run_id = "123abc"
                #   -> artifact_path = "xgboost"
                run_id = model_uri.split("/")[1]
                artifact_path = "/".join(model_uri.split("/")[2:])

                # Download the serialized model artifact from MLflow storage
                local_path = mlflow.artifacts.download_artifacts(
                    run_id=run_id, 
                    artifact_path=f"{artifact_path}_model.pkl"
                )

                 # Deserialize the model using Joblib
                return joblib.load(local_path)
            else:
                raise ValueError(f"Cannot load model from {model_uri}")
    
    def register_model(self, run_id: str, model_name: str, artifact_path: str) -> str:
        """
        Register a model in the MLflow Model Registry.

        If model registration is unavailable or not supported by the
        current MLflow deployment, the method falls back to returning
        the run ID as a version identifier.

        Parameters
        ----------
        run_id : str
            MLflow run ID containing the model artifacts.

        model_name : str
            Logical model name used when registering the model.

        artifact_path : str
            Artifact path where the model is stored within the run.

        Returns
        -------
        str
            Registered model version if registration succeeds;
            otherwise the originating run ID.

        Notes
        -----
        - Registered model names are prefixed with the configured
        registry name to avoid naming conflicts.
        - This fallback behavior allows training pipelines to continue
        running even when the MLflow Model Registry is unavailable.
        """
        try:
            # Construct MLflow model URI from the run artifact location
            model_uri = f"runs:/{run_id}/{artifact_path}"

            # Register the model in the MLflow Model Registry
            model_version = mlflow.register_model(model_uri, f"{self.registry_name}_{model_name}")

            # Return the assigned model version
            return model_version.version
        except Exception as e:
            # Some MLflow deployments may not support Model Registry
            # (e.g. file-based tracking stores or restricted environments)
            logger.warning(f"Model registration not available, using run_id as version. Error: {e}")

            # Fallback identifier to keep downstream processes working
            return run_id

    def transition_model_stage(self, model_name: str, version: str, stage: str):
        """
        Transition a registered model version to a specified stage.

        Parameters
        ----------
        model_name : str
            Logical name of the registered model.

        version : str
            Model version to be transitioned.

        stage : str
            Target model stage.

            Common values include:
            - "Staging"
            - "Production"
            - "Archived"
            - "None"

        Returns
        -------
        None

        Notes
        -----
        This operation requires MLflow Model Registry support.
        If the registry is unavailable, the transition request is
        skipped and a warning is logged.
        """
        try:
            # Move the specified model version to the target lifecycle stage
            self.client.transition_model_version_stage(
                name=f"{self.registry_name}_{model_name}",
                version=version,
                stage=stage
            )

            logger.info(
                f"Transitioned model '{model_name}' "
                f"(version={version}) to stage '{stage}'"
            )
        except Exception as e:
            # Some MLflow deployments do not support Model Registry
            # or model stage transitions
            logger.warning(f"Model stage transition not available. Error: {e}")

    def get_latest_model_version(self, model_name: str, stage: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieve the latest registered version of a model.

        The method searches the MLflow Model Registry for model versions
        matching the specified model name and optional stage. If Model
        Registry functionality is unavailable, it falls back to the best
        run from the experiment.

        Parameters
        ----------
        model_name : str
            Logical name of the registered model.

        stage : Optional[str], default=None
            Filter model versions by lifecycle stage.

            Common values include:
            - "Staging"
            - "Production"
            - "Archived"

            If None, all model versions are considered.

        Returns
        -------
        Dict[str, Any]
            Dictionary containing:

            - version : Model version identifier.
            - stage : Current model stage.
            - run_id : Associated MLflow run ID.
            - source : Artifact source URI.

        Notes
        -----
        - When multiple versions are available, the highest version
        number is returned.
        - If Model Registry is unavailable, the method falls back to
        the best experiment run and uses its run ID as a pseudo-version.
        """
        try:
            # Build MLflow Model Registry search filter
            filter_string = f"name='{self.registry_name}_{model_name}'"

            # Restrict results to a specific lifecycle stage if requested
            if stage:
                filter_string += f" AND current_stage='{stage}'"

            # Retrieve all matching model versions
            versions = self.client.search_model_versions(filter_string)
            if not versions:
                raise ValueError(f"No model versions found for {model_name}")

            # Select the newest version based on version number
            latest_version = max(versions, key=lambda x: int(x.version))
            return {
                "version": latest_version.version,
                "stage": latest_version.current_stage,
                "run_id": latest_version.run_id,
                "source": latest_version.source
            }
        except Exception as e:
            # Fallback for environments where Model Registry is not
            # configured or supported.
            logger.warning(
                f"Model Registry unavailable. "
                f"Falling back to best experiment run. Error: {e}"
            )

            # Use the best experiment run as a pseudo model version
            best_model = self.get_best_model()

            return {
                "version": best_model["run_id"],
                "stage": "None",
                "run_id": best_model["run_id"],

                # Construct a synthetic model URI compatible with
                # downstream loading logic
                "source": f"runs:/{best_model['run_id']}/models"
            }
