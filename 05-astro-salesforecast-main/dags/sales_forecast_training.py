from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import pandas as pd
import os
import sys

# Add include path
sys.path.append("/usr/local/airflow/include")

from utils.logger import logger
from utils.data_generator import RealisticSalesDataGenerator
from ml_models.train_models import ModelTrainer
from utils.mlflow_utils import MLflowManager
from data_validation.validators import DataValidator

default_args = {
    "owner": "thuandao",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 12),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["daominhthuan091296@gmail.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    schedule= "@weekly",
    start_date=datetime(2026, 6, 12),
    catchup=False,
    default_args=default_args,
    description="Train sales forecasting models",
    tags=["ml", "training", "sales"]
)
def sales_forecast_training():
    @task()
    def extract_data_task():
        """
        Generate synthetic sales datasets for forecasting pipeline.

        This task creates:
        - Sales transaction data
        - Promotion data
        - Store event data
        - Customer traffic data

        Returns:
            dict:
                data_output_dir : location of generated datasets
                file_paths      : generated file paths by dataset type
                total_files     : total number of generated files
        """

        # Temporary output directory used by downstream tasks
        data_output_dir = "/tmp/sales_data"

        # Initialize sales data generator
        generator = RealisticSalesDataGenerator(
            start_date="2021-01-01", end_date="2021-12-31"
        )
        logger.info("Generating realistic sales data...")

        # Generate all datasets and collect output file paths
        file_paths = generator.generate_sales_data(logger=logger, output_dir=data_output_dir)

        # Calculate total number of generated files
        total_files = sum(len(paths) for paths in file_paths.values())
        logger.info(f"Generated {total_files} files:")

        # Log file count by dataset type
        for data_type, paths in file_paths.items():
            logger.debug(f"  - {data_type}: {len(paths)} files")

        # Return metadata for downstream tasks
        return {
            "data_output_dir": data_output_dir,
            "file_paths": file_paths,
            "total_files": total_files,
        }

    @task()
    def validate_data_task(extract_result):
        """
        Validate generated sales datasets before downstream processing.

        Validation checks:
        - Sales files are not empty
        - Required columns exist
        - No negative quantities or revenue values
        - Sample supplementary datasets can be read successfully

        Returns:
            dict: Validation summary including:
                - total_files_validated
                - total_rows
                - issues_found
                - issues
        """

        # Extract file paths returned from extract_data_task
        file_paths = extract_result["file_paths"]

        # Validation metrics
        total_rows = 0
        issues_found = []
        logger.info(f"Validating {len(file_paths['sales'])} sales files...")

        # Validate only first 10 sales files for faster execution
        for i, sales_file in enumerate(file_paths["sales"][:10]):
            # Load sales parquet file
            df = pd.read_parquet(sales_file)

            # Log schema from first file for debugging purposes
            if i == 0:
                logger.debug(f"Sales data columns: {df.columns.tolist()}")

            # Check empty file
            if df.empty:
                issues_found.append(f"Empty file: {sales_file}")
                continue

            # Required business columns
            required_cols = ["date", "store_id", "product_id", "quantity_sold", "revenue"]

            # Check for missing columns
            missing_cols = set(required_cols) - set(df.columns)

            # Track total rows validated
            if missing_cols:
                issues_found.append(f"Missing columns in {sales_file}: {missing_cols}")

            # Business rule: quantity cannot be negative
            total_rows += len(df)
            if df["quantity_sold"].min() < 0:
                issues_found.append(f"Negative quantities in {sales_file}")

            # Business rule: revenue cannot be negative
            if df["revenue"].min() < 0:
                issues_found.append(f"Negative revenue in {sales_file}")

            # Check for missing values
            null_counts = df[required_cols].isnull().sum()
            if null_counts.sum() > 0:
                issues_found.append(f"Null values found in {sales_file}: {null_counts.to_dict()}")

            # Check Duplicate
            duplicate_count = df.duplicated(subset=["date", "store_id", "product_id"]).sum()
            if duplicate_count > 0:
                issues_found.append(f"{duplicate_count} duplicate records found in {sales_file}")

        # Validate supplementary datasets
        # (sample file only to avoid expensive full scan)
        for data_type in ["promotions", "store_events", "customer_traffic"]:
            if data_type in file_paths and file_paths[data_type]:
                sample_file = file_paths[data_type][0]
                df = pd.read_parquet(sample_file)
                logger.debug(f"{data_type} data shape: {df.shape}")
                logger.debug(f"{data_type} columns: {df.columns.tolist()}")

        # Build validation report
        validation_summary = {
            "total_files_validated": len(file_paths["sales"][:10]),
            "total_rows": total_rows,
            "issues_found": len(issues_found),
            "issues": issues_found,
        }

        # Log validation result
        if issues_found:
            logger.info(f"Validation completed with {len(issues_found)} issues:")
            for issue in issues_found:
                logger.debug(f"  - {issue}")
        else:
            logger.info(f"Validation passed! Total rows: {total_rows}")
        return validation_summary

    @task()
    def train_models_task(extract_result, validation_summary):
        """
        Train multiple forecasting/regression models using generated sales data.

        Workflow:
        1. Load and combine sales data from multiple parquet files.
        2. Aggregate transaction-level data into daily sales metrics.
        3. Enrich sales data with promotion information.
        4. Enrich sales data with customer traffic and holiday information.
        5. Prepare store-level daily sales dataset.
        6. Split data into train/validation/test sets.
        7. Train and evaluate multiple ML models using Optuna optimization.
        8. Log metrics, charts, and artifacts to MLflow.
        9. Return serializable training results and MLflow run information.

        Args:
            extract_result (dict):
                Output from extract_data_task containing generated file paths.

            validation_summary (dict):
                Output from validate_data_task.
                Currently not used directly but included for task dependency tracking.

        Returns:
            dict:
                {
                    "training_results": {
                        "<model_name>": {
                            "metrics": {...}
                        }
                    },
                    "mlflow_run_id": "<run_id>"
                }
        """

        file_paths = extract_result["file_paths"]

        # Load sales data from multiple parquet files
        logger.info("Loading sales data from multiple files...")
        sales_dfs = []
        max_files = 50
        for i, sales_file in enumerate(file_paths["sales"][:max_files]):
            df = pd.read_parquet(sales_file)
            sales_dfs.append(df)
            if (i + 1) % 10 == 0:
                logger.debug(f"  Loaded {i + 1} files...")
        sales_df = pd.concat(sales_dfs, ignore_index=True)
        logger.info(f"Combined sales data shape: {sales_df.shape}")

        # Aggregate transaction-level sales into daily product-store sales
        daily_sales = (
            sales_df.groupby(["date", "store_id", "product_id", "category"])
            .agg(
                {
                    "quantity_sold": "sum",
                    "revenue": "sum",
                    "cost": "sum",
                    "profit": "sum",
                    "discount_percent": "mean",
                    "unit_price": "mean",
                }
            ).reset_index()
        )

        # Rename revenue column to sales for model training consistency
        daily_sales = daily_sales.rename(columns={"revenue": "sales"})

        # Merge promotion information
        # Create a binary feature indicating whether a product
        # was under promotion on a specific day
        if file_paths.get("promotions"):
            promo_df = pd.read_parquet(file_paths["promotions"][0])
            promo_summary = (
                promo_df.groupby(["date", "product_id"])["discount_percent"]
                .max()
                .reset_index()
            )
            promo_summary["has_promotion"] = 1
            daily_sales = daily_sales.merge(
                promo_summary[["date", "product_id", "has_promotion"]],
                on=["date", "product_id"],
                how="left",
            )
            daily_sales["has_promotion"] = daily_sales["has_promotion"].fillna(0)

        # Merge customer traffic and holiday features
        # These features may help explain sales fluctuations
        if file_paths.get("customer_traffic"):
            traffic_dfs = []
            for traffic_file in file_paths["customer_traffic"][:10]:
                traffic_dfs.append(pd.read_parquet(traffic_file))
            traffic_df = pd.concat(traffic_dfs, ignore_index=True)
            traffic_summary = (
                traffic_df.groupby(["date", "store_id"])
                .agg({"customer_traffic": "sum", "is_holiday": "max"})
                .reset_index()
            )
            daily_sales = daily_sales.merge(
                traffic_summary, on=["date", "store_id"], how="left"
            )
        logger.info(f"Final training data shape: {daily_sales.shape}")
        logger.info(f"Columns: {daily_sales.columns.tolist()}")
        trainer = ModelTrainer()

        # Create store-level daily sales dataset
        # Product-level data is aggregated to store-day granularity
        store_daily_sales = (
            daily_sales.groupby(["date", "store_id"])
            .agg(
                {
                    "sales": "sum",
                    "quantity_sold": "sum",
                    "profit": "sum",
                    "has_promotion": "mean",
                    "customer_traffic": "first",
                    "is_holiday": "first",
                }
            )
            .reset_index()
        )
        store_daily_sales["date"] = pd.to_datetime(store_daily_sales["date"])

        # Generate train, validation, and test datasets
        train_df, val_df, test_df = trainer.prepare_data(
            store_daily_sales,
            target_col="sales",
            date_col="date",
            group_cols=["store_id"],
            categorical_cols=["store_id"],
        )
        logger.info(f"Train shape: {train_df.shape}, Val shape: {val_df.shape}, Test shape: {test_df.shape}")

        # Train all configured models with Optuna hyperparameter tuning
        results = trainer.train_all_models(
            train_df, val_df, test_df, target_col="sales", use_optuna=True
        )

        # Log model evaluation metrics
        for model_name, model_results in results.items():
            if "metrics" in model_results:
                logger.info(f"\n{model_name} metrics:")
                for metric, value in model_results["metrics"].items():
                    logger.debug(f"  {metric}: {value:.4f}")

        # MLflow artifacts generated during training
        logger.info("\nVisualization charts have been generated and saved to MLflow/MinIO")
        logger.info("Charts include:")
        logger.info("  - Model metrics comparison")
        logger.info("  - Predictions vs actual values")
        logger.info("  - Residuals analysis")
        logger.info("  - Error distribution")
        logger.info("  - Feature importance comparison")

        # Keep only serializable outputs for Airflow/XCom compatibility
        serializable_results = {}
        for model_name, model_results in results.items():
            serializable_results[model_name] = {
                "metrics": model_results.get("metrics", {})
            }
        import mlflow

        # Retrieve active MLflow run ID
        current_run_id = (
            mlflow.active_run().info.run_id if mlflow.active_run() else None
        )

        # Return training summary and MLflow tracking information
        return {
            "training_results": serializable_results,
            "mlflow_run_id": current_run_id,
        }

    @task()
    def evaluate_models_task(training_result):
        """
        Evaluate trained models and identify the best-performing model.

        This task compares model evaluation metrics, selects the model
        with the lowest RMSE, and retrieves the corresponding best MLflow
        run information for downstream deployment or reporting.

        Parameters
        ----------
        training_result : dict
            Output from train_models_task containing model metrics and
            MLflow tracking information.

        Returns
        -------
        dict
            Dictionary containing:

            - best_model : Name of the best-performing model
            - best_run_id : MLflow run ID associated with the best model
        """

        # Retrieve model training results
        results = training_result["training_results"]

        # MLflow utility for querying experiment results
        mlflow_manager = MLflowManager()

        # Track best model based on RMSE
        best_model_name = None
        best_rmse = float("inf")

        # Compare model performance
        # Lower RMSE indicates better predictive accuracy
        for model_name, model_results in results.items():
            if "metrics" in model_results and "rmse" in model_results["metrics"]:
                if model_results["metrics"]["rmse"] < best_rmse:
                    best_rmse = model_results["metrics"]["rmse"]
                    best_model_name = model_name
        logger.info(f"Best model: {best_model_name} with RMSE: {best_rmse:.4f}")

        # Retrieve the best MLflow run based on RMSE ranking
        best_run = mlflow_manager.get_best_model(metric="rmse", ascending=True)

        # Return model selection results for downstream tasks
        return {"best_model": best_model_name, "best_run_id": best_run["run_id"]}

    @task()
    def register_best_model_task(evaluation_result):
        """
        Register trained models in the MLflow Model Registry.

        This task registers model artifacts from the selected MLflow run
        and stores the generated model version information for downstream
        deployment or promotion workflows.

        Parameters
        ----------
        evaluation_result : dict
            Output from evaluate_models_tasks containing:

            - best_model : Name of the best-performing model
            - best_run_id : MLflow run ID

        Returns
        -------
        dict
            Mapping between model names and their registered versions.

            Example:
            {
                "xgboost": 1,
                "lightgbm": 2
            }
        """

        # Retrieve model evaluation results
        evaluation_result["best_model"]
        run_id = evaluation_result["best_run_id"]

        # MLflow registry utility
        mlflow_manager = MLflowManager()
        model_versions = {}

         # Register model artifacts from the selected MLflow run
        for model_name in ["xgboost", "lightgbm"]:
            version = mlflow_manager.register_model(run_id, model_name, model_name)
            model_versions[model_name] = version
            logger.debug(f"Registered {model_name} version: {version}")
        logger.info(f"Model registration completed using run_id={run_id}")
        return model_versions

    @task()
    def transition_to_production_task(model_versions):
        """
        Promote registered models to the Production stage in MLflow.

        This task updates the lifecycle stage of registered model versions
        from Staging (or None) to Production, making them available for
        deployment and inference workloads.

        Parameters
        ----------
        model_versions : dict
            Output from register_best_model_task containing model names
            and their corresponding registered versions.

            Example:
            {
                "xgboost": 3,
                "lightgbm": 2
            }

        Returns
        -------
        str
            Status message indicating successful model promotion.
        """

        # MLflow registry utility
        mlflow_manager = MLflowManager()

        # Promote each registered model version to Production
        for model_name, version in model_versions.items():
            mlflow_manager.transition_model_stage(model_name, version, "Production")
            logger.debug(f"Transitioned {model_name} v{version} to Production")
        return "Models transitioned to production"

    @task()
    def generate_performance_report_task(training_result, validation_summary):
        """
        Generate a consolidated model performance report.

        This task combines data validation statistics and model evaluation
        metrics into a single report for monitoring, auditing, and
        downstream reporting purposes.

        Parameters
        ----------
        training_result : dict
            Output from train_models_task containing model metrics.

        validation_summary : dict
            Output from validate_data_task containing dataset validation
            statistics and detected data quality issues.

        Returns
        -------
        dict
            Consolidated performance report containing:

            - timestamp
            - data_summary
            - model_performance
        """

        # Retrieve model training results
        results = training_result["training_results"]

        # Build report structure
        report = {
            "timestamp": datetime.now().isoformat(),
            "data_summary": {
                "total_rows": (
                    validation_summary.get("total_rows", 0) if validation_summary else 0
                ),
                "files_validated": (
                    validation_summary.get("total_files_validated", 0)
                    if validation_summary
                    else 0
                ),
                "issues_found": (
                    validation_summary.get("issues_found", 0)
                    if validation_summary
                    else 0
                ),
                "issues": (
                    validation_summary.get("issues", []) if validation_summary else []
                ),
            },
            "model_performance": {},
        }

        # Add model evaluation metrics
        if results:
            for model_name, model_results in results.items():
                if "metrics" in model_results:
                    report["model_performance"][model_name] = model_results["metrics"]

        # Persist report as JSON artifact
        import json
        with open("/tmp/performance_report.json", "w") as f:
            json.dump(report, f, indent=2)
        logger.info("Performance report generated")
        logger.info(f"Models trained: {list(report['model_performance'].keys())}")
        return report

    # Task dependencies using function calls
    extract_result = extract_data_task()
    validation_summary = validate_data_task(extract_result)
    training_result = train_models_task(extract_result, validation_summary)
    evaluation_result = evaluate_models_task(training_result)
    model_versions = register_best_model_task(evaluation_result)
    transition = transition_to_production_task(model_versions)
    report = generate_performance_report_task(training_result, validation_summary)
    cleanup = BashOperator(
        task_id="cleanup",
        bash_command="rm -rf /tmp/sales_data /tmp/performance_report.json || true",
    )
    report >> cleanup


sales_forecast_training_dag = sales_forecast_training()
