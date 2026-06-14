"""
Simple service discovery for MLflow and MinIO endpoints
"""

import os
from typing import Optional
from utils.logger import logger

def get_mlflow_endpoint() -> Optional[str]:
    """
    Discover the most suitable MLflow tracking endpoint.

    The function first checks environment variables, then attempts
    to connect to a list of candidate endpoints based on the current
    execution environment (containerized or local).

    Returns
    -------
    Optional[str]
        Reachable MLflow tracking URI if found. Otherwise, returns
        the most likely default endpoint for the current environment.

    Notes
    -----
    Endpoint discovery order:

    1. MLFLOW_TRACKING_URI environment variable
    2. Container-specific endpoints (if running in Docker)
    3. Localhost endpoints
    4. Environment-specific fallback endpoint
    """

    # Check if explicitly set in environment
    env_uri = os.getenv('MLFLOW_TRACKING_URI')
    if env_uri:
        return env_uri
    
    # Check if we're in a container by looking for common container indicators
    in_container = os.path.exists('/.dockerenv') or os.environ.get('AIRFLOW__CORE__EXECUTOR')
    
    # Order endpoints based on environment
    if in_container:
        # In container, prioritize service names
        endpoints = [
            'http://mlflow:5001',
            'http://host.docker.internal:5001',
            'http://172.17.0.1:5001',  # Default Docker bridge
            'http://localhost:5001'
        ]
    else:
        # Outside container, prioritize localhost
        endpoints = [
            'http://localhost:5001',
            'http://127.0.0.1:5001',
            'http://host.docker.internal:5001'
        ]

    import urllib.request
    for endpoint in endpoints:
        try:
            # Try to actually connect, not just resolve DNS
            req = urllib.request.Request(f"{endpoint}/health")
            with urllib.request.urlopen(req, timeout=2) as response:
                if response.getcode() == 200:
                    logger.info(f"MLflow is accessible at: {endpoint}")
                    return endpoint
        except Exception as e:
            logger.debug(f"MLflow not accessible at {endpoint}: {str(e)}")
            continue

    # If nothing works, return the most likely default based on environment
    default = 'http://mlflow:5001' if in_container else 'http://localhost:5001'
    logger.warning(f"Could not connect to MLflow, using default: {default}")
    return default

def get_minio_endpoint() -> Optional[str]:
    """
    Discover the most suitable MinIO S3 endpoint.

    The function attempts to locate an accessible MinIO instance
    by checking environment variables and probing a set of known
    endpoints using MinIO's health endpoint.

    Returns
    -------
    Optional[str]
        Reachable MinIO endpoint URL if found. Otherwise, returns
        the most likely default endpoint for the current environment.

    Notes
    -----
    Endpoint discovery order:

    1. MLFLOW_S3_ENDPOINT_URL environment variable
    2. Container-specific endpoints (if running in Docker)
    3. Localhost endpoints
    4. Environment-specific fallback endpoint
    """

    # Check if explicitly set in environment
    env_url = os.getenv('MLFLOW_S3_ENDPOINT_URL')
    if env_url:
        return env_url
    
    # Check if we're in a container
    in_container = os.path.exists('/.dockerenv') or os.environ.get('AIRFLOW__CORE__EXECUTOR')
    
    # Order endpoints based on environment
    if in_container:
        # In container, prioritize service names
        endpoints = [
            'http://minio:9000',
            'http://host.docker.internal:9000',
            'http://172.17.0.1:9000',  # Default Docker bridge
            'http://localhost:9000'
        ]
    else:
        # Outside container, prioritize localhost
        endpoints = [
            'http://localhost:9000',
            'http://127.0.0.1:9000',
            'http://host.docker.internal:9000'
        ]

    import urllib.request
    for endpoint in endpoints:
        try:
            # Try to actually connect, not just resolve DNS
            req = urllib.request.Request(f"{endpoint}/minio/health/live")
            with urllib.request.urlopen(req, timeout=2) as response:
                if response.getcode() == 200:
                    logger.info(f"MinIO is accessible at: {endpoint}")
                    return endpoint
        except Exception as e:
            logger.debug(f"MinIO not accessible at {endpoint}: {str(e)}")
            continue

    # If nothing works, return the most likely default based on environment
    default = 'http://minio:9000' if in_container else 'http://localhost:9000'
    logger.warning(f"Could not connect to MinIO, using default: {default}")
    return default


# Backward compatibility
def get_mlflow_uri() -> str:
    """
    Backward-compatible wrapper for MLflow endpoint discovery.

    Returns
    -------
    str
        MLflow tracking URI.
    """
    return get_mlflow_endpoint()

def get_minio_url() -> str:
    """
    Backward-compatible wrapper for MinIO endpoint discovery.

    Returns
    -------
    str
        MinIO endpoint URL.
    """
    return get_minio_endpoint()
