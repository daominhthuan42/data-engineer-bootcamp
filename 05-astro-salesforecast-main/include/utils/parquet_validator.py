import pandas as pd
from pathlib import Path
from typing import List, Tuple, Dict
from utils.logger import logger

def validate_parquet_file(file_path: str) -> Tuple[bool, str]:
    """
    Validate a parquet file by attempting to read it.

    Parameters
    ----------
    file_path : str
        Path to the parquet file.

    Returns
    -------
    Tuple[bool, str]
        A tuple containing:

        - bool : True if the file is readable.
        - str : Error message if validation fails, otherwise an empty string.

    Notes
    -----
    Validation is performed by loading the parquet file into a
    DataFrame and accessing basic metadata attributes.
    """
    try:
        # Try to read the file
        df = pd.read_parquet(file_path)
        # Access common DataFrame properties to ensure the file
        # was loaded correctly and metadata is available
        _ = df.shape
        _ = df.columns
        return True, ""
    except Exception as e:
        return False, str(e)

def find_corrupted_parquet_files(directory: str, pattern: str = "*.parquet") -> Dict[str, List[str]]:
    """
    Scan a directory recursively and identify corrupted parquet files.

    Parameters
    ----------
    directory : str
        Root directory to search.

    pattern : str, default="*.parquet"
        File pattern used to locate parquet files.

    Returns
    -------
    Dict[str, Any]
        Dictionary containing:

        - valid : List of valid parquet files.
        - corrupted : List of corrupted files and associated errors.
        - total : Total number of discovered parquet files.
        - valid_count : Number of valid files.
        - corrupted_count : Number of corrupted files.
    """
    path = Path(directory)

    # Recursively discover parquet files under the target directory
    parquet_files = list(path.rglob(pattern))

    valid_files = []
    corrupted_files = []

    for file_path in parquet_files:

        # Validate each parquet file individually
        is_valid, error = validate_parquet_file(str(file_path))

        if is_valid:
            valid_files.append(str(file_path))
        else:
            corrupted_files.append((str(file_path), error))

            # Log corrupted file details for troubleshooting
            logger.debug(f"Corrupted file found: {file_path}")
            logger.debug(f"  Error: {error}")

    return {
        "valid": valid_files,
        "corrupted": corrupted_files,
        "total": len(parquet_files),
        "valid_count": len(valid_files),
        "corrupted_count": len(corrupted_files)
    }

def safe_read_parquet(file_path: str, default=None):
    """
    Safely read a parquet file.

    Parameters
    ----------
    file_path : str
        Path to the parquet file.

    default : Any, default=None
        Value returned if the parquet file cannot be read.

    Returns
    -------
    Any
        Loaded DataFrame if successful, otherwise the specified
        default value.

    Notes
    -----
    This utility is intended for non-critical workflows where
    corrupted files should not interrupt processing.
    """
    try:
        # Attempt to load the parquet file
        return pd.read_parquet(file_path)

    except Exception as e:
        # Log the failure and return a fallback value
        logger.warning(f"Could not read parquet file '{file_path}': {e}")
        return default

if __name__ == "__main__":
    # Test the validator
    import sys
    if len(sys.argv) > 1:
        directory = sys.argv[1]
        results = find_corrupted_parquet_files(directory)
        logger.info(f"\nValidation Summary:")
        logger.info(f"Total files: {results['total']}")
        logger.info(f"Valid files: {results['valid_count']}")
        logger.info(f"Corrupted files: {results['corrupted_count']}")
