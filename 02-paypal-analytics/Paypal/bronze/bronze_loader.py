import pandas as pd
import logging
from config.db import get_connection

def load_csv(path: str) -> pd.DataFrame:
    """
    Load a CSV file into a pandas DataFrame for raw ingestion.

    Parameters
    ----------
    path : str
        Path to the CSV file.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing raw CSV data.

    Notes
    -----
    - No transformations are applied.
    - Intended for Bronze layer ingestion.
    """
    return pd.read_csv(path)


def insert_data(
    df: pd.DataFrame,
    database_name: str,
    sql_server: str,
    schema_name: str,
    table_name: str,
    insert_sql: str,
    batch_size: int,
    logger: logging.Logger
) -> None:
    """
    Insert a pandas DataFrame into a SQL Server table using batch execution.

    Parameters
    ----------
    df : pandas.DataFrame
        Source data to insert.
    database_name : str
        Target database name.
    sql_server : str
        SQL Server instance or hostname.
    schema_name : str
        Target schema name.
    table_name : str
        Destination table name.
    insert_sql: str
        SQL statement used to insert data into the table.
    batch_size : int
        Number of rows per batch insert.
    logger : logging.Logger
        Logger instance used for progress and error logging.
    """

    logger.info("Starting data insert | database=%s schema=%s table=%s rows=%s", database_name, schema_name, table_name, len(df))

    # Create database connection and cursor
    conn = get_connection(database=database_name, sql_server=sql_server, autocommit=True)
    cursor = conn.cursor()

    # convert NaN -> None
    df = df.astype(object).where(pd.notnull(df), None)

    # Enable high-performance batch execution
    # This significantly improves insert performance with pyodbc
    cursor.fast_executemany = True

    # Total number of rows to insert
    total_rows = len(df)

    try:
        # Iterate through DataFrame in batches to control memory usage
        for i in range(0, total_rows, batch_size):

            # Slice DataFrame batch and convert to list of row values
            # executemany() expects an iterable of tuples/lists
            batch = df.iloc[i:i + batch_size].values.tolist()

            # Execute batch insert
            cursor.executemany(insert_sql, batch)

            logger.info("Batch inserted | start_row=%s end_row=%s batch_size=%s", i, i + len(batch), len(batch))

        # Commit transaction after all batches are successfully inserted
        conn.commit()
        logger.info("Insert completed successfully")

    except Exception as e:
        logger.exception("Insert failed: %s", e)
        # Rollback transaction to avoid partial writes
        conn.rollback()
        raise

    finally:
        # Always release database resources
        cursor.close()
        conn.close()
        logger.debug("Database connection closed")
