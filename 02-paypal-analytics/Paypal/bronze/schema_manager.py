import logging
from config.db import get_connection
from config.config import *

def create_database(database_name: str, sql_server: str, logger: logging.Logger) -> None:
    """
    Create a SQL Server database (drop if exists).

    Parameters
    ----------
    database_name : str
        Name of the database to create.
    sql_server : str
        SQL Server instance or hostname.
    logger : logging.Logger
        Logger instance used for execution and error logging.
    """

    logger.info("Starting database creation | database=%s", database_name)

    # Connect to SQL Server master database
    # Database creation must be executed from the master context
    conn = get_connection(database="master", sql_server=sql_server, autocommit=True)
    cursor = conn.cursor()

    try:
        # Drop database if it already exists
        logger.info("Checking existing database and dropping if exists")

        drop_sql = f"""
        IF EXISTS (SELECT 1 FROM sys.databases WHERE name = '{database_name}')
        BEGIN
            ALTER DATABASE {database_name} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE {database_name};
        END
        """

        cursor.execute(drop_sql)
        conn.commit()

        logger.info("Existing database dropped (if present)")

        # Create new database
        logger.info("Creating database | database=%s", database_name)

        create_sql = f"CREATE DATABASE {database_name};"

        cursor.execute(create_sql)
        conn.commit()

        logger.info("Database created successfully | database=%s", database_name)

    except Exception as e:
        logger.exception("Database creation failed | database=%s | error=%s", database_name, e)
        raise

    finally:
        # Release database resources
        cursor.close()
        conn.close()
        logger.debug("Database connection closed")

def create_schema(
    database_name: str,
    sql_server: str,
    schema_name: str,
    logger: logging.Logger
) -> None:
    """
    Create a schema in a SQL Server database if it does not exist.

    Parameters
    ----------
    database_name : str
        Target database name.
    sql_server : str
        SQL Server instance or hostname.
    schema_name : str
        Schema name to create.
    logger : logging.Logger
        Logger instance used for execution and error logging.
    """

    logger.info(
        "Starting schema creation | database=%s schema=%s",
        database_name,
        schema_name
    )

    # Connect to the target database
    conn = get_connection(database=database_name, sql_server=sql_server, autocommit=True)
    cursor = conn.cursor()

    try:
        # Check if schema exists and create if missing
        logger.info("Checking schema existence | schema=%s", schema_name)

        sql = f"""
        IF NOT EXISTS (
            SELECT 1
            FROM sys.schemas
            WHERE name = '{schema_name}'
        )
        EXEC('CREATE SCHEMA {schema_name}')
        """

        cursor.execute(sql)
        conn.commit()

        logger.info(
            "Schema ensured successfully | database=%s schema=%s",
            database_name,
            schema_name
        )

    except Exception as e:
        logger.exception(
            "Schema creation failed | database=%s schema=%s | error=%s",
            database_name,
            schema_name,
            e
        )
        raise

    finally:
        # Release database resources
        cursor.close()
        conn.close()

        logger.debug("Database connection closed")

def create_table(
    database_name: str,
    sql_server: str,
    schema_name: str,
    table_name: str,
    sql: str,
    logger: logging.Logger
) -> None:
    """
    Create a Bronze table in SQL Server (drop if exists).

    Parameters
    ----------
    database_name : str
        Target database name.
    sql_server : str
        SQL Server instance or hostname.
    schema_name : str
        Schema containing the table.
    table_name : str
        Table name to create.
    sql : str
        SQL statement used to create the table.
    logger : logging.Logger
        Logger instance used for execution and error logging.
    """

    logger.info(
        "Starting table creation | database=%s schema=%s table=%s",
        database_name,
        schema_name,
        table_name
    )

    # Establish connection to the target database
    conn = get_connection(database=database_name, sql_server=sql_server, autocommit=True)
    cursor = conn.cursor()

    try:
        logger.info(
            "Dropping existing table if exists | schema=%s table=%s",
            schema_name,
            table_name
        )

        # Execute table creation statement
        cursor.execute(sql)
        conn.commit()

        logger.info(
            "Table created successfully | schema=%s table=%s",
            schema_name,
            table_name
        )

    except Exception as e:
        logger.exception(
            "Table creation failed | schema=%s table=%s | error=%s",
            schema_name,
            table_name,
            e
        )
        raise

    finally:
        # Release database resources
        cursor.close()
        conn.close()

        logger.debug("Database connection closed")
