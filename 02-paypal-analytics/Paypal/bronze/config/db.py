import pyodbc

def get_connection(database: str, sql_server: str, 
                   autocommit: bool = False) -> pyodbc.Connection:
    """
    Create a pyodbc connection to SQL Server using Windows Authentication.

    Parameters
    ----------
    database : str
        Target database name.
    sql_server : str
        SQL Server hostname or instance name.
    autocommit : bool, default False
        Enable or disable autocommit mode.

    Returns
    -------
    pyodbc.Connection
        Active SQL Server connection object.

    Notes
    -----
    - Uses ODBC Driver 17 for SQL Server.
    - Authentication via Trusted_Connection (Windows Auth).
    - TrustServerCertificate enabled for local/dev environments.
    """

    connection_string = f"""
        DRIVER={{ODBC Driver 17 for SQL Server}};
        SERVER={sql_server};
        DATABASE={database};
        Trusted_Connection=yes;
        TrustServerCertificate=yes;
    """

    return pyodbc.connect(connection_string, autocommit=autocommit)
