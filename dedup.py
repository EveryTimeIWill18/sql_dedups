#!ve/bin/python3
"""Delete duplicated records from the CallTraceAttempts table in the
LifelineV2 database.
"""
import configparser
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy.text import text

# --- configuration variables
CONFIG_INI = "config.ini"
DATABASE_SERVER = "10.1.1.12"
DB_NAME = "LifelineV2"

def load_credentials() -> tuple:
    """Loads credentials from configuration file"""
    config = configparser.ConfigParser()
    if not len(config.read(CONFIG_INI)):
        raise ValueError("Database config does not exist.")
    try:
        username = config.get('credentials', 'DB_USERNAME')
        password = config.get('credentials', 'DB_PASSWORD')
    except Exception as e:
        raise e
    return (username, password)

def get_database_connection(creds):
    """Returns a SQLAlchemy connection to the database."""
    username, password = creds
    available_drivers = pyodbc.drivers()
    effective_drivers = filter(lambda x: x in driver_priority,
                               available_drivers)
    # --- find the correct driver for sql-server 2014
    # --- Must have sql odbc driver 2014 to connect
    driver_priority = {
                       "ODBC Driver 17 for SQL Server":   7,
                       "ODBC Driver 13.1 for SQL Server": 6,
                       "ODBC Driver 13 for SQL Server":   5,
                       "ODBC Driver 11 for SQL Server":   4,
                       "SQL Server Native Client 11.0":   3,
                       "SQL Server Native Client 10.0":   2,
                       "SQL Server":                      1
                      }
    try:
        best_driver = max(effective_drivers,
                          key=(lambda x: driver_priority[x])
    except ValueError as e:
        raise e
    try:
        engine = create_engine("mssql+pyodbc://"
                            + username + ":" + password
                            + "@" + DATABASE_SERVER + "/" + DB_NAME
                            + "?driver={}".format(best_driver))
    except Exception as e:
        raise e

    return engine


def query_dedup(eng):
    """Query the duplicated records and store them in a logfile"""
    try:
        f = open("cta_dedup_recs_select.sql", 'r')
    except FileExistsError as e:
        raise e
    try:
        connection = engine.connect()
    except Exception as e:
        raise e

    # --- execute the query
    result_proxy = connection.execute(f.read())






def delete_dups(engn):
    """Delete duplicated records from the CallTraceAttempts table."""
    try:
        f = open("cta_dedup_recs_delete.sql", 'r')
    except FileNotFoundError as e:
        raise e

    # -- creates a SQLAlchemy text object and compiles it
    query = text(f.read()).compile(engn)
    f.close()
