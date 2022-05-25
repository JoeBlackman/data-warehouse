"""
The create_tables.py module will be used to delete existing tables in a specified cluster,
then create the staging and star schema tables used by our ETL logic.
"""

import boto3
import configparser
from datetime import datetime, date
import logging
import pathlib
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import sys

import config


logging.basicConfig(
    filename=pathlib.Path('logs', f'{date.today()}.log'),
    encoding='utf-8',
    level=logging.DEBUG,
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)


def drop_tables(cur, conn):
    """
    Drop tables in redshift if they exist
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            logging.info("Successfully ran drop_table query.")
        except Exception as e:
            logging.error(f"An error occurred while running query: {query}")
            logging.error(f"Error: {e}")
            sys.exit(1)


def create_tables(cur, conn):
    """
    Create tables in redshift
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            logging.info("Successfully ran create_table query.")
        except Exception as e:
            logging.error(f"An error occurred while running query: {query}")
            logging.error(f"Error: {e}")
            sys.exit(1)


def main():
    """
    Entry point when create_tables.py is run
    """
    start_time = datetime.now()
    logging.info(f'create_tables run started at {start_time}')

    # connect to redshift
    conn = psycopg2.connect(f"""
        host={config.ENDPOINT}
        dbname={config.DB_NAME}
        user={config.USER}
        password={config.PW}
        port={config.PORT}
    """)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    logging.info(f'Completed create_tables run started at {start_time}')

if __name__ == "__main__":
    main()
