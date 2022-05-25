from datetime import datetime, date
import logging
import pathlib
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import sys

import config


# initialize logging
logging.basicConfig(
    filename=pathlib.Path('logs', f'{date.today()}.log'),
    encoding='utf-8',
    level=logging.DEBUG,
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)

def load_staging_tables(cur, conn):
    """
    Extract data from S3 and load into redshift staging tables
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            logging.info("Successfully ran load_staging table_query.")
        except Exception as e:
            logging.error("An error occurred while loading staging tables.")
            logging.error(f"Error: {e}")
            sys.exit(1)


def insert_tables(cur, conn):
    """
    Transform data by carefully loading it from staging tables into analytics tables
    and applying data transformations wherever necessary (e.g. timestamps)
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            logging.info("Successfully ran insert_table query.")
        except Exception as e:
            logging.error(f"An error occurred while loading analysis tables.")
            logging.error(f"Error: {e}")
            sys.exit(1)


def main():
    """
    Entry point when etl.py is run
    """
    etl_start_time = datetime.now()
    logging.info(f'Starting ETL run at {etl_start_time}')

    # connect to redshift
    conn = psycopg2.connect(f"""
        host={config.ENDPOINT}
        dbname={config.DB_NAME}
        user={config.USER}
        password={config.PW}
        port={config.PORT}
    """)
    cur = conn.cursor()
    
    # Extract data from S3 and load into redshift staging tables
    load_staging_tables(cur, conn)

    # Transform data by carefully loading it from staging tables into analytics tables
    insert_tables(cur, conn)

    # close redshift connection
    conn.close()
    logging.info(f'End of ETL that began at {etl_start_time}')


if __name__ == "__main__":
    main()