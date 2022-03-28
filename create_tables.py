"""
The create_tables.py module will be used to delete existing tables in a specified cluster,
then create the staging and star schema tables used by our ETL logic.
"""

import boto3
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    REGION = config.get('AWS', 'REGION')
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    CLUSTER_ID = config.get('REDSHIFT', 'DWH_CLUSTER_IDENTIFIER')
    DB_NAME = config.get('REDSHIFT', 'DWH_DB')
    USER = config.get('REDSHIFT', 'DWH_DB_USER')
    PW = config.get('REDSHIFT', 'DWH_DB_PASSWORD')
    PORT = config.get('REDSHIFT', 'DWH_PORT')

    redshift_client = boto3.client(
        'redshift',
        region_name=REGION,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )
    HOST = redshift_client.describe_clusters(ClusterIdentifier=CLUSTER_ID)[
        'Clusters'][0]['Endpoint']['Address']

    conn = psycopg2.connect(
        f"host={HOST} dbname={DB_NAME} user={USER} password={PW} port={PORT}")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
