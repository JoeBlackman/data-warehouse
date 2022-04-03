"""
The create_tables.py module will be used to delete existing tables in a specified cluster,
then create the staging and star schema tables used by our ETL logic.
"""

import boto3
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

import config


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    redshift_client = boto3.client(
        'redshift',
        region_name=config.REGION,
        aws_access_key_id=config.KEY,
        aws_secret_access_key=config.SECRET
    )
    HOST = redshift_client.describe_clusters(ClusterIdentifier=config.CLUSTER_ID)[
        'Clusters'][0]['Endpoint']['Address']

    conn = psycopg2.connect(
        f"host={HOST} dbname={config.DB_NAME} user={config.USER} password={config.PW} port={config.PORT}")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
