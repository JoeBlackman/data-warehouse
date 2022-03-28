import boto3
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
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

    # EXTRACT
    # Staging data extracted from s3 bucket to staging database in redshift
    load_staging_tables(cur, conn)

    # TRANSFORM/LOAD
    # Data from staging database loaded into production database (star schema)
    #insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
