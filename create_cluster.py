"""
This module will programmatically create a redshift cluster and store it's endpoint for later usage.
"""
import boto3
import configparser


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    REGION = config.get('AWS', 'REGION')
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    CLUSTER_TYPE = config.get('REDSHIFT', 'DWH_CLUSTER_TYPE')
    NUM_NODES = config.get('REDSHIFT', 'DWH_NUM_NODES')
    NODE_TYPE = config.get('REDSHIFT', 'DWH_NODE_TYPE')
    CLUSTER_ID = config.get('REDSHIFT', 'DWH_CLUSTER_IDENTIFIER')
    DB_NAME = config.get('REDSHIFT', 'DWH_DB')
    USER = config.get('REDSHIFT', 'DWH_DB_USER')
    PW = config.get('REDSHIFT', 'DWH_DB_PASSWORD')
    # PORT = config.get('REDSHIFT', 'DWH_PORT')
    IAM_ROLE = config.get('IAM_ROLE', 'ARN')

    try:
        redshift_client = boto3.client(
            'redshift',
            region_name=REGION,
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
        )
        redshift_client.create_cluster(
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUM_NODES),
            DBName=DB_NAME,
            ClusterIdentifier=CLUSTER_ID,
            MasterUsername=USER,
            MasterUserPassword=PW,
            IamRoles=[IAM_ROLE]
        )
    except Exception as error:
        print(error)

    print(redshift_client.describe_clusters(
        ClusterIdentifier=CLUSTER_ID)['Clusters'][0])


if __name__ == "__main__":
    main()
