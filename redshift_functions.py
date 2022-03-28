"""
Module for interfacing with AWS redshift
"""
import boto3


def create_cluster(client, cluster_type, node_type, num_of_nodes, cluster_id, db_name,
                   master_un, master_pw, iamroles):
    """
    Creates a redshift cluster given the specified arguments

    args
    :region:
    :key:
    :secret:
    :ClusterType:
    :NodeType:
    :NumberOfNodes:
    :DBName:
    :ClusterIdentifier:
    :MasterUserName:
    :MasterUserPassword:
    :IamRoles: a list of iam roles
    """

    try:
        return client.create_cluster(
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=int(num_of_nodes),
            DBName=db_name,
            ClusterIdentifier=cluster_id,
            MasterUsername=master_un,
            MasterUserPassword=master_pw,
            IamRoles=iamroles
        )
    except Exception as error:
        print(error)
        return None


def delete_cluster(client, cluster_identifier):
    """
    Deletes a redshift cluster given it's cluster identifier and proper credentials

    args
    :region:
    :key:
    :secret:
    :cluster_identifier:
    """
    try:
        return client.delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalclusterSnapshot=True
        )
    except Exception as error:
        print(error)
        return None


def enable_cluster_access(vpcId, port):
    """
    open incoming TCP port to acccess cluster endpoint

    args
    :vpcId: identifier for vitual private cloud, usually returned by describe_clusters()
    :port: desired port number to open
    """
    vpc = boto3.ec2.Vpc(id=vpcId)
    defaultSg = list(vpc.security_groups.all())[0]
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(port),
        ToPort=int(port)
    )


def describe_cluster(client, cluster_id):
    return client.describe_clusters(ClusterIdentifier=cluster_id)['Clusters'][0]


def test_connection():
    pass
