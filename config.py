import configparser

# using a config.py file to reduce repetition of code for reading dwh.cfg.
# source:
# https://docs.python.org/3/faq/programming.html#how-do-i-share-global-variables-across-modules

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
PORT = config.get('REDSHIFT', 'DWH_PORT')
IAM_ROLE = config.get('REDSHIFT', 'DWH_IAM_ROLE_NAME')
ENDPOINT = config.get('REDSHIFT', 'DWH_ENDPOINT')

IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
