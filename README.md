# **SUMMARY**

steps:

1. create redshift cluster for data warehousing

- add code for creating cluster (redshift_functions.py, create_cluster.py) [DONE]
- add code for describing cluster (status, properties) (redshift_functions.py) [DONE]
- add code for opening tcp port to access cluster endpoint (redshift_functions.py) [DONE]
- add code for testing cluster connection (redshift_functions.py)
- add code for deleting cluster (redshift_functions.py) [DONE]

2. extract metadata and logs files from S3 bucket to redshift staging
3. load data from reshift staging to redshift dwh
4. run queries given by analytics team to prove successful etl pipeline construction

create_table.py responsible for aws redshift schema creation
sql_queries.py responsible for housing SQL queries for creation, insertion, selection, and deletion

# **INSTRUCTIONS**

- [console, possibly create_cluster.py] create a redshift cluster on AWS via the redshift console
- [dwh_interface.ipynb] verify that the cluster is available, needs to be easily callable
- [dwh_interface.ipynb] ensure redshift is permitted to copy from s3 to its database
- [create_tables.py, sql_queries.py] create tables in redshift cluster
- [etl.py, sql_queries.py] copy contents of s3 bucket to staging tables in redshift

- [code] verify that the tables have been filled properly
- [code] insert data from staging tables to production tables
- [code] verify that tables have been filled properly
- [code] run queries by analytics team to validate etl pipeline construction
- [console] delete the cluster (avoid extra charges)

# **MANIFEST**

- create cluster [might delete this]
- create_table.py
- etl.py
- README.md
- sql_queries.py
