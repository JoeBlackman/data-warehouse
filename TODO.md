# **TODO**

1. create redshift cluster for data warehousing

- add code for creating cluster (redshift_functions.py, create_cluster.py) [DONE]
- add code for describing cluster (status, properties) (redshift_functions.py) [DONE]
- add code for opening tcp port to access cluster endpoint (redshift_functions.py) [DONE]
- add code for testing cluster connection (redshift_functions.py) [DONE]
- add code for deleting cluster (redshift_functions.py) [DONE]

2. extract metadata and logs files from S3 bucket to redshift staging

- add code for creating staging tables [DONE]
- add code for running redshift copy [DONE]
- add code for testing proper insertion of data into staging tables [DONE]

3. load data from reshift staging to redshift dwh

- add code for creating production tables [DONE]
- add code for inserting data from staging tables into analytics tables [DONE]
- add code for testing that data was properly inserted into analytics tables []

4. run queries given by analytics team to prove successful etl pipeline construction

create_table.py responsible for aws redshift schema creation
sql_queries.py responsible for housing SQL queries for creation, insertion, selection, and deletion
