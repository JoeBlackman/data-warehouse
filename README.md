# **SUMMARY**

steps:

1. extract metadata and logs files from S3 bucket to redshift staging
2. load data from reshift staging to redshift dwh
3. run queries given by analytics team to prove successful etl pipeline construction

create_table.py responsible for aws redshift schema creation
sql_queries.py responsible for housing SQL queries for creation, insertion, selection, and deletion

ER_Diagram goes here

# **INSTRUCTIONS**

# **MANIFEST**

- create_table.py
- etl.py
- README.md
- sql_queries.py
