# **SUMMARY**

The purpose of this project is to leverage AWS and data warehouse concepts to build and ETL pipeline. The ETL pipeline will copy data from S3 to staging tables in Redshift, then fill the analytics tables from the staging tables. We'll know if the ETL is working correctly if queries given by the analytics team return expected results about user activity and song plays when run against the analytics tables in Redshift.

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
