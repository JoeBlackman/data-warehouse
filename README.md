# **SUMMARY**

The purpose of this project is to leverage AWS and data warehouse concepts to build and ETL pipeline. The ETL pipeline will copy data from S3 to staging tables in Redshift, then fill the analytics tables from the staging tables. We'll know if the ETL is working correctly if queries given by the analytics team return expected results about user activity and song plays when run against the analytics tables in Redshift.

# **INSTRUCTIONS**

- create a redshift user with programmatic access and the following permissions:
    - AmazonSSMFullAccess
    - Administrator Access
    - AmazonRedshiftFullAccess
    - Amazons3ReadOnlyAccess
- store the key and secret for the redshift user for the next step
- [dwh.cfg] set key and secret using what you stored from the previous step and save the file
- [dwh_interface.ipynb] create a redshift cluster on AWS by running the appropriate cells in dwh_interface.ipynb
- [dwh_interface.ipynb] verify that the cluster is available by running the appropriate cells in dwh_interface.ipynb
- [dwh_interface.ipynb] ensure redshift is permitted to copy from s3 to its database
- [create_tables.py, sql_queries.py] create tables in redshift cluster by running create_tables.py
- [etl.py, sql_queries.py] copy contents of s3 bucket to staging tables in redshift and insert that data into analysis tables by running etl.py
- [dwh_interface.ipynb] verify that tables have been filled properly by running queries from dwh_interface.ipynb
- [dwh_interface.ipynb] delete the cluster (avoid extra charges) by running the appropriate cells in dwh_interface.ipynb

# **MANIFEST**

- config.py
    - code config file for accessing constants
- create_table.py
    - script for dropping old staging and analysis tables in redshift, then creating them
- dwh.cfg
    - text config file with IAM user, Redshift, and S3 settings and locations. To be read by config.py and dwh_interface.ipynb
- dwh_interface.ipynb
    - dashboard for setting up and tearing down redshift cluster
- etl.py 
    - script for extracting data from S3, loading it into staging tables in redshift, and transforming the data by loading it into analysis tables in redshift
- README.md
    - this file
- sql_queries.py
    - a store of query strings used by the etl.py script
- TODO.md
    - keeps rough status of work
- analysis_star_schema.odg
    - rough schema diagram
- analysis_star_schema.pdf
    - rough schema diagram
- scratch.ipynb
    - scratch pad for quick tests of code