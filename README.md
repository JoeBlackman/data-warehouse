# **SUMMARY**

The purpose of this project is to leverage AWS and data warehouse concepts to build and ETL pipeline. The ETL pipeline will copy data from S3 to staging tables in Redshift, then fill the analytics tables from the staging tables. We'll know if the ETL is working correctly if queries given by the analytics team return expected results about user activity and song plays when run against the analytics tables in Redshift.

# **INSTRUCTIONS**

- [dwh_interface.ipynb] create a redshift cluster on AWS by running the appropriate cells in dwh_interface.ipynb
- [dwh_interface.ipynb] verify that the cluster is available by running the appropriate cells in dwh_interface.ipynb
- [dwh_interface.ipynb] ensure redshift is permitted to copy from s3 to its database
- [create_tables.py, sql_queries.py] create tables in redshift cluster by running create_tables.py
- [etl.py, sql_queries.py] copy contents of s3 bucket to staging tables in redshift and insert that data into analysis tables by running etl.py
- [dwh_interface.ipynb] verify that tables have been filled properly by running queries from dwh_interface.ipynb
- [dwh_interface.ipynb] delete the cluster (avoid extra charges) by running the appropriate cells in dwh_interface.ipynb

# **MANIFEST**

- analysis_star_schema.odg
- analysis_star_schema.dpf
- config.py
- create_table.py
- dwh.cfg
- dwh_interface.ipynb
- etl.py
- README.md
- sql_queries.py
- TODO.md
