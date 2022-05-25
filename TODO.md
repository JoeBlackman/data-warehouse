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

# Bugs

- [FIXED]SQL syntax error for insert strings at "ON CONFLICT DO NOTHING;"
  -- now i have insertion errors for songs with single quotes
  -- need correct escape sequences which are $escape$<sql>$escape$, not sure how this will work with my templated strings
  -- Resolved, '$$' is escaped to '$' when Template.substitute is called so each '$' character that is intended to be part of the query string needs an additional '$' in the templated string
- [FIXED] Redshift doesn't upsert, need strategy for handling duplicates
  -- SQL added to clean up tables with potential for duplicates
- etl.py now runs without error but is very slow. unsure of which operation is taking so much time but it seems that after over an hour, only the songs and artists tables have data in them. perhaps joining staging table to production table with grouping would reduce the execution time?
- [FIXED] and things failed... on inserting a song. TypeError: dict is not a sequence
  -- added sa.text to escape sql text
- [TESTING] user_id needs to be a proper integer in the insert statement for user entities
  -- added casting for user_id when building query string, in the process of testing it
- [FIXED] pandas date time has no object attribute dt, something isn't right with how i'm creating the date time components
  -- was not calling methods of datetime object correctly, also changed weekday to day of week
- last run of etl ended with my cluster disappearing. why?!
- taking forever to run code and test it. need more testable code to speed up dev process

Extensions to specific resources in S3 buckets to debug better
- /2018/11/2018-11-12-events.json
- /A/A/A