# introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

# steps
1- build some queries to drop tables if they existed (for the puropse of frequent trials in this project).
2- build queries needed to create tables if the did not existed.
3- build queries to copy data from s3 bucket into staging tables.
4- build queries to insert data from staging tables to final dwh tables.
5- creating a redshift cluster (from the file create_delete_cluster)
6- user the IAM role and credentials form that file and load the into dwh.cfg file
5 - run son create_tables.py and etl.py respectively
6 - after running these file some processes are executed in the backend
(like drop table then 
           creating them then 
           copying data from s3 bucket into staging tables and 
           loading from staging tables into final tables)


# total files 
- <dwh.cfg> and <dwh2.cfg> to use them to connect to database and our cluster
- <sql_queries.py> all used queries are written in this file
- <create_tables.py> this file is used to drop table and create them
- <etl.py> copies data from s3 bucket into staging tables and then extracts data from staging tables to final 5 tables
- <create_delete_cluster.ipynb> to create and delete redshift cluster and IAM roles
- <try to get the error.py> to get a certain error that may pop up while running <etl.py> file
