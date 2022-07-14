# Data-Pipeline

Overview
This project builds an ETL pipeline on Amazon S3 for Sparkify's analytics purposes.


Structure
The project contains
etl.py reads data from S3, processes it into analytics tables, and then writes them to S3



Data tables
songplays - which contains a log of user song plays
songplays has foreign keys to the following dimension tables:

users
songs
artists
time
Instructions
You will need to create a configuration file dl.cfg with the following structure:

[AWS]
AWS_ACCESS_KEY_ID=<your_aws_access_key_id>
AWS_SECRET_ACCESS_KEY=<your_aws_secret_access_key>
AWS_DEFAULT_REGION=<your_aws_region>
