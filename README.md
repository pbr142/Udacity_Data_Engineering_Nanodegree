# Udacity Data Engineering Nanodegree

This repo contains the material (in folder *Lessons*) and my project files (in folder *Projects*) from the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## Project 1: Relational Databases
Developed a relational database using PostgreSQL to model user activity data for a music streaming app. Skills include:
* Created a relational database using PostgreSQL
* Developed a Star Schema database using optimized definitions of Fact and Dimension tables. Normalization of tables.
* Built out an ETL pipeline to optimize queries in order to understand what songs users listen to.


## Project 2: NoSQL Databases
Designed a NoSQL database using Apache Cassandra based on the original schema outlined in project one. Skills include:
* Created a nosql database using Apache Cassandra (both locally and with docker containers)
* Developed denormalized tables optimized for a specific set queries and business needs


## Project 3: Data Warehouse
Created a database warehouse utilizing Amazon Redshift. Skills include:
* Creating a Redshift Cluster, IAM Roles, Security groups.
* Develop an ETL Pipeline that copies data from S3 buckets into staging tables to be processed into a star schema
* Developed a star schema with optimization to specific queries required by the data analytics team.


## Project 4: Data Lake
Scaled up the current ETL pipeline by moving the data warehouse to a data lake. Skills include:
* Create an EMR Hadoop Cluster
* Further develop the ETL Pipeline copying datasets from S3 buckets, data processing using Spark and writing to S3 buckets using efficient partitioning and parquet formatting.
* Fast-tracking the data lake buildout using (serverless) AWS Lambda and cataloging tables with AWS Glue Crawler.


## Project 5: Data Pipelines
Automate the ETL pipeline and creation of data warehouse using Apache Airflow. Skills include:
* Using Airflow to automate ETL pipelines using Airflow, Python, Amazon Redshift.
* Writing custom operators to perform tasks such as staging data, filling the data warehouse, and validation through data quality checks.
* Transforming data from various sources into a star schema optimized for the analytics team's use cases.

