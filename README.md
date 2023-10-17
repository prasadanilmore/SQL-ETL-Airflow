# SQL to PostgreSQL ETL with Airflow
This project provides an ETL pipeline using Apache Airflow to transfer data from an SQL Server database to a PostgreSQL database.

## Overview
The ETL process is a critical component in data warehousing and analytics. The objective is to ensure that data from various source systems is transformed and loaded into a data warehouse in an automated, consistent, and reliable manner.

In this project, we extract specific tables from an SQL Server database, transform the data as required, and then load it into a PostgreSQL database.

## Prerequisites
- Apache Airflow
- Python libraries: sqlalchemy, pyodbc, pandas, os
- SQL Server with the AdventureWorksDW2019 database installed
- PostgreSQL server

## Setup
### Environment Variables
Before running the script, ensure that the following environment variables are set:

- PGPASS: Your PostgreSQL password
- PGUID: Your PostgreSQL user ID
These environment variables are used to fetch the PostgreSQL credentials securely.

### Database Details
- SQL Server:
    - driver: The ODBC driver used to connect to the SQL Server. For this project, we use "{SQL Server Native Client 11.0}".
    - server: The name or IP address of the SQL Server. In this example, it's set to "haq-PC".
    - database: The specific database on the server that you want to connect to. We use the sample database "AdventureWorksDW2019".

## Workflow
1. Extraction:

- Establish a connection to the SQL Server database.
- Fetch specific table names (DimProduct, DimProductSubcategory, DimProductCategory, etc.).
- For each table, fetch its rows and store them into a pandas DataFrame.

2. Transformation:
- Rename specific columns for clarity.
- Handle missing values by replacing nulls.
- Drop unnecessary columns.

3. Loading:

- Establish a connection to the PostgreSQL database.
- Load the transformed data from the DataFrame into the PostgreSQL database with a prefix "stg_" for staging tables.


## Airflow DAG
The Airflow DAG orchestrates the ETL process by:

1. Extracting and loading source data from SQL Server to PostgreSQL.
2. Transforming the source data in the PostgreSQL staging tables.
3. Loading the final transformed data into the production tables in PostgreSQL.

To view the DAG and its progress, open the Airflow web UI, find the DAG with the ID "product_etl_dag", and monitor its execution.

## Conclusion
This project offers an automated and efficient way to transfer and transform data between SQL Server and PostgreSQL using Apache Airflow. By leveraging environment variables and Airflow's powerful orchestration capabilities, we ensure secure, reliable, and organized ETL operations.