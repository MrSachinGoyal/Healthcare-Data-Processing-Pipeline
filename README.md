# Healthcare Data Processing Pipeline

# Overview
This repository contains scripts to implement a data processing pipeline for healthcare data using Apache Airflow, PySpark, and Google BigQuery. The pipeline is designed to process daily healthcare data files, perform data validation, consistency checks, data transformations, and load the processed data into BigQuery table. Additionally, it includes SQL queries to analyze the processed data in BigQuery.

# Technologies Used
This project leverages various technologies to implement a healthcare data processing pipeline:

- **Programming Languages**: Python, SQL
- **Google Cloud Platform (GCP) Services**: 
  - Cloud Storage
  - Dataproc
  - Composer (managed Apache Airflow service)
  - BigQuery
- **Apache Spark**

# Project Structure
- **pyspark_app.py**: PySpark script responsible for processing healthcare data, performing data validation, consistency checks, and data transformations.
- **airflow_script.py**: Apache Airflow DAG script defining the workflow for the data processing pipeline, including tasks to generate mock health records, submit PySpark job, and archive processed files.
- **bigquery_sql_queries.sql**: SQL queries for data analysis on the processed healthcare data in Google BigQuery.

## Airflow DAG Visualization

![Airflow DAG](https://github.com/MrSachinGoyal/healthcare_data_processing_pipeline/blob/master/airflow_dag.png?raw=true)

## PySpark Script (`pyspark_app.py`)

The `pyspark_app.py` script performs the following tasks:

- Reads daily healthcare data from Google Cloud Storage (GCS).
- Performs data validation and consistency checks on the input data.
- Performs data transformations, including adding new columns (`age_group`, `is_senior_citizen`, `load_time`) and renaming columns (`diagnosis_description` to `disease`).
- Writes the processed data to Google BigQuery.

## Apache Airflow DAG Script (`airflow_script.py`)

The `airflow_script.py` defines an Apache Airflow DAG named `health_data_processor_dag`, which orchestrates the data processing pipeline. It includes the following tasks:

- **Generate Health Records**: Executes a Python function to generate mock health records and upload them to Google Cloud Storage as CSV files.
- **Submit PySpark Job**: Submits the PySpark job defined in `pyspark_app.py` to process the generated health records.
- **Archive Processed Files**: Moves the processed CSV files from the input folder to an archive folder in Google Cloud Storage.

## Google BigQuery SQL Queries (`bigquery_sql_queries.sql`)

The `bigquery_sql_queries.sql` file contains SQL queries for analyzing the processed healthcare data stored in Google BigQuery. The queries include:

1. Calculating the gender ratio for each disease.
2. Finding the top 3 most common diseases in the dataset.
3. Calculating the number of patients in each age category for each disease.
4. Analyzing the number of cases of each disease for each day of the week.

# Prerequisites

Before you begin with the setup, make sure you have the following:

- **Programming Language** : Python 3.1 or higher

- **Google Cloud Platform (GCP) Services**:
   - An active GCP account with the necessary permissions.
     - Google Cloud Storage (GCS): A GCS bucket set up to store input and output data files.
     - Google Dataproc: Access to Google Dataproc service to run Apache Spark jobs.
     - Google BigQuery: Access to Google BigQuery for storing and analyzing processed data.
     - Google Cloud Composer: Google Cloud Composer (managed Apache Airflow service) set up to execute DAGs.
