from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import argparse

def spark_job(date):
    # create SparkSession
    spark = SparkSession.builder \
            .appName("Healthcare Data Processor") \
            .getOrCreate()

    # define input path
    input_path = f"gs://bucket-name/folder-name/health_data_{date}.csv"

    # create dataframe
    health_data_df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(input_path)

    # Data Validation and Data Consistency Checks
    print("Performing DATA VALIDATION & DATA CONSISTENCY checks...")

    # checking mandatory columns 
    def check_mandatory_cols(actual_cols, mandatory_cols):
        try:
            for column in mandatory_cols:
                if column not in actual_cols:
                    print(f"{column} is not present in DataFrame")
        except ValueError as e:
            return e
        else:
            return "All mandatory columns are present"

    actual_cols = health_data_df.columns
    mandatory_cols = ['patient_id', 'age', 'gender', 'diagnosis_code', 'diagnosis_description', 'diagnosis_date']
    print(check_mandatory_cols(actual_cols, mandatory_cols))

    # check column datatypes
    def check_col_datatype(expected_dtypes, df_actual_dtypes):
        try:
            for column in expected_dtypes:
                if column in df_actual_dtypes:
                    if expected_dtypes[column] != df_actual_dtypes[column]:
                        print(f"{column} data type is not matching")
        except Exception as e:
            return e
        else:
            return "All column datatypes are matching"

    expected_dtypes = { "diagnosis_code": "string", "patient_id": "string", "age": "int", "diagnosis_description": "string", "gender": "string", "diagnosis_data": "timestamp"}
    df_actual_dtypes = {key: value for key, value in health_data_df.dtypes}
    print(check_col_datatype(expected_dtypes, df_actual_dtypes))

    # check missing values in the dataset
    def check_missing_value(df):
        try:
            for column in health_data_df.columns:
                if health_data_df.filter(col(column).isNull()).count() > 0:
                    print(f"{column} has missing value")
        except Exception as e:
            return e
        else:    
            return 'All columns do not have missing value'
        
    print(check_missing_value(health_data_df))

    # removing duplicate rows from the dataframe
    health_data_df_without_duplicates = health_data_df.dropDuplicates()

    # checking if no of rows are same in dafaframe with or without duplicates
    if health_data_df_without_duplicates.count() == health_data_df.count():
        print('Dataframe does not have duplicates')

    # cache dataframe 
    health_data_df_without_duplicates.cache()

    # checking whether 'gender' column have only permitted values or not
    gender_val = ['M', 'F']

    invalid_gender_val = health_data_df_without_duplicates.filter(~col('gender').isin(gender_val)).count()
    if invalid_gender_val == 0:
        print('All gender values are valid')
    else:
        print('There is a presence of invalid gender values')

    # checking whether diagnosis_code is written as per prescribed format
    pattern = r'^[A-Z]\d+$'
    unmatching_diag_code = health_data_df_without_duplicates.filter(~col('diagnosis_code').rlike(pattern)).count()

    if unmatching_diag_code == 0:
        print('All diagnosis_code values are in the prescribed format.')
    else:
        print('There are some rows with diagnosis_code values not in the prescribed format.')

    print("DATA VALIDATION & CONSISTENCY Checks Completed Successfully.")

    # Transformations
    print('Performing DATA TRANSFORMATIONS')

    # adding two columns - 'age_groups', 'is_senior_citizen', 'load_time' and renaming column 'diagnosis_description' to disease
    final_df = health_data_df_without_duplicates \
                    .withColumn('age_group', 
                            when((col('age') >= 30) & (col('age') <= 40), '30-40')
                            .when((col('age') > 40) & (col('age') <= 50), '41-50')
                            .when((col('age') >= 50) & (col('age') <= 60), '51-60')
                            .when((col('age') >= 60) & (col('age') <= 70), '61-70')
                            .otherwise('Beyond 70')
                        ) \
                    .withColumn('is_senior_citizen', when(col('age') >= 60, 'Yes').otherwise('No')) \
                    .withColumnRenamed('diagnosis_description', 'disease') \
                    .withColumn('load_time', current_timestamp())

    # Write the data to BigQuery
    final_df.write \
        .format("bigquery") \
        .option("dataset", "gcp_project_id.dataset_id") \
        .option("table", "gcp_project_id.dataset_id.patients_data") \
        .option("temporaryGcsBucket", "bucket-name") \
        .mode("append") \
        .save()
    
    print('DATA TRANSFORMATION Completed Successfully.')

    # stop SparkSession
    spark.stop()
    print("Spark Session Stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process date argument')
    parser.add_argument('--date', type=str, required=True, help='Date in yyyymmdd format')
    args = parser.parse_args()
    
    spark_job(args.date)