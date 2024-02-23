from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import random
from airflow.utils.dates import days_ago
from airflow.models import Variable

# defining default argument for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# creating a DAG instance
dag = DAG(
    'health_data_processor_dag',
    default_args=default_args,
    description='A DAG to process daily healthcare data files and write processed data to snowflake target tables',
    schedule_interval=timedelta(days=1),
    start_date = datetime(2024, 2, 23),
    tags=['example'],
)

#Operators
#Executing the mock data generator script
def get_diagnosis_date():
    day = random.randint(1, 28)
    month = random.randint(1, 12)
    year = random.choice([2021, 2022, 2023, 2024])
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    datetime_object = datetime(year, month, day, hour, minute, second).strftime("%Y-%m-%d %H:%M:%S")
    return datetime_object

# storing data for 1000 patients
data = []
used_ids = set() # it is used to keep track of used patient ids

# generate a unique patient id
def generate_unique_patient_id(used_ids):
    while True:
        patient_id = f"P{random.randint(1, 10000)}"
        if patient_id not in used_ids:
            used_ids.add(patient_id)
            return patient_id
        
def upload_to_cloud():        
    for i in range(1, 1001):
        patient_id = generate_unique_patient_id(used_ids)
        age = random.randint(30, 70)
        gender = random.choice(["M", "F"])
        diagnosis_code, diagnosis_description = random.choice(
           [    ("D123", "Diabetes"), ("H234", "High Blood Pressure"), 
                ("C345", "Cancer"), ("A456", "Allergies"), 
                ("I567", "Influenza"), ("A678", "Arthritis")]
        )
        diagnosis_date = get_diagnosis_date()
        # Append the row to the data list
        data.append([patient_id, age, gender, diagnosis_code, diagnosis_description, diagnosis_date])

    # creating csv file
    df = pd.DataFrame(data, columns=["patient_id", "age", "gender", "diagnosis_code", "diagnosis_description", "diagnosis_date"])
    df.to_csv(f'gs://bucket-name/folder-name/health_data_{datetime.now().date().strftime("%Y-%m-%d").replace("-", "")}.csv', index=False)

generate_health_data_task = PythonOperator(
    task_id = 'generate_health_records',
    python_callable = upload_to_cloud,
    dag=dag
)

# Fetch configuration from Airflow variables
config = Variable.get("cluster_details", deserialize_json=True)
CLUSTER_NAME = config['CLUSTER_NAME']
PROJECT_ID = config['PROJECT_ID']
REGION = config['REGION']

# Define the DataprocSubmitPySparkJobOperator to exceute pyspark job
pyspark_job = {'main_python_file_uri': 'gs://bucket-name/spark_app.py'}

# Check if execution_date is provided manually, otherwise use the default execution date
# date_variable = "{{ ds_nodash }}" 
date_variable = "{{ dag_run.conf['execution_date'] if dag_run and dag_run.conf and 'execution_date' in dag_run.conf else ds_nodash }}"

submit_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_job',
    main=pyspark_job['main_python_file_uri'],
    arguments=[f"--date={date_variable}"],  # Passing date as an argument to the PySpark script
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    # dataproc_pyspark_properties=spark_job_resources_parm,
    dag=dag,
)

# Move processed files to archive bucket
archive_processed_file = BashOperator(
    task_id='archive_processed_file',
    bash_command=f"gsutil mv gs://bucket-name/folder-name/*.csv gs://bucket-name/archieve-folder-name",
    dag=dag
)

# define task dependencies
generate_health_data_task >> submit_pyspark_job >> archive_processed_file

