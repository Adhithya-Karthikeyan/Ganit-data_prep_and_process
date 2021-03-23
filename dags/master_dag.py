from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys 
import json

#sys.path.insert(0, os.path.abspath(os.path.dirname('__file__')))
sys.path.append(os.getcwd()+'/airflow/Scripts/')
sys.path.append(os.getcwd()+'/airflow/inputs/')

from data_preparation import data_preparation
from data_processing import data_processing


default_args = {
    'owner': 'Adhithya_Karthikeyan',
    'depends_on_past': False,
    'email': ['skadhithya95@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
}
dag = DAG(
    'data_prep_and_process',
    default_args=default_args,
    description='test',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
)

with open(os.getcwd()+'/airflow/inputs/inputs.json', 'r') as file:
    runtime_args = eval(file.read())

# t1, t2 and t3 are examples of tasks created by instantiating operators
data_preparation_target_file_system = runtime_args['data_preparation_target_file_system']
data_preparation_target_path = runtime_args['data_preparation_target_path']
data_preparation_target_file_name = runtime_args['data_preparation_target_file_name']
data_preparation_s3_creds = runtime_args['data_preparation_s3_creds']
data_preparation_s3_bucket = runtime_args['data_preparation_s3_bucket']
data_processing_json_read_path = runtime_args['data_processing_json_read_path']
data_processing_psql_creds = runtime_args['data_processing_psql_creds']



data_preparation_runtime_args = [data_preparation_target_file_system, data_preparation_target_path, data_preparation_target_file_name, data_preparation_s3_creds, data_preparation_s3_bucket]
data_processing_runtime_args = [data_processing_json_read_path, data_processing_psql_creds]

t1 = PythonOperator(
    task_id='data_preparation',
    provide_context=True,
    python_callable=data_preparation,
    op_args=data_preparation_runtime_args,
    dag=dag,
    do_xcom_push = False
)

t2 = PythonOperator(
    task_id='data_processing',
    provide_context=True,
    python_callable=data_processing,
    op_args=data_processing_runtime_args,
    dag=dag,
    do_xcom_push = False
)



t1 >> t2

