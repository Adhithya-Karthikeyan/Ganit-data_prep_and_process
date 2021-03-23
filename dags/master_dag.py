from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys 

#sys.path.insert(0, os.path.abspath(os.path.dirname('__file__')))
sys.path.append(os.getcwd()+'/airflow/Scripts/')

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
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'data_prep_and_process',
    default_args=default_args,
    description='test',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
)


# t1, t2 and t3 are examples of tasks created by instantiating operators

t1 = PythonOperator(
    task_id='data_preparation',
    provide_context=True,
    python_callable=data_preparation,
    op_args=['local'],
    dag=dag,
    do_xcom_push = False
)

t2 = PythonOperator(
    task_id='data_processing',
    provide_context=True,
    python_callable=data_processing,
    op_args=[os.getcwd(), {'databasename':'hist_layer','username':'adhithyakarthikeyan','password':'1234','host':'localhost:5432'}],
    dag=dag,
    do_xcom_push = False
)



t1 >> t2

