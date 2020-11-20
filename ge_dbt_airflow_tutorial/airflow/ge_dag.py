from datetime import datetime
import airflow
from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import os
import pandas as pd
from sqlalchemy import create_engine
import great_expectations as ge
from airflow_func import moneylion_redshift

GE_TUTORIAL_DB_URL = os.getenv('GE_TUTORIAL_DB_URL')
GE_TUTORIAL_ROOT_PATH = os.getenv('GE_TUTORIAL_ROOT_PATH')

great_expectations_context_path = os.getenv('GE_TUTORIAL_GE_CONTEXT_PATH') or os.path.join(GE_TUTORIAL_ROOT_PATH,
                                                                                           "great_expectations_projects",
                                                                                           "final",
                                                                                           "great_expectations")

default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1)
}

# The DAG definition
dag = DAG(
    dag_id='my_ge_on_airflow',
    default_args=default_args,
    schedule_interval=None,
)


def generate_expectation(ds, **kwargs):
    mr = moneylion_redshift(great_expectations_context_path)
    mr.create_expectation_from_table_defined("ml_public", "usertransaction_v3", "transactiondate", '2019-01-01',
                                             '2019-06-01', '_id')


def generate_profile(ds, **kwargs):
    mr = moneylion_redshift(great_expectations_context_path)
    mr.create_expectation_from_table_profiled("ml_public", "usertransaction_v3", "transactiondate", '2019-01-01',
                                              '2019-06-01')


task_generate_exp = PythonOperator(
    task_id='generate_expectation',
    python_callable=generate_expectation,
    provide_context=True,
    dag=dag)

task_generate_profile = PythonOperator(
    task_id='generate_profile',
    python_callable=generate_expectation,
    provide_context=True,
    dag=dag)

task_generate_exp >> task_generate_profile
