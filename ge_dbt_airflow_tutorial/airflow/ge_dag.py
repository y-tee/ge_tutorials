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

great_expectations_context_path = os.getenv('GE_TUTORIAL_GE_CONTEXT_PATH') or os.path.join(GE_TUTORIAL_ROOT_PATH, "great_expectations_projects", "final", "great_expectations")


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

def generate_expectation(ds,**kwargs):
    mr=moneylion_redshift(great_expectations_context_path)
    mr.create_expectation_from_table()