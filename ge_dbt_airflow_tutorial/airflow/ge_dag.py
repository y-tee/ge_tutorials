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

GE_TUTORIAL_DB_URL = os.getenv("GE_TUTORIAL_DB_URL")
GE_TUTORIAL_ROOT_PATH = os.getenv("GE_TUTORIAL_ROOT_PATH")

great_expectations_context_path = os.getenv(
    "GE_TUTORIAL_GE_CONTEXT_PATH"
) or os.path.join(
    GE_TUTORIAL_ROOT_PATH, "great_expectations_projects", "final", "great_expectations"
)

default_args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(1)}

# The DAG definition
dag = DAG(
    dag_id="my_ge_on_airflow",
    default_args=default_args,
    schedule_interval=None,
)


def generate_expectation(ds, **kwargs):
    mr = moneylion_redshift(great_expectations_context_path)
    mr.create_expectation_from_table_defined(
        "column_type_check",
        "usr_yloo",
        "transaction_test",
        "created",
        "2020-10-26",
        "2020-11-10",
        columnA="created",
        columntype="TIMESTAMP",
    )


def generate_profile(ds, **kwargs):
    mr = moneylion_redshift(great_expectations_context_path)
    mr.create_expectation_from_table_profiled(
        "usr_yloo", "transaction_test", "created", "2020-10-26", "2020-12-30"
    )


def validate(ds, **kwargs):
    mr = moneylion_redshift(great_expectations_context_path)
    mr.get_rs_data_to_validate(
        "usr_yloo",
        "transaction_test",
        "created",
        "2020-11-11",
        "2020-11-30",
        "column_type_check",
    )


task_generate_exp = PythonOperator(
    task_id="generate_expectation",
    python_callable=generate_expectation,
    provide_context=True,
    dag=dag,
)

task_generate_profile = PythonOperator(
    task_id="generate_profile",
    python_callable=generate_profile,
    provide_context=True,
    dag=dag,
)

validate_data = PythonOperator(
    task_id="validate_data", python_callable=validate, provide_context=True, dag=dag
)

task_generate_exp >> task_generate_profile >> validate_data
