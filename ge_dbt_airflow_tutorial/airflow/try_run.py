import os
import pandas as pd
from sqlalchemy import create_engine
import great_expectations as ge
from airflow_func import moneylion_redshift

GE_TUTORIAL_ROOT_PATH = os.getenv('GE_TUTORIAL_ROOT_PATH')

great_expectations_context_path = os.getenv('GE_TUTORIAL_GE_CONTEXT_PATH') or os.path.join(GE_TUTORIAL_ROOT_PATH,
                                                                                           "great_expectations_projects",
                                                                                           "final",
                                                                                           "great_expectations")

def generate_profile():
    mr = moneylion_redshift(great_expectations_context_path)
    mr.create_expectation_from_table_profiled("ml_public", "usertransaction_v3", "transactiondate", '2019-01-01',
                                              '2019-06-01')

if __name__=="__main__":
    generate_profile()