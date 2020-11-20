from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator
import great_expectations as ge
from great_expectations.profile import BasicSuiteBuilderProfiler


class moneylion_redshift:
    def __init__(self, ymlfilelocation):
        self.context = ge.data_context.DataContext(ymlfilelocation)

    def create_expectation_from_table_defined(self, schema, table, datecolumn, startdate, enddate,unique_column):
        batch_kwargs = self.context.build_batch_kwargs(
            "my_redshift_db",
            "queries",
            "query_template",
            query_parameters={
                "schema": schema,
                "table": table,
                "datecolumn": datecolumn,
                "start": startdate,
                "end": enddate
            }
        )

        suite = self.context.create_expectation_suite(
            "my_expectation_suite", overwrite_existing=True
        )

        batch = self.context.get_batch(
            batch_kwargs=batch_kwargs,
            expectation_suite_name=suite
        )

        batch.expect_column_values_to_be_unique(unique_column)

        batch.save_expectation_suite()

    def create_expectation_from_table_profiled(self, schema, table, datecolumn, startdate, enddate):
        batch_kwargs = self.context.build_batch_kwargs(
            "my_redshift_db",
            "queries",
            "query_template",
            query_parameters={
                "schema": schema,
                "table": table,
                "datecolumn": datecolumn,
                "start": startdate,
                "end": enddate
            }
        )
        suite_name="profiled_expectation_suite"

        suite = self.context.create_expectation_suite(suite_name, overwrite_existing=True)

        batch = self.context.get_batch(
            batch_kwargs=batch_kwargs,
            expectation_suite_name=suite
        )

        suite, validation_result = BasicSuiteBuilderProfiler().profile(batch, profiler_configuration="demo")

        self.context.save_expectation_suite(suite,suite_name)



    def get_rs_data_to_validate(self, ds, ymlfilelocation, **kwargs):
        context = ge.data_context.DataContext(ymlfilelocation)

        batch_kwargs = {
            "datasource": "my_db",
            "schema": "my_schema",  # schema is optional; default schema will be used if it is omitted
            "table": "my_view"  # note that the "table" key is used even to validate a view
        }
