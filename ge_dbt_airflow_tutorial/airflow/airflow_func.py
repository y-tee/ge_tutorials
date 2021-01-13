from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator
import great_expectations as ge
from great_expectations.profile import BasicSuiteBuilderProfiler
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler


class moneylion_redshift:
    def __init__(self, ymlfilelocation):
        self.context = ge.data_context.DataContext(ymlfilelocation)

    def create_expectation_from_table_defined(
        self,
        expectation_name,
        schema,
        table,
        datecolumn,
        startdate,
        enddate,
        unique_column1=None,
        unique_column2=None,
        unique_column3=None,
        columna=None,
        columnb=None,
        columnt=None,
        columntype=None,
    ):
        batch_kwargs = self.context.build_batch_kwargs(
            "my_redshift_db",
            "queries",
            "query_template",
            query_parameters={
                "schema": schema,
                "table": table,
                "datecolumn": datecolumn,
                "start": startdate,
                "end": enddate,
            },
        )

        suite = self.context.create_expectation_suite(
            expectation_name, overwrite_existing=True
        )

        batch = self.context.get_batch(
            batch_kwargs=batch_kwargs, expectation_suite_name=suite
        )

        batch.set_config_value("interactive_evaluation", False)

        try:
            batch.expect_table_row_count_to_be_between(min_value=100, max_value=500000)
        except Exception as e:
            print(e)
            print("expected table row count wasnt given")

        """
        try:
            batch.expect_column_values_to_be_of_type(columnt, columntype)
        except Exception as e:
            print(e)
            print("expected column type wasnt given")
        
        try:
            batch.expect_column_values_to_be_unique(unique_column1)
        except Exception as e:
            print(e)
            print("column to be unique is not provided")
        """
        try:
            batch.expect_column_pair_values_to_be_equal(columna, columnb)
        except Exception as e:
            print(e)
            print("equal column pair is not provided")

        """
        try:
            batch.expect_compound_columns_to_be_unique(
                [unique_column1, unique_column2, unique_column3]
            )
        except Exception as e:
            print(e)
            print("compound column to be unique is not provided")
        """

        batch.save_expectation_suite()

    """
    Profiler documentation
    https://docs.greatexpectations.io/en/latest/autoapi/great_expectations/profile/index.html?highlight=profiler%20config#great_expectations.profile.BasicSuiteBuilderProfiler
    """

    def create_expectation_from_table_profiled(
        self, schema, table, datecolumn, startdate, enddate, suite_name
    ):
        batch_kwargs = self.context.build_batch_kwargs(
            "my_redshift_db",
            "queries",
            "query_template",
            query_parameters={
                "schema": schema,
                "table": table,
                "datecolumn": datecolumn,
                "start": startdate,
                "end": enddate,
            },
        )

        suite_name = suite_name

        suite = self.context.create_expectation_suite(
            suite_name, overwrite_existing=True
        )

        batch = self.context.get_batch(
            batch_kwargs=batch_kwargs, expectation_suite_name=suite
        )

        suite, validation_result = BasicSuiteBuilderProfiler().profile(
            batch
        )

        self.context.save_expectation_suite(suite, suite_name)

    def create_expectation_from_table_profiled_EDA(  # this profiler is created from pandas_profiling project supposed to be for EDA. Doesnt seem to work now
        self, schema, table, datecolumn, startdate, enddate, suite_name
    ):
        batch_kwargs = self.context.build_batch_kwargs(
            "my_redshift_db",
            "queries",
            "query_template",
            query_parameters={
                "schema": schema,
                "table": table,
                "datecolumn": datecolumn,
                "start": startdate,
                "end": enddate,
            },
        )

        suite_name = suite_name

        suite = self.context.create_expectation_suite(
            suite_name, overwrite_existing=True
        )

        batch = self.context.get_batch(
            batch_kwargs=batch_kwargs, expectation_suite_name=suite
        )

        suite, validation_result = BasicDatasetProfiler().profile(batch)

        self.context.save_expectation_suite(suite, suite_name)

    def get_rs_data_to_validate(
        self, schema, table, datecolumn, startdate, enddate, expectation_suite_name
    ):
        batch_kwargs = self.context.build_batch_kwargs(
            "my_redshift_db",
            "queries",
            "query_template",
            query_parameters={
                "schema": schema,
                "table": table,
                "datecolumn": datecolumn,
                "start": startdate,
                "end": enddate,
            },
        )

        batch = self.context.get_batch(batch_kwargs, expectation_suite_name)

        results = self.context.run_validation_operator(
            "action_list_operator", assets_to_validate=[batch], run_id="my_run_id"
        )  # e.g., Airflow run id or some run identifier that your pipeline uses.

        if not results["success"]:
            raise AirflowException(
                "The analytical output does not meet the expectations in the suite: {0:s}".format(
                    expectation_suite_name
                )
            )
