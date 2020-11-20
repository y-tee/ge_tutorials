# Great Expectations tutorials

This repository contains the material for a number of Great Expectations tutorials. They all contain Dockerfiles and instructions in the respective README files.

**We invite community contributions for these tutorials!**

## ge_getting_started_tutorial
This example contains the a basic deploy of Great Expectations for the "Getting started with Great Expectations" tutorial, and as a starting point to explore and demo Great Expectations. See the README in the directory for details.

## ge_dbt_airflow_tutorial
This example demonstrates the use of Great Expectations in a data pipeline with dbt and Apache Airflow. See the README in the directory for details.

## edits
Set up datasource with [redshift](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_datasources/how_to_configure_a_redshift_datasource.html):
1. In `great_expectations.yml`, already included in repo no changes needed
2. In `config_variables.yml`, need to add to file 
```
my_redshift_db:
    url: "postgresql+psycopg2://username:password@host:port/database_name?sslmode=require"
```
