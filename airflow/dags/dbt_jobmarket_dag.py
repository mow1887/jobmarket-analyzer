from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'morten',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'dbt_snowflake_transformations',
    default_args=default_args,
    description='Triggers dbt run and test for the Jobmarket Analyzer',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dbt', 'snowflake', 'silver'],
) as dag:

    # Task 1: Execute dbt transformations
    # - Redirects logs and target artifacts to /tmp to prevent Windows volume permission conflicts
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt/dbt_jobmarket && /home/airflow/.local/bin/dbt run -t airflow',
        env={
            'DBT_LOG_PATH': '/tmp/dbt_logs',
            'DBT_TARGET_PATH': '/tmp/dbt_target'
        }
    )

    # Task 2: Validate data quality
    # - Redirects logs and target artifacts to /tmp to prevent Windows volume permission conflicts
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt/dbt_jobmarket && /home/airflow/.local/bin/dbt test -t airflow',
        env={
            'DBT_LOG_PATH': '/tmp/dbt_logs',
            'DBT_TARGET_PATH': '/tmp/dbt_target'
        }
    )

    dbt_run >> dbt_test