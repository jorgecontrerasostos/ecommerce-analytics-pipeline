from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "test-redshift-connection",
    default_args=default_args,
    description="Test Redshift connection",
    schedule_interval=None,
    catchup=False,
)

test_redshift_connection = PostgresOperator(
    task_id="test_redshift_connection",
    postgres_conn_id="redshift_default",
    sql="""
    SELECT
        current_user as user,
        current_database() as database_name,
        version() as version
    """,
    dag=dag,
)
