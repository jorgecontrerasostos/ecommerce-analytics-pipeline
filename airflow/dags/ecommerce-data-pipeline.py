import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
from faker import Faker
import random
import boto3
import os

from generate_data import generate_customers, generate_orders

default_args = {
    "owner": "jorge-contreras",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ecommerce_data_pipeline",
    default_args=default_args,
    description="End-to-end e-commerce data pipeline",
    schedule_interval=None,
    catchup=False,
    tags=["ecommerce", "etl", "analytics"],
)


def generate_data(**context):
    print("=== Starting generate_data function ===")

    execution_date = context["execution_date"]
    print(f"Execution date: {execution_date}")

    try:
        print("Generating customers...")
        new_customers = generate_customers(num_customers=10)
        print(f"Successfully generated {len(new_customers)} customers")

        print("Generating orders...")
        new_orders = generate_orders(
            num_orders=50,
            num_customers=1000,
            num_products=500,
        )
        print(f"Successfully generated {len(new_orders)} orders")

        execution_date_str = execution_date.strftime("%Y%m%d")
        print(f"Date string: {execution_date_str}")

        customer_file = f"/tmp/new_customers_{execution_date_str}.csv"
        orders_file = f"/tmp/new_orders_{execution_date_str}.csv"

        print(f"Saving customers to: {customer_file}")
        new_customers.to_csv(customer_file, index=False)
        print("Customers saved successfully!")

        print(f"Saving orders to: {orders_file}")
        new_orders.to_csv(orders_file, index=False)
        print("Orders saved successfully!")

        print("=== Function completed successfully ===")
        return f"Generated daily data for {execution_date_str}"

    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback

        print(f"Traceback: {traceback.format_exc()}")
        raise


generate_data_task = PythonOperator(
    task_id="generate_data",
    python_callable=generate_data,
    dag=dag,
)

success_task = BashOperator(
    task_id="pipeline_complete",
    bash_command='echo "Pipeline completed successfully!"',
    dag=dag,
)

generate_data_task >> success_task
