import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import boto3
import psycopg2
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
    print("Starting generate_data function")

    execution_date = context["execution_date"]

    date_str = execution_date.strftime("%Y%m%d")
    hour_minute = execution_date.strftime("%H%M")
    unique_base = int(date_str[-4:] + hour_minute)

    print(f"Execution date: {execution_date}")
    print(f"Using unique base: {unique_base} (from {date_str}{hour_minute})")

    try:
        print("Generating customers...")
        new_customers = generate_customers(num_customers=10)

        new_customers["customer_id"] = range(
            100000 + unique_base, 100000 + unique_base + len(new_customers)
        )
        print(
            f"Customer IDs: {new_customers['customer_id'].min()} to {new_customers['customer_id'].max()}"
        )

        print("Generating orders...")
        new_orders = generate_orders(num_orders=50, num_customers=1000, num_products=500)

        new_orders["order_id"] = range(500000 + unique_base, 500000 + unique_base + len(new_orders))
        new_orders["order_date"] = execution_date.date()
        print(f"Order IDs: {new_orders['order_id'].min()} to {new_orders['order_id'].max()}")

        execution_date_str = execution_date.strftime("%Y%m%d")
        customer_file = f"/tmp/new_customers_{execution_date_str}.csv"
        orders_file = f"/tmp/new_orders_{execution_date_str}.csv"

        print(f"Saving customers to: {customer_file}")
        new_customers.to_csv(customer_file, index=False)
        print("Customers saved successfully!")

        print(f"Saving orders to: {orders_file}")
        new_orders.to_csv(orders_file, index=False)
        print("Orders saved successfully!")

        print(" Function completed successfully ")
        return f"Generated daily data for {execution_date_str}"

    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback

        print(f"Traceback: {traceback.format_exc()}")
        raise


def upload_to_s3(**context):
    print("Starting S3 upload")

    execution_date = context["execution_date"]
    execution_date_str = execution_date.strftime("%Y%m%d")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

    bucket_name = os.getenv("S3_BUCKET")
    print(f"Using bucket: {bucket_name}")

    files_to_upload = [
        {
            "local": f"/tmp/new_customers_{execution_date_str}.csv",
            "s3_key": f"daily_data/{execution_date_str}/new_customers_{execution_date_str}.csv",
        },
        {
            "local": f"/tmp/new_orders_{execution_date_str}.csv",
            "s3_key": f"daily_data/{execution_date_str}/new_orders_{execution_date_str}.csv",
        },
    ]

    try:
        for file_info in files_to_upload:
            local_path = file_info["local"]
            s3_key = file_info["s3_key"]

            print(f"Uploading {local_path} to s3://{bucket_name}/{s3_key}")
            s3_client.upload_file(local_path, bucket_name, s3_key)
            print(f"Successfully uploaded {s3_key}")

        print("S3 upload completed")
        return f"Uploaded files for {execution_date_str}"

    except Exception as e:
        print(f"S3 upload error: {str(e)}")
        raise


def load_to_redshift(**context):
    print(" Starting Redshift loading ")

    execution_date = context["execution_date"]
    execution_date_str = execution_date.strftime("%Y%m%d")

    conn = psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT", "5439"),
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
    )

    cur = conn.cursor()

    try:
        # Create temp tables
        print("Creating temp tables...")
        cur.execute("CREATE TEMP TABLE temp_customers (LIKE staging.customers);")
        cur.execute("CREATE TEMP TABLE temp_orders (LIKE staging.orders);")
        print("Temp tables created successfully!")

        # Load customers from S3
        print("Loading customers from S3...")
        customers_copy = f"""
        COPY temp_customers
        FROM 's3://{os.getenv('S3_BUCKET')}/daily_data/{execution_date_str}/new_customers_{execution_date_str}.csv'
        CREDENTIALS 'aws_access_key_id={os.getenv('AWS_ACCESS_KEY_ID')};aws_secret_access_key={os.getenv('AWS_SECRET_ACCESS_KEY')}'
        CSV IGNOREHEADER 1;
        """
        cur.execute(customers_copy)
        print("Customers loaded from S3!")

        # Load orders from S3
        print("Loading orders from S3...")
        orders_copy = f"""
        COPY temp_orders
        FROM 's3://{os.getenv('S3_BUCKET')}/daily_data/{execution_date_str}/new_orders_{execution_date_str}.csv'
        CREDENTIALS 'aws_access_key_id={os.getenv('AWS_ACCESS_KEY_ID')};aws_secret_access_key={os.getenv('AWS_SECRET_ACCESS_KEY')}'
        CSV IGNOREHEADER 1;
        """
        cur.execute(orders_copy)
        print("Orders loaded from S3!")

        # Insert new customers
        print("Inserting new customers...")
        cur.execute(
            """
            INSERT INTO staging.customers 
            SELECT * FROM temp_customers 
            WHERE customer_id NOT IN (SELECT customer_id FROM staging.customers);
        """
        )
        print("New customers inserted!")

        # Insert new orders
        print("Inserting new orders...")
        cur.execute(
            """
            INSERT INTO staging.orders 
            SELECT * FROM temp_orders 
            WHERE order_id NOT IN (SELECT order_id FROM staging.orders);
        """
        )
        print("New orders inserted!")

        # Get counts for verification
        cur.execute("SELECT COUNT(*) FROM temp_customers;")
        temp_customers_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM temp_orders;")
        temp_orders_count = cur.fetchone()[0]

        print(f"Processed {temp_customers_count} customers and {temp_orders_count} orders")

        conn.commit()
        print(" Redshift loading completed successfully! ")
        return f"Loaded {temp_customers_count} customers and {temp_orders_count} orders"

    except Exception as e:
        print(f"Redshift loading error: {e}")
        import traceback

        print(f"Traceback: {traceback.format_exc()}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


# Task 1: Generate data
generate_data_task = PythonOperator(
    task_id="generate_data",
    python_callable=generate_data,
    dag=dag,
)

# Task 2: Upload to S3
upload_s3_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    dag=dag,
)

# Task 3: Load to Redshift
load_redshift_task = PythonOperator(
    task_id="load_to_redshift",
    python_callable=load_to_redshift,
    dag=dag,
)

# Task 4: Run dbt models
dbt_run_task = BashOperator(
    task_id="run_dbt_models",
    bash_command="""
        cd /opt/airflow/dbt/ecommerce_analytics && 
        dbt run --profiles-dir /opt/airflow/dbt/ecommerce_analytics
    """,
    dag=dag,
)

# Task 5: Test dbt models
dbt_test_task = BashOperator(
    task_id="test_dbt_models",
    bash_command="""
        cd /opt/airflow/dbt/ecommerce_analytics && 
        dbt test --profiles-dir /opt/airflow/dbt/ecommerce_analytics
    """,
    dag=dag,
)

# Define task dependencies - linear pipeline
generate_data_task >> upload_s3_task >> load_redshift_task >> dbt_run_task >> dbt_test_task
