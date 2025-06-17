import pandas as pd
import psycopg2
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

# AWS and Redshift connection details
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET")

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", "5439")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")


def check_s3_file_exists(bucket, key):
    """Check if a file exists in S3"""
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )

    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False


def upload_to_s3(file_path, bucket, key):
    """Upload a file to S3"""
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )

    try:
        s3_client.upload_file(file_path, bucket, key)
        print(f"Successfully uploaded {file_path} to s3://{bucket}/{key}")
        return True
    except Exception as e:
        print(f"Error uploading {file_path} to S3: {e}")
        return False


def create_redshift_connection():
    """Create connection to Redshift using psycopg2"""

    try:
        conn = psycopg2.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
        )
        print("✅ Connection successful!")
        return conn
    except Exception as e:
        print(f"❌ Error connecting to Redshift: {e}")
        return None


def create_tables(conn):
    """Create tables in Redshift"""

    create_schemas = """
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS analytics;
    """

    create_customers = """
    CREATE TABLE IF NOT EXISTS staging.customers (
        customer_id INTEGER PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(100),
        phone VARCHAR(20),
        created_at TIMESTAMP
    );
    """

    create_products = """
    CREATE TABLE IF NOT EXISTS staging.products (
        product_id INTEGER PRIMARY KEY,
        product_name VARCHAR(200),
        category VARCHAR(50),
        price DECIMAL(10,2),
        created_at TIMESTAMP
    );
    """

    create_orders = """
    CREATE TABLE IF NOT EXISTS staging.orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        order_date DATE,
        status VARCHAR(20)
    );
    """

    cur = conn.cursor()
    try:
        cur.execute(create_schemas)
        cur.execute(create_customers)
        cur.execute(create_products)
        cur.execute(create_orders)
        conn.commit()
        print("Schemas and tables created successfully!")
    except Exception as e:
        print(f"Error creating tables: {e}")
        conn.rollback()
    finally:
        cur.close()


def copy_from_s3_to_redshift(conn, table_name, s3_path):
    """Load data from S3 to Redshift using COPY command"""

    copy_sql = f"""
    COPY staging.{table_name}
    FROM '{s3_path}'
    CREDENTIALS 'aws_access_key_id={AWS_ACCESS_KEY_ID};aws_secret_access_key={AWS_SECRET_ACCESS_KEY}'
    CSV
    IGNOREHEADER 1
    DELIMITER ','
    REGION '{AWS_DEFAULT_REGION}';
    """

    cur = conn.cursor()
    try:
        cur.execute(copy_sql)
        conn.commit()
        print(f"Successfully loaded {table_name} from S3")
    except Exception as e:
        print(f"Error loading {table_name} from S3: {e}")
        conn.rollback()
    finally:
        cur.close()


def main(skip_s3_upload=False):
    """Main function to orchestrate the data loading process"""

    # Check if required environment variables are set
    required_vars = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "S3_BUCKET",
        "REDSHIFT_HOST",
        "REDSHIFT_DB",
        "REDSHIFT_USER",
        "REDSHIFT_PASSWORD",
    ]

    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        return

    # CSV files to process
    csv_files = [
        {"file": "./data/customers.csv", "table": "customers"},
        {"file": "./data/products.csv", "table": "products"},
        {"file": "./data/orders.csv", "table": "orders"},
    ]

    print("Starting data loading process...")

    # Step 1: Upload CSV files to S3 (optional)
    if not skip_s3_upload:
        print("\n1. Uploading CSV files to S3...")
        for csv_file in csv_files:
            file_path = csv_file["file"]
            table_name = csv_file["table"]
            s3_key = f"data/{table_name}.csv"

            if os.path.exists(file_path):
                # Check if file already exists in S3
                if check_s3_file_exists(S3_BUCKET, s3_key):
                    print(f"File s3://{S3_BUCKET}/{s3_key} already exists, skipping upload")
                else:
                    upload_to_s3(file_path, S3_BUCKET, s3_key)
            else:
                print(f"Warning: {file_path} not found, skipping...")
    else:
        print("\n1. Skipping S3 upload (using existing files)")

    # Step 2: Connect to Redshift
    print("\n2. Connecting to Redshift...")
    conn = create_redshift_connection()
    if not conn:
        print("Failed to connect to Redshift. Exiting.")
        return

    # Step 3: Create tables
    print("\n3. Creating tables...")
    create_tables(conn)

    # Step 4: Load data from S3 to Redshift
    print("\n4. Loading data from S3 to Redshift...")
    for csv_file in csv_files:
        table_name = csv_file["table"]
        s3_path = f"s3://{S3_BUCKET}/data/{table_name}.csv"
        copy_from_s3_to_redshift(conn, table_name, s3_path)

    # Close connection
    conn.close()
    print("\nData loading process completed!")


if __name__ == "__main__":
    # Set skip_s3_upload=True if your data is already in S3
    main(skip_s3_upload=False)
