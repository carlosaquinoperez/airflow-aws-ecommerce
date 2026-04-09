import os
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------
# Updated to your new dynamically generated bucket
BRONZE_BUCKET_NAME = 'ecommerce-bronze-allowing-amoeba' 
LOCAL_FILE_PATH = '/opt/airflow/data/olist_orders_dataset.csv'
S3_OBJECT_KEY = f"raw/orders/ingestion_date={datetime.now().strftime('%Y-%m-%d')}/olist_orders.csv"

# ------------------------------------------------------------------------------
# DAG DEFINITION
# ------------------------------------------------------------------------------
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# ------------------------------------------------------------------------------
# PYTHON CALLABLES (TASKS)
# ------------------------------------------------------------------------------
def check_local_file(**kwargs):
    """Verifies if the source file exists before attempting upload."""
    if not os.path.exists(LOCAL_FILE_PATH):
        raise FileNotFoundError(f"Source file not found at {LOCAL_FILE_PATH}")
    print(f"File found! Size: {os.path.getsize(LOCAL_FILE_PATH)} bytes.")

def upload_to_s3(**kwargs):
    """Uploads the local CSV file to the AWS S3 Bronze Layer using Boto3."""
    s3_client = boto3.client('s3')
    
    print(f"Starting upload to s3://{BRONZE_BUCKET_NAME}/{S3_OBJECT_KEY}")
    s3_client.upload_file(LOCAL_FILE_PATH, BRONZE_BUCKET_NAME, S3_OBJECT_KEY)
    print("Upload completed successfully!")

# ------------------------------------------------------------------------------
# ORCHESTRATION
# ------------------------------------------------------------------------------
with DAG(
    'ecommerce_ingestion_bronze_to_silver',
    default_args=default_args,
    description='Ingests raw CSV to Bronze, then triggers Glue to transform to Silver Parquet',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'ingestion', 'bronze', 'silver']
) as dag:

    task_check_file = PythonOperator(
        task_id='verify_source_data',
        python_callable=check_local_file
    )

    task_upload_s3 = PythonOperator(
        task_id='upload_to_s3_bronze',
        python_callable=upload_to_s3
    )

    # Trigger the AWS Glue Job
    task_transform_silver = GlueJobOperator(
        task_id='transform_bronze_to_silver_parquet',
        job_name='ecommerce_bronze_to_silver_job',
        # wait_for_completion pauses the Airflow task until AWS says the job is done
        wait_for_completion=True, 
        region_name='us-east-1',
        # We pass the IAM role dynamically. Replace the suffix if your role name changed.
        iam_role_name='ecommerce_glue_role_allowing-amoeba' 
    )

    # Trigger the Crawler to update the Catalog
    task_run_crawler = GlueCrawlerOperator(
        task_id='run_glue_crawler',
        config={'Name': 'ecommerce_silver_crawler'},
        region_name='us-east-1'
    )

    # Define the new execution order (Pipeline Flow)
    task_check_file >> task_upload_s3 >> task_transform_silver >> task_run_crawler