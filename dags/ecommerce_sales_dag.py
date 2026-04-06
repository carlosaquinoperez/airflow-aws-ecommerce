from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# 1. Definimos los argumentos básicos del DAG
default_args = {
    'owner': 'carlos_aquino',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Instanciamos el DAG
with DAG(
    'ecommerce_sales_pipeline_v1',
    default_args=default_args,
    description='End-to-end Sales ETL using Airflow and AWS',
    schedule_interval='@daily',  # Se ejecuta cada medianoche
    catchup=False
) as dag:

    # Definimos tareas de ejemplo (Placeholder por ahora)
    def download_data():
        print("Downloading data from Kaggle...")

    def upload_to_s3():
        print("Uploading raw data to AWS S3 Bronze layer...")

    # 3. Creamos las tareas usando el PythonOperator
    task_extract = PythonOperator(
        task_id='extract_from_kaggle',
        python_callable=download_data
    )

    task_load = PythonOperator(
        task_id='load_to_s3_bronze',
        python_callable=upload_to_s3
    )

    # 4. Establecemos la dependencia (El flujo)
    task_extract >> task_load  # Primero extrae, luego carga