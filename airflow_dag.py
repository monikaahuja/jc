from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

# Add the directory containing your modules to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from main import main as run_ingestion

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'jcr_data_ingestion',
    default_args=default_args,
    description='Ingest data from JCR API to BigQuery',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    start_date=days_ago(1),
    tags=['jcr', 'api', 'bigquery'],
    catchup=False
)

start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

ingest_data = PythonOperator(
    task_id='ingest_jcr_data',
    python_callable=run_ingestion,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

start_task >> ingest_data >> end_task
