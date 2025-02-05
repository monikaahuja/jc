# File: composer/dags/create_tables_dag.py
# Purpose: DAG to create BigQuery tables using SQL files and Airflow variables

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Read SQL files
def read_sql_file(filename):
    """Read SQL file from the sql directory"""
    sql_dir = os.path.join(os.path.dirname(__file__), '..', 'sql')
    with open(os.path.join(sql_dir, filename), 'r') as f:
        return f.read()

# Validate required variables
def validate_variables():
    """Validate that all required Airflow variables are present"""
    required_vars = {
        'var.json.gcp.project_id': 'Project ID',
        'var.json.gcp.dataset_id': 'Dataset ID',
        'var.json.gcp.bigquery.environment': 'Environment',
        'var.json.gcp.bigquery.partition_expiration_days': 'Partition Expiration Days',
        'var.json.gcp.bigquery.default_clustering_fields': 'Clustering Fields'
    }
    
    missing_vars = []
    try:
        gcp_config = Variable.get('gcp', deserialize_json=True)
        
        if not gcp_config.get('project_id'):
            missing_vars.append('Project ID')
        if not gcp_config.get('dataset_id'):
            missing_vars.append('Dataset ID')
        if not gcp_config.get('bigquery', {}).get('environment'):
            missing_vars.append('Environment')
            
        if missing_vars:
            raise ValueError(f"Missing required variables: {', '.join(missing_vars)}")
            
        logger.info("All required variables are present")
    except Exception as e:
        logger.error(f"Variable validation failed: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# List of tables to create
TABLES = [
    'observation_summary',
    'observation_details',
    'observation_headers',
    'tracer_details',
    'programs',
    'hco_details'
]

with DAG(
    'create_jcr_tables',
    default_args=default_args,
    description='Create JCR BigQuery tables',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    # Validate variables task
    validate_vars = PythonOperator(
        task_id='validate_variables',
        python_callable=validate_variables,
        dag=dag
    )

    # Create tasks for each table
    create_table_tasks = []
    for table in TABLES:
        task = BigQueryExecuteQueryOperator(
            task_id=f'create_{table}_table',
            sql=read_sql_file(f'create_{table}.sql'),
            use_legacy_sql=False,
            location='{{ var.json.gcp.location }}',
            gcp_conn_id='google_cloud_default',
            dag=dag
        )
        create_table_tasks.append(task)

    # Set task dependencies
    validate_vars >> create_table_tasks