# File: cloud_functions/observation_summary/main.py
import functions_framework
import requests
from google.cloud import bigquery
import json
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_observation_summary(base_url: str, headers: dict) -> dict:
    """Retrieve observation summary from JCR API."""
    endpoint = f"{base_url}/external/api/v1.0/Observation/ObservationSummary"
    
    try:
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API error: {e}")
        raise

def load_to_bigquery(data: dict, project_id: str, dataset_id: str):
    """Load observation summary data to BigQuery."""
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_id}.observation_summary"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("site_id", "INTEGER"),
            bigquery.SchemaField("hco_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("program_id", "INTEGER"),
            bigquery.SchemaField("site_name", "STRING"),
            bigquery.SchemaField("program_name", "STRING"),
            bigquery.SchemaField("has_active_license", "BOOLEAN"),
            bigquery.SchemaField("observations_found", "INTEGER"),
            bigquery.SchemaField("updated_from", "TIMESTAMP"),
            bigquery.SchemaField("updated_thru", "TIMESTAMP")
        ]
    )
    
    try:
        job = client.load_table_from_json(
            data['observation_summary'],
            table_id,
            job_config=job_config
        )
        job.result()  # Wait for job completion
        logger.info(f"Loaded {len(data['observation_summary'])} rows to {table_id}")
    except Exception as e:
        logger.error(f"BigQuery load error: {e}")
        raise

@functions_framework.http
def handle_observation_summary(request):
    """Cloud Function to handle observation summary retrieval and storage."""
    try:
        # Get request data
        request_json = request.get_json(silent=True)
        if not request_json or 'auth_headers' not in request_json:
            raise ValueError("Missing required authentication headers")
            
        auth_headers = request_json['auth_headers']
        
        # Get configuration from environment variables
        project_id = os.environ.get('GCP_PROJECT')
        dataset_id = os.environ.get('BIGQUERY_DATASET')
        base_url = os.environ.get('JCR_BASE_URL')
        
        # Get observation summary
        summary_data = get_observation_summary(base_url, auth_headers)
        
        # Load to BigQuery
        load_to_bigquery(summary_data, project_id, dataset_id)
        
        return json.dumps({"success": True}), 200, {
            'Content-Type': 'application/json'
        }
        
    except Exception as e:
        error_message = f"Observation summary processing failed: {str(e)}"
        logger.error(error_message)
        return json.dumps({
            "success": False,
            "error": error_message
        }), 500, {'Content-Type': 'application/json'}