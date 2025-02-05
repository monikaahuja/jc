# File: jcr_data_pipeline/cloud_functions/auth_handshake/main.py
# Purpose: Cloud Function for JCR API authentication handshake
# Dependencies: requirements.txt in the same directory

import functions_framework
from google.cloud import secretmanager
import requests
import json
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """Retrieve secret from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Error accessing secret: {e}")
        raise

def get_auth_token(base_url: str, user_id: str, password: str) -> dict:
    """Get authentication token from JCR API."""
    auth_endpoint = f"{base_url}/external/api/v1.0/Authenticate/Authenticate"
    
    payload = {
        "user_logon_id": user_id,
        "password": password
    }
    
    try:
        response = requests.post(auth_endpoint, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Authentication error: {e}")
        raise

@functions_framework.http
def handle_auth_request(request):
    """Cloud Function to handle authentication with JCR API."""
    try:
        project_id = os.environ.get('GCP_PROJECT')
        user_id = get_secret(project_id, "jcr-user-id")
        password = get_secret(project_id, "jcr-password")
        base_url = get_secret(project_id, "jcr-base-url")
        
        auth_data = get_auth_token(base_url, user_id, password)
        
        return json.dumps({"success": True, "data": auth_data}), 200, {
            'Content-Type': 'application/json'
        }
    except Exception as e:
        error_message = f"Authentication failed: {str(e)}"
        logger.error(error_message)
        return json.dumps({
            "success": False,
            "error": error_message
        }), 500, {'Content-Type': 'application/json'}