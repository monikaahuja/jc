import requests
from typing import Dict, Tuple, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AuthenticationHandler:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.auth_endpoint = f"{base_url}/external/api/v1.0/Authenticate/Authenticate"
        self.token = None
        self.user_id = None

    def authenticate(self, user_logon_id: str, password: str) -> Tuple[bool, Optional[Dict]]:
        """
        Authenticate with the JCR API
        
        Args:
            user_logon_id (str): User login ID (email)
            password (str): User password
            
        Returns:
            Tuple[bool, Optional[Dict]]: Success status and authentication details
        """
        try:
            payload = {
                "user_logon_id": user_logon_id,
                "password": password
            }
            
            response = requests.post(self.auth_endpoint, json=payload)
            
            if response.status_code == 200:
                auth_data = response.json()
                self.token = auth_data.get('token')
                self.user_id = auth_data.get('user_id')
                
                logger.info("Authentication successful")
                return True, auth_data
            else:
                logger.error(f"Authentication failed with status code: {response.status_code}")
                return False, None
                
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return False, None
    
    def get_auth_headers(self) -> Dict:
        """
        Get headers required for authenticated API calls
        
        Returns:
            Dict: Headers with authentication information
        """
        if not self.token or not self.user_id:
            raise ValueError("Not authenticated. Call authenticate() first.")
            
        return {
            "user_id": str(self.user_id),
            "token": self.token,
            "Content-Type": "application/json"
        }
