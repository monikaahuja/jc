import requests
from typing import Dict, Optional
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JCRApiClient:
    def __init__(self, base_url: str, auth_headers: Dict):
        self.base_url = base_url
        self.auth_headers = auth_headers
        self.observation_summary_endpoint = f"{base_url}/external/api/v1.0/Observation/ObservationSummary"
        self.observation_details_endpoint = f"{base_url}/external/api/v1.0/Observation/ObservationDetails"

    def get_observation_summary(self) -> Optional[Dict]:
        """
        Retrieve observation summary for all sites and programs
        
        Returns:
            Optional[Dict]: Observation summary data
        """
        try:
            response = requests.get(
                self.observation_summary_endpoint,
                headers=self.auth_headers
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get observation summary. Status code: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting observation summary: {str(e)}")
            return None

    def get_observation_details(
        self,
        site_ids: str,
        from_date: datetime,
        thru_date: datetime
    ) -> Optional[Dict]:
        """
        Retrieve detailed observations for specified sites and date range
        
        Args:
            site_ids (str): Comma-separated string of site IDs
            from_date (datetime): Start date for observations
            thru_date (datetime): End date for observations
            
        Returns:
            Optional[Dict]: Detailed observation data
        """
        try:
            payload = {
                "site_id": site_ids,
                "from_date": from_date.strftime("%m/%d/%Y %I:%M %p"),
                "thru_date": thru_date.strftime("%m/%d/%Y %I:%M %p")
            }
            
            response = requests.post(
                self.observation_details_endpoint,
                headers=self.auth_headers,
                json=payload
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get observation details. Status code: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting observation details: {str(e)}")
            return None
