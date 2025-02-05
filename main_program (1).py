import json
from datetime import datetime, timedelta
from airflow.models import Variable
from auth_handler import AuthenticationHandler
from api_client import JCRApiClient
from bigquery_handler import BigQueryHandler
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_config() -> Dict:
    """
    Retrieve configuration from Airflow variables
    
    Returns:
        Dict: Configuration dictionary
    """
    try:
        # Get configuration from Airflow variables
        jcr_api_config = Variable.get("jcr_api", deserialize_json=True)
        gcp_config = Variable.get("gcp", deserialize_json=True)
        batch_settings = Variable.get("batch_settings", deserialize_json=True)
        
        return {
            'base_url': jcr_api_config['base_url'],
            'project_id': gcp_config['project_id'],
            'dataset_id': gcp_config['dataset_id'],
            'user_logon_id': jcr_api_config['user_logon_id'],
            'password': jcr_api_config['password'],
            'batch_size': batch_settings['site_batch_size'],
            'lookback_days': batch_settings['lookback_days']
        }
    except Exception as e:
        logger.error(f"Error retrieving Airflow variables: {str(e)}")
        raise

def process_site_batch(
    api_client: JCRApiClient,
    bq_handler: BigQueryHandler,
    site_ids: List[str],
    start_date: datetime,
    end_date: datetime
) -> None:
    """
    Process a batch of sites
    
    Args:
        api_client: Initialized API client
        bq_handler: Initialized BigQuery handler
        site_ids: List of site IDs to process
        start_date: Start date for data retrieval
        end_date: End date for data retrieval
    """
    site_ids_str = ','.join(site_ids)
    logger.info(f"Fetching observation details for sites: {site_ids_str}")
    
    details_data = api_client.get_observation_details(
        site_ids_str,
        start_date,
        end_date
    )

    if details_data:
        bq_handler.store_observation_details(details_data)

def main():
    try:
        # Get configuration from Airflow variables
        config = get_config()

        # Initialize authentication
        auth_handler = AuthenticationHandler(config['base_url'])
        success, auth_data = auth_handler.authenticate(
            config['user_logon_id'],
            config['password']
        )

        if not success:
            logger.error("Authentication failed")
            return

        # Initialize API client
        api_client = JCRApiClient(
            config['base_url'],
            auth_handler.get_auth_headers()
        )

        # Initialize BigQuery handler
        bq_handler = BigQueryHandler(
            config['project_id'],
            config['dataset_id']
        )

        # Get observation summary
        logger.info("Fetching observation summary...")
        summary_data = api_client.get_observation_summary()
        
        if summary_data:
            bq_handler.store_observation_summary(summary_data)

        # Get observation details for each site
        if summary_data and 'observation_summary' in summary_data:
            site_ids = [str(site['site_id']) 
                       for site in summary_data['observation_summary']
                       if site['has_active_license']]

            # Process sites in batches using configured batch size
            batch_size = config['batch_size']
            site_batches = [site_ids[i:i + batch_size] 
                          for i in range(0, len(site_ids), batch_size)]

            # Set date range using configured lookback days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=config['lookback_days'])

            for batch in site_batches:
                process_site_batch(
                    api_client,
                    bq_handler,
                    batch,
                    start_date,
                    end_date
                )

        logger.info("Data ingestion completed successfully")

    except Exception as e:
        logger.error(f"Error in main program: {str(e)}")
        # Here you could add additional error handling like sending notifications
        raise

if __name__ == "__main__":
    main()
