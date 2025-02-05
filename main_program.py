import os
from datetime import datetime, timedelta
from auth_handler import AuthenticationHandler
from api_client import JCRApiClient
from bigquery_handler import BigQueryHandler
import logging
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Configuration
    config = {
        'base_url': 'https://eprodservices.jcrinc.com',
        'project_id': os.getenv('GCP_PROJECT_ID'),
        'dataset_id': os.getenv('BIGQUERY_DATASET_ID'),
        'user_logon_id': os.getenv('JCR_USER_ID'),
        'password': os.getenv('JCR_PASSWORD')
    }

    try:
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

            # Process sites in batches to handle API limitations
            batch_size = 5
            site_batches = [site_ids[i:i + batch_size] 
                          for i in range(0, len(site_ids), batch_size)]

            # Set date range (7 days max as per API docs)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)

            for batch in site_batches:
                site_ids_str = ','.join(batch)
                logger.info(f"Fetching observation details for sites: {site_ids_str}")
                
                details_data = api_client.get_observation_details(
                    site_ids_str,
                    start_date,
                    end_date
                )

                if details_data:
                    bq_handler.store_observation_details(details_data)

        logger.info("Data ingestion completed successfully")

    except Exception as e:
        logger.error(f"Error in main program: {str(e)}")

if __name__ == "__main__":
    main()
