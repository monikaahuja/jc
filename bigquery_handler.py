from google.cloud import bigquery
from typing import Dict, List
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BigQueryHandler:
    def __init__(self, project_id: str, dataset_id: str):
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.project_id = project_id

    def _get_table_ref(self, table_name: str) -> str:
        """Get fully qualified table reference"""
        return f"{self.project_id}.{self.dataset_id}.{table_name}"

    def store_observation_summary(self, data: Dict) -> bool:
        """
        Store observation summary data in BigQuery
        
        Args:
            data (Dict): Observation summary data from API
            
        Returns:
            bool: Success status
        """
        try:
            if not data or 'observation_summary' not in data:
                logger.error("Invalid observation summary data")
                return False

            df = pd.DataFrame(data['observation_summary'])
            
            # Convert to proper dtypes
            df['site_id'] = df['site_id'].astype('Int64')
            df['hco_id'] = df['hco_id'].astype('Int64')
            df['program_id'] = df['program_id'].astype('Int64')
            df['observations_found'] = df['observations_found'].astype('Int64')
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
            )

            table_ref = self._get_table_ref("observation_summary")
            
            job = self.client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            job.result()  # Wait for job to complete
            
            logger.info(f"Loaded {len(df)} rows into {table_ref}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing observation summary: {str(e)}")
            return False

    def store_observation_details(self, data: Dict) -> bool:
        """
        Store observation details data in BigQuery
        
        Args:
            data (Dict): Observation details data from API
            
        Returns:
            bool: Success status
        """
        try:
            # Store HCO details
            if data.get('hco_details'):
                df_hco = pd.DataFrame(data['hco_details'])
                self._store_dataframe('hco_details', df_hco)

            # Store programs
            if data.get('programs'):
                df_programs = pd.DataFrame(data['programs'])
                self._store_dataframe('programs', df_programs)

            # Store tracer details
            if data.get('tracer_details'):
                df_tracers = pd.DataFrame(data['tracer_details'])
                self._store_dataframe('tracer_details', df_tracers)

            # Store observation headers
            if data.get('observation_headers'):
                df_headers = pd.DataFrame(data['observation_headers'])
                self._store_dataframe('observation_headers', df_headers)

            # Store observation details
            if data.get('observation_details'):
                df_details = pd.DataFrame(data['observation_details'])
                self._store_dataframe('observation_details', df_details)

            return True

        except Exception as e:
            logger.error(f"Error storing observation details: {str(e)}")
            return False

    def _store_dataframe(self, table_name: str, df: pd.DataFrame) -> None:
        """
        Helper method to store a DataFrame in BigQuery
        
        Args:
            table_name (str): Name of the target table
            df (pd.DataFrame): DataFrame to store
        """
        table_ref = self._get_table_ref(table_name)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
        )
        
        job = self.client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()
        
        logger.info(f"Loaded {len(df)} rows into {table_ref}")
