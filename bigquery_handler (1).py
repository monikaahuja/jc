from google.cloud import bigquery
from typing import Dict, List, Optional
import logging
import pandas as pd
from google.api_core import exceptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BigQueryHandler:
    def __init__(self, project_id: str, dataset_id: str):
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.project_id = project_id
        self._initialize_schemas()

    def _initialize_schemas(self):
        """Initialize schema definitions for all tables"""
        self.table_schemas = {
            'observation_summary': [
                bigquery.SchemaField("site_id", "INTEGER"),
                bigquery.SchemaField("hco_id", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("program_id", "INTEGER"),
                bigquery.SchemaField("site_name", "STRING"),
                bigquery.SchemaField("program_name", "STRING"),
                bigquery.SchemaField("has_active_license", "BOOLEAN"),
                bigquery.SchemaField("observations_found", "INTEGER"),
                bigquery.SchemaField("updated_from", "TIMESTAMP"),
                bigquery.SchemaField("updated_thru", "TIMESTAMP")
            ],
            'hco_details': [
                bigquery.SchemaField("hco_id", "INTEGER"),
                bigquery.SchemaField("site_id", "INTEGER"),
                bigquery.SchemaField("site_name", "STRING"),
                bigquery.SchemaField("zip", "STRING")
            ],
            'programs': [
                bigquery.SchemaField("program_id", "INTEGER"),
                bigquery.SchemaField("program_name", "STRING")
            ],
            'tracer_details': [
                bigquery.SchemaField("site_id", "INTEGER"),
                bigquery.SchemaField("program_id", "INTEGER"),
                bigquery.SchemaField("tracer_id", "INTEGER"),
                bigquery.SchemaField("category_name", "STRING"),
                bigquery.SchemaField("tracer_name", "STRING"),
                bigquery.SchemaField("tracer_status", "STRING"),
                bigquery.SchemaField("tracer_type", "STRING"),
                bigquery.SchemaField("is_locked_system_tracer", "BOOLEAN"),
                bigquery.SchemaField("tracer_instructions", "STRING"),
                bigquery.SchemaField("updated_by_fullname", "STRING"),
                bigquery.SchemaField("updated_by_email", "STRING")
            ],
            'observation_headers': [
                bigquery.SchemaField("tracer_id", "INTEGER"),
                bigquery.SchemaField("observation_id", "INTEGER"),
                bigquery.SchemaField("observation_title", "STRING"),
                bigquery.SchemaField("contracted_service", "STRING"),
                bigquery.SchemaField("survey_team", "STRING"),
                bigquery.SchemaField("staff_interviewed", "STRING"),
                bigquery.SchemaField("medical_staff_involved", "STRING"),
                bigquery.SchemaField("location", "STRING"),
                bigquery.SchemaField("equipment_observed", "STRING"),
                bigquery.SchemaField("unique_identifier", "STRING"),
                bigquery.SchemaField("total_completed_observations", "INTEGER"),
                bigquery.SchemaField("observation_note", "STRING"),
                bigquery.SchemaField("observation_status", "STRING"),
                bigquery.SchemaField("department", "STRING"),
                bigquery.SchemaField("department_level_2", "STRING"),
                bigquery.SchemaField("department_level_3", "STRING"),
                bigquery.SchemaField("observation_date", "TIMESTAMP"),
                bigquery.SchemaField("last_updated", "TIMESTAMP"),
                bigquery.SchemaField("updated_by_fullname", "STRING"),
                bigquery.SchemaField("updated_by_email", "STRING")
            ],
            'observation_details': [
                bigquery.SchemaField("observation_id", "INTEGER"),
                bigquery.SchemaField("question_id", "INTEGER"),
                bigquery.SchemaField("additional_information", "STRING"),
                bigquery.SchemaField("multiple_choices", "STRING"),
                bigquery.SchemaField("numerator", "INTEGER"),
                bigquery.SchemaField("denominator", "INTEGER"),
                bigquery.SchemaField("is_not_applicable", "BOOLEAN"),
                bigquery.SchemaField("question_response", "STRING")
            ],
            'observation_notes': [
                bigquery.SchemaField("question_id", "INTEGER"),
                bigquery.SchemaField("question_note_id", "INTEGER"),
                bigquery.SchemaField("observation_id", "INTEGER"),
                bigquery.SchemaField("question_note", "STRING"),
                bigquery.SchemaField("last_updated", "TIMESTAMP"),
                bigquery.SchemaField("updated_by_full_name", "STRING"),
                bigquery.SchemaField("updated_by_email", "STRING")
            ]
        }

    def _get_table_ref(self, table_name: str) -> str:
        """Get fully qualified table reference"""
        return f"{self.project_id}.{self.dataset_id}.{table_name}"

    def ensure_table_exists(self, table_name: str) -> None:
        """
        Create table if it doesn't exist
        
        Args:
            table_name (str): Name of the table to create
        """
        table_ref = self._get_table_ref(table_name)
        try:
            self.client.get_table(table_ref)
            logger.info(f"Table {table_ref} already exists")
        except exceptions.NotFound:
            schema = self.table_schemas.get(table_name)
            if not schema:
                raise ValueError(f"Schema not defined for table {table_name}")

            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="last_updated" if "last_updated" in [field.name for field in schema]
                else None
            )
            
            self.client.create_table(table)
            logger.info(f"Created table {table_ref}")

    def _prepare_dataframe(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Prepare DataFrame for BigQuery insertion by matching schema
        
        Args:
            df (pd.DataFrame): Input DataFrame
            table_name (str): Target table name
            
        Returns:
            pd.DataFrame: Prepared DataFrame
        """
        schema = self.table_schemas.get(table_name)
        if not schema:
            raise ValueError(f"Schema not defined for table {table_name}")

        # Create a mapping of BigQuery types to pandas dtypes
        type_mapping = {
            'INTEGER': 'Int64',
            'FLOAT': 'float64',
            'BOOLEAN': 'boolean',
            'STRING': 'string',
            'TIMESTAMP': 'datetime64[ns]'
        }

        # Convert DataFrame columns to match schema
        for field in schema:
            if field.name in df.columns:
                target_type = type_mapping.get(field.field_type)
                if target_type:
                    try:
                        if field.field_type == 'TIMESTAMP':
                            df[field.name] = pd.to_datetime(df[field.name])
                        else:
                            df[field.name] = df[field.name].astype(target_type)
                    except Exception as e:
                        logger.warning(f"Error converting column {field.name}: {str(e)}")

        return df

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

            table_name = 'observation_summary'
            self.ensure_table_exists(table_name)
            
            df = pd.DataFrame(data['observation_summary'])
            df = self._prepare_dataframe(df, table_name)
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
            )

            table_ref = self._get_table_ref(table_name)
            
            job = self.client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            job.result()
            
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
            table_mappings = {
                'hco_details': 'hco_details',
                'programs': 'programs',
                'tracer_details': 'tracer_details',
                'observation_headers': 'observation_headers',
                'observation_details': 'observation_details',
                'observation_notes': 'observation_notes'
            }

            for api_key, table_name in table_mappings.items():
                if data.get(api_key):
                    self.ensure_table_exists(table_name)
                    df = pd.DataFrame(data[api_key])
                    df = self._prepare_dataframe(df, table_name)
                    
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_APPEND",
                    )
                    
                    table_ref = self._get_table_ref(table_name)
                    job = self.client.load_table_from_dataframe(
                        df, table_ref, job_config=job_config
                    )
                    job.result()
                    
                    logger.info(f"Loaded {len(df)} rows into {table_ref}")

            return True

        except Exception as e:
            logger.error(f"Error storing observation details: {str(e)}")
            return False
