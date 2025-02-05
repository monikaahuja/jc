# File: composer/scripts/validate_variables.py
# Purpose: Validate Airflow variables before deployment

from airflow.models import Variable
import json
import logging
import sys
from typing import List, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VariableValidator:
    def __init__(self):
        self.required_structure = {
            'gcp': {
                'project_id': str,
                'dataset_id': str,
                'location': str,
                'batch_size': int,
                'timeout_seconds': int,
                'bigquery': {
                    'dataset_retention_days': int,
                    'partition_expiration_days': int,
                    'default_clustering_fields': list,
                    'environment': str
                }
            },
            'jcr_api': {
                'cloud_functions': {
                    'auth_function_url': str,
                    'summary_function_url': str,
                    'details_function_url': str
                }
            }
        }

    def validate_type(self, value: any, expected_type: type, path: str) -> List[str]:
        """Validate type of a value"""
        errors = []
        if not isinstance(value, expected_type):
            errors.append(f"Invalid type for {path}: expected {expected_type.__name__}, got {type(value).__name__}")
        return errors

    def validate_structure(self, data: Dict, structure: Dict, path: str = '') -> List[str]:
        """Recursively validate variable structure"""
        errors = []
        
        for key, expected in structure.items():
            current_path = f"{path}.{key}" if path else key
            
            if key not in data:
                errors.append(f"Missing required key: {current_path}")
                continue
                
            if isinstance(expected, dict):
                if not isinstance(data[key], dict):
                    errors.append(f"Invalid type for {current_path}: expected dict")
                    continue
                errors.extend(self.validate_structure(data[key], expected, current_path))
            else:
                errors.extend(self.validate_type(data[key], expected, current_path))
                
        return errors

    def validate_values(self, data: Dict) -> List[str]:
        """Validate specific value constraints"""
        errors = []
        
        bigquery = data.get('gcp', {}).get('bigquery', {})
        
        # Validate partition expiration days
        partition_days = bigquery.get('partition_expiration_days', 0)
        if partition_days <= 0 or partition_days > 365:
            errors.append("partition_expiration_days must be between 1 and 365")
            
        # Validate environment
        environment = bigquery.get('environment', '')
        valid_environments = ['production', 'development', 'testing']
        if environment not in valid_environments:
            errors.append(f"environment must be one of: {', '.join(valid_environments)}")
            
        # Validate clustering fields
        clustering_fields = bigquery.get('default_clustering_fields', [])
        if not clustering_fields or len(clustering_fields) > 4:
            errors.append("default_clustering_fields must have 1-4 fields")
            
        return errors

    def validate_all(self) -> bool:
        """Validate all variables"""
        try:
            # Get variables from Airflow
            gcp_config = Variable.get('gcp', deserialize_json=True)
            jcr_api_config = Variable.get('jcr_api', deserialize_json=True)
            
            data = {
                'gcp': gcp_config,
                'jcr_api': jcr_api_config
            }
            
            # Validate structure
            structure_errors = self.validate_structure(data, self.required_structure)
            if structure_errors:
                logger.error("Structure validation errors:")
                for error in structure_errors:
                    logger.error(f"  - {error}")
                return False
                
            # Validate values
            value_errors = self.validate_values(data)
            if value_errors:
                logger.error("Value validation errors:")
                for error in value_errors:
                    logger.error(f"  - {error}")
                return False
                
            logger.info("All variables validated successfully")
            return True
            
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            return False

if __name__ == "__main__":
    validator = VariableValidator()
    if not validator.validate_all():
        sys.exit(1)