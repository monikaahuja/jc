# File: composer/scripts/deploy_variables.py
# Purpose: Deploy and validate Airflow variables

import argparse
from airflow.models import Variable
import json
import logging
import os
from validate_variables import VariableValidator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_variables(env: str) -> dict:
    """Load variables from JSON file"""
    config_dir = os.path.join(os.path.dirname(__file__), '..', 'config')
    filename = f'variables_{env}.json'
    filepath = os.path.join(config_dir, filename)
    
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise ValueError(f"Configuration file not found: {filename}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in configuration file: {str(e)}")

def deploy_variables(variables: dict) -> None:
    """Deploy variables to Airflow"""
    for key, value in variables.items():
        Variable.set(
            key=key,
            value=json.dumps(value),
            serialize_json=True
        )
        logger.info(f"Deployed variable: {key}")

def main():
    parser = argparse.ArgumentParser(description='Deploy Airflow variables')
    parser.add_argument(
        '--env',
        choices=['dev', 'test', 'prod'],
        required=True,
        help='Environment to deploy'
    )
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate variables without deploying'
    )
    
    args = parser.parse_args()
    
    try:
        # Load variables
        variables = load_variables(args.env)
        
        # Create validator
        validator = VariableValidator()
        
        if args.validate_only:
            # Only validate
            if not validator.validate_all():
                raise ValueError("Variable validation failed")
            logger.info("Variables validated successfully")
        else:
            # Deploy and validate
            deploy_variables(variables)
            if not validator.validate_all():
                raise ValueError("Post-deployment validation failed")
            logger.info("Variables deployed and validated successfully")
            
    except Exception as e:
        logger.error(f"Deployment failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()