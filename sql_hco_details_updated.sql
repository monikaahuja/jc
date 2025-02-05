-- File: jcr_data_pipeline/composer/sql/create_hco_details.sql
-- Purpose: Creates the healthcare organization details table in BigQuery
-- Variables: Uses Airflow variables as defined in airflow_variables.json

CREATE TABLE IF NOT EXISTS `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.hco_details`
(
    hco_id INT64 NOT NULL,
    site_id INT64 NOT NULL,
    site_name STRING NOT NULL,
    zip STRING,
    state STRING,
    city STRING,
    address STRING,
    organization_type STRING,
    accreditation_status STRING,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    environment STRING NOT NULL DEFAULT '{{ var.json.gcp.bigquery.environment }}'
)
CLUSTER BY {{ var.json.gcp.bigquery.default_clustering_fields[0] }}, {{ var.json.gcp.bigquery.default_clustering_fields[1] }};

-- Create a view for accredited organizations
CREATE OR REPLACE VIEW `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.vw_accredited_organizations_{{ var.json.gcp.bigquery.environment }}` AS
SELECT 
    hco.*,
    os.program_id,
    os.program_name,
    os.observations_found
FROM `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.hco_details` hco
JOIN `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.observation_summary` os
    ON hco.site_id = os.site_id
    AND os.environment = '{{ var.json.gcp.bigquery.environment }}'
WHERE hco.accreditation_status = 'Accredited'
    AND os.has_active_license = TRUE
    AND hco.environment = '{{ var.json.gcp.bigquery.environment }}';