-- File: jcr_data_pipeline/composer/sql/create_programs.sql
-- Purpose: Creates the programs table in BigQuery
-- Variables: Uses Airflow variables as defined in airflow_variables.json

CREATE TABLE IF NOT EXISTS `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.programs`
(
    program_id INT64 NOT NULL,
    program_name STRING NOT NULL,
    program_description STRING,
    is_active BOOL NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    environment STRING NOT NULL DEFAULT '{{ var.json.gcp.bigquery.environment }}'
)
CLUSTER BY {{ var.json.gcp.bigquery.default_clustering_fields[0] }};

-- Create a view for program summary
CREATE OR REPLACE VIEW `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.vw_program_summary_{{ var.json.gcp.bigquery.environment }}` AS
SELECT 
    p.program_id,
    p.program_name,
    COUNT(DISTINCT os.site_id) as total_sites,
    COUNT(DISTINCT oh.observation_id) as total_observations,
    MIN(oh.observation_date) as earliest_observation,
    MAX(oh.observation_date) as latest_observation
FROM `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.programs` p
LEFT JOIN `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.observation_summary` os
    ON p.program_id = os.program_id
    AND os.environment = '{{ var.json.gcp.bigquery.environment }}'
LEFT JOIN `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.observation_headers` oh
    ON os.site_id = oh.site_id
    AND oh.environment = '{{ var.json.gcp.bigquery.environment }}'
WHERE p.environment = '{{ var.json.gcp.bigquery.environment }}'
GROUP BY p.program_id, p.program_name;