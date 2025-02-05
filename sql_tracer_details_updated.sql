-- File: jcr_data_pipeline/composer/sql/create_tracer_details.sql
-- Purpose: Creates the tracer details table in BigQuery
-- Variables: Uses Airflow variables as defined in airflow_variables.json

CREATE TABLE IF NOT EXISTS `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.tracer_details`
(
    site_id INT64 NOT NULL,
    program_id INT64 NOT NULL,
    tracer_id INT64 NOT NULL,
    category_name STRING NOT NULL,
    tracer_name STRING NOT NULL,
    tracer_status STRING NOT NULL,
    tracer_type STRING NOT NULL,
    is_locked_system_tracer BOOL NOT NULL,
    tracer_instructions STRING,
    updated_by_fullname STRING NOT NULL,
    updated_by_email STRING NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    environment STRING NOT NULL DEFAULT '{{ var.json.gcp.bigquery.environment }}'
)
CLUSTER BY {{ var.json.gcp.bigquery.default_clustering_fields[0] }}, {{ var.json.gcp.bigquery.default_clustering_fields[1] }};

-- Create a view for active tracers
CREATE OR REPLACE VIEW `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.vw_active_tracers_{{ var.json.gcp.bigquery.environment }}` AS
SELECT *
FROM `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.tracer_details`
WHERE tracer_status = 'Published'
AND environment = '{{ var.json.gcp.bigquery.environment }}';