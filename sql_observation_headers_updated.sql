-- File: jcr_data_pipeline/composer/sql/create_observation_headers.sql
-- Purpose: Creates the observation headers table in BigQuery
-- Variables: Uses Airflow variables as defined in airflow_variables.json

CREATE TABLE IF NOT EXISTS `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.observation_headers`
(
    tracer_id INT64 NOT NULL,
    observation_id INT64 NOT NULL,
    observation_title STRING NOT NULL,
    contracted_service STRING,
    survey_team STRING,
    staff_interviewed STRING,
    medical_staff_involved STRING,
    location STRING,
    equipment_observed STRING,
    unique_identifier STRING,
    total_completed_observations INT64 NOT NULL,
    observation_note STRING,
    observation_status STRING NOT NULL,
    department STRING NOT NULL,
    department_level_2 STRING,
    department_level_3 STRING,
    observation_date TIMESTAMP NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    updated_by_fullname STRING NOT NULL,
    updated_by_email STRING NOT NULL,
    environment STRING NOT NULL DEFAULT '{{ var.json.gcp.bigquery.environment }}'
)
PARTITION BY DATE(observation_date)
OPTIONS(
    partition_expiration_days={{ var.json.gcp.bigquery.partition_expiration_days }},
    require_partition_filter=true
)
CLUSTER BY {{ var.json.gcp.bigquery.default_clustering_fields[0] }}, {{ var.json.gcp.bigquery.default_clustering_fields[1] }};

-- Create a view for recent observations
CREATE OR REPLACE VIEW `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.vw_recent_observations_{{ var.json.gcp.bigquery.environment }}` AS
SELECT *
FROM `{{ var.json.gcp.project_id }}.{{ var.json.gcp.dataset_id }}.observation_headers`
WHERE observation_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
AND environment = '{{ var.json.gcp.bigquery.environment }}';