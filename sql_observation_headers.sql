-- File: jcr_data_pipeline/composer/sql/create_observation_headers.sql
-- Purpose: Creates the observation headers table in BigQuery
-- Note: This table stores metadata about each observation

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.observation_headers`
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
    updated_by_email STRING NOT NULL
)
PARTITION BY DATE(observation_date)
CLUSTER BY tracer_id, observation_id;

-- Create a view for recent observations
CREATE OR REPLACE VIEW `{project}.{dataset}.recent_observations` AS
SELECT *
FROM `{project}.{dataset}.observation_headers`
WHERE observation_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY);