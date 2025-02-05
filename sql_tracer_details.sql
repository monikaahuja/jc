-- File: jcr_data_pipeline/composer/sql/create_tracer_details.sql
-- Purpose: Creates the tracer details table in BigQuery
-- Note: This table stores information about tracers (surveys/checklists)

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.tracer_details`
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
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY site_id, program_id, tracer_id;

-- Create a view for active tracers
CREATE OR REPLACE VIEW `{project}.{dataset}.active_tracers` AS
SELECT *
FROM `{project}.{dataset}.tracer_details`
WHERE tracer_status = 'Published';