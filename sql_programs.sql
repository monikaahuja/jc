-- File: jcr_data_pipeline/composer/sql/create_programs.sql
-- Purpose: Creates the programs table in BigQuery
-- Note: This table stores information about healthcare programs

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.programs`
(
    program_id INT64 NOT NULL,
    program_name STRING NOT NULL,
    program_description STRING,
    is_active BOOL NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY program_id;

-- Create a view for program summary
CREATE OR REPLACE VIEW `{project}.{dataset}.program_summary` AS
SELECT 
    p.program_id,
    p.program_name,
    COUNT(DISTINCT os.site_id) as total_sites,
    COUNT(DISTINCT oh.observation_id) as total_observations,
    MIN(oh.observation_date) as earliest_observation,
    MAX(oh.observation_date) as latest_observation
FROM `{project}.{dataset}.programs` p
LEFT JOIN `{project}.{dataset}.observation_summary` os
    ON p.program_id = os.program_id
LEFT JOIN `{project}.{dataset}.observation_headers` oh
    ON os.site_id = oh.site_id
GROUP BY p.program_id, p.program_name;