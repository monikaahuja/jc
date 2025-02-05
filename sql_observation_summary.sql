-- File: jcr_data_pipeline/composer/sql/create_observation_summary.sql
-- Purpose: Creates the observation summary table in BigQuery

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.observation_summary`
(
    site_id INT64 NOT NULL,
    hco_id INT64,
    program_id INT64 NOT NULL,
    site_name STRING NOT NULL,
    program_name STRING NOT NULL,
    has_active_license BOOL NOT NULL,
    observations_found INT64 NOT NULL,
    updated_from TIMESTAMP NOT NULL,
    updated_thru TIMESTAMP NOT NULL
)
PARTITION BY DATE(updated_thru)
CLUSTER BY site_id, program_id;

-- Create a view for active sites
CREATE OR REPLACE VIEW `{project}.{dataset}.active_sites` AS
SELECT 
    site_id,
    site_name,
    program_id,
    program_name,
    observations_found
FROM `{project}.{dataset}.observation_summary`
WHERE has_active_license = TRUE;