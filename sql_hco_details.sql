-- File: jcr_data_pipeline/composer/sql/create_hco_details.sql
-- Purpose: Creates the healthcare organization details table in BigQuery
-- Note: This table stores information about healthcare organizations

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.hco_details`
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
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY hco_id, site_id;

-- Create a view for accredited organizations
CREATE OR REPLACE VIEW `{project}.{dataset}.accredited_organizations` AS
SELECT 
    hco.*,
    os.program_id,
    os.program_name,
    os.observations_found
FROM `{project}.{dataset}.hco_details` hco
JOIN `{project}.{dataset}.observation_summary` os
    ON hco.site_id = os.site_id
WHERE hco.accreditation_status = 'Accredited'
    AND os.has_active_license = TRUE;