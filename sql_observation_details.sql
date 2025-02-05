-- File: jcr_data_pipeline/composer/sql/create_observation_details.sql
-- Purpose: Creates the observation details table in BigQuery
-- Note: This table stores the detailed responses for each observation question

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.observation_details`
(
    observation_id INT64 NOT NULL,
    question_id INT64 NOT NULL,
    additional_information STRING,
    multiple_choices STRING,
    numerator INT64,
    denominator INT64,
    is_not_applicable BOOL NOT NULL,
    question_response STRING,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY observation_id, question_id;

-- Create a view for non-compliant responses
CREATE OR REPLACE VIEW `{project}.{dataset}.non_compliant_responses` AS
SELECT 
    od.*,
    oh.observation_title,
    oh.department,
    oh.observation_date
FROM `{project}.{dataset}.observation_details` od
JOIN `{project}.{dataset}.observation_headers` oh
    ON od.observation_id = oh.observation_id
WHERE od.question_response = 'Non-Compliant';