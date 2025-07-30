WITH source AS (
  SELECT *
  FROM {{ source('sheetload', 'crowdin_translation_contributions') }}
  
), renamed AS (

  SELECT 
    user_name::VARCHAR          AS user_name,
    language_id::VARCHAR        AS language_id,
    month::VARCHAR              AS month,
    unit::VARCHAR               AS unit,
    translated_count::INTEGER         AS translated_count,
    target_units_count::INTEGER       AS target_units_count,
    approved_count::INTEGER           AS approved_count,
    voted_count::INTEGER              AS voted_count,
    positive_votes_count::INTEGER     AS positive_votes_count,
    negative_votes_count::INTEGER     AS negative_votes_count,
    received_approvals_count::INTEGER AS received_approvals_count
  FROM source
)
SELECT *
FROM renamed

