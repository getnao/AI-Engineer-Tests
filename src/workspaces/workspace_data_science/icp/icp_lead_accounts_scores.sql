WITH source AS (

    SELECT *
    FROM {{ ref('icp_lead_accounts_scores_source') }}

)

SELECT *
FROM source