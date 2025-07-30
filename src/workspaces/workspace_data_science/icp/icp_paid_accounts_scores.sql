WITH source AS (

    SELECT *
    FROM {{ ref('icp_paid_accounts_scores_source') }}

)

SELECT *
FROM source