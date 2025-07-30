WITH source AS (

    SELECT *
    FROM {{ ref('internal_issues_with_sentiment_scores_source') }}

)

SELECT *
FROM source