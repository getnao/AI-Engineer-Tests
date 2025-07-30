WITH source AS (

    SELECT
        issue_id::NUMBER       AS issue_id,
        sentiment              AS sentiment,
        sentiment_score        AS sentiment_score,
        sentiment_summary      AS sentiment_summary,
        score_date             AS score_date,
        model_version          AS model_version,
        uploaded_at::TIMESTAMP AS uploaded_at
    FROM {{ source('data_science', 'internal_issues_with_sentiment_scores') }}

)

SELECT *
FROM source