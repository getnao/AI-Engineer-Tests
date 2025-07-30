WITH source AS (

    SELECT
        run_id                    AS run_id,
        model_name                AS model_name,
        sub_model                 AS sub_model,
        model_version             AS model_version,
        score_date                AS score_date,
        feature_name              AS feature_name,
        psi_value                 AS psi_value,
        feature_importance        AS feature_importance,
        status                    AS status,
        created_at::TIMESTAMP     AS created_at
        
    FROM {{ source('data_science', 'model_feature_drift') }}
)

SELECT *
FROM source