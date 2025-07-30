WITH source AS (

    SELECT
        run_id                                          AS run_id,
        model_name                                      AS model_name,
        sub_model                                       AS sub_model,
        model_version                                   AS model_version,
        score_date                                      AS score_date,
        record_count                                    AS record_count,
        prediction_metrics                              AS prediction_metrics,
        prediction_baseline                             AS prediction_baseline,
        prediction_drift                                AS prediction_drift,
        prediction_drift_status                         AS prediction_drift_status,
        feature_drift_summary                           AS feature_drift_summary,
        feature_drift_status                            AS feature_drift_status,
        model_configuration                             AS model_configuration,
        created_at::TIMESTAMP                           AS created_at
        
    FROM {{ source('data_science', 'model_scoring_summary') }}
)

SELECT *
FROM source