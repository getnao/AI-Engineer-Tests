{{ config(
     materialized = "view"
) }}

WITH model_feature_drift AS (

    SELECT

        run_id                                          AS run_id,
        model_name                                      AS model_name,
        sub_model                                       AS sub_model,
        model_version                                   AS model_version,
        score_date                                      AS score_date,
        feature_name                                    AS feature_name,
        psi_value                                       AS psi_value,
        feature_importance                              AS feature_importance,
        status                                          AS status,
        created_at                                      AS created_at

    FROM {{ ref('model_feature_drift_source') }}

)

SELECT *
FROM model_feature_drift