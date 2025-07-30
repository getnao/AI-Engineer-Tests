WITH source AS (

    SELECT
        crm_account_id,
        score_date,
        prob_cluster_0,
        prob_cluster_1,
        prob_cluster_2,
        prob_cluster_3,
        prob_cluster_4,
        prob_cluster_5,
        predicted_cluster_id,
        predicted_cluster_name,
        predicted_cluster_prob,
        predicted_icp_group,
        successful_customer_cluster_id,
        model_version,
        submodel,
        tier_quality,
        uploaded_at::TIMESTAMP as uploaded_at
    FROM {{ source('data_science', 'icp_paid_accounts_scores') }}
)

SELECT *
FROM source