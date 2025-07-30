{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','sf_1_i_0025_dxe_6_kkg_8_jcv_1_ibzm_5_harjdqsio_1_m_2_k') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR                           AS gsid,
    _fivetran_deleted::BOOLEAN              AS _fivetran_deleted,
    _fivetran_synced::TIMESTAMP             AS _fivetran_synced,
    sq_000574_value::VARCHAR                AS sq_000574_value,
    saa_0002363_sq_000579_value::NUMBER     AS saa_0002363_sq_000579_value,
    sq_000576_score::NUMBER                 AS sq_000576_score,
    survey_id::VARCHAR                      AS survey_id,
    created_at::TIMESTAMP_TZ                AS created_at,
    modified_by::VARCHAR                    AS modified_by,
    participant_id::VARCHAR                 AS participant_id,
    saa_0002361_sq_000579_value::NUMBER     AS saa_0002361_sq_000579_value,
    modified_at::TIMESTAMP_TZ               AS modified_at,
    sq_000577_value::VARCHAR                AS sq_000577_value,
    saa_0002364_sq_000579_value::NUMBER     AS saa_0002364_sq_000579_value,
    sq_000577_score::NUMBER                 AS sq_000577_score,
    sq_000574_score::NUMBER                 AS sq_000574_score,
    deleted::BOOLEAN                        AS is_deleted,
    responded_date::TIMESTAMP_TZ            AS responded_date,
    sq_000575_score::NUMBER                 AS sq_000575_score,
    created_by::VARCHAR                     AS created_by,
    company_id::VARCHAR                     AS company_id,
    saa_0002401_sq_000579_value::NUMBER     AS saa_0002401_sq_000579_value,
    saa_0002362_sq_000579_value::NUMBER     AS saa_0002362_sq_000579_value,
    sq_000575_value::VARCHAR                AS sq_000575_value,
    sq_000578::VARCHAR                      AS sq_000578,
    person_id::VARCHAR                      AS person_id,
    sq_000576_value::VARCHAR                AS sq_000576_value,
    saa_0002360_sq_000579_value::NUMBER     AS saa_0002360_sq_000579_value,
    sq_000579_answers::VARCHAR              AS sq_000579_answers
  FROM source
)

SELECT *
FROM renamed
