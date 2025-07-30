WITH all_rows_source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_custom_field_select_options_dedupe_source') }}

), internal_rows_source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_custom_field_select_options_internal_only_dedupe_source') }}

), all_rows_renamed AS (

  SELECT
    id::NUMBER                        AS custom_field_select_option_id,
    namespace_id::NUMBER              AS namespace_id,
    custom_field_id::NUMBER           AS custom_field_id,
    created_at::TIMESTAMP             AS created_at,
    updated_at::TIMESTAMP             AS updated_at,
    position::NUMBER                  AS position,
    value::VARCHAR                    AS value,
    _uploaded_at::FLOAT               AS uploaded_at

  FROM all_rows_source

), internal_rows_renamed AS (

  SELECT
    id::NUMBER                        AS internal_custom_field_select_option_id,
    namespace_id::NUMBER              AS internal_namespace_id,
    created_at::TIMESTAMP             AS internal_created_at,
    updated_at::TIMESTAMP             AS internal_updated_at,
    value::VARCHAR                    AS internal_value,
    _uploaded_at::FLOAT               AS internal_uploaded_at

  FROM internal_rows_source

), joined AS (

  SELECT

    custom_field_select_option_id                     AS custom_field_select_option_id,
    namespace_id                                      AS namespace_id,
    custom_field_id                                   AS custom_field_id,
    created_at                                        AS created_at,
    updated_at                                        AS updated_at,
    position                                          AS position,
    COALESCE(value, internal_value)                   AS value,
    uploaded_at                                       AS uploaded_at

  FROM all_rows_renamed
  LEFT JOIN internal_rows_renamed 
    ON all_rows_renamed.custom_field_select_option_id = internal_rows_renamed.internal_custom_field_select_option_id

)

SELECT *
FROM joined
