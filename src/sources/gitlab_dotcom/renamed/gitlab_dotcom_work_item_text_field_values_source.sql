WITH all_rows_source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_work_item_text_field_values_dedupe_source') }}

), internal_rows_source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_work_item_text_field_values_internal_only_dedupe_source') }}

), all_rows_renamed AS (

  SELECT
    id::NUMBER                        AS work_item_text_field_value_id,
    namespace_id::NUMBER              AS namespace_id,
    work_item_id::NUMBER              AS work_item_id,
    custom_field_id::NUMBER           AS custom_field_id,
    created_at::TIMESTAMP             AS created_at,
    updated_at::TIMESTAMP             AS updated_at,
    value::VARCHAR                    AS value,
    _uploaded_at::FLOAT               AS uploaded_at

  FROM all_rows_source

), internal_rows_renamed AS (

  SELECT
    id::NUMBER                        AS internal_work_item_text_field_value_id,
    namespace_id::NUMBER              AS internal_namespace_id,
    created_at::TIMESTAMP             AS internal_created_at,
    updated_at::TIMESTAMP             AS internal_updated_at,
    value::VARCHAR                    AS internal_value,
    _uploaded_at::FLOAT               AS internal_uploaded_at

  FROM internal_rows_source

), joined AS (

  SELECT

    work_item_text_field_value_id                     AS work_item_text_field_value_id,
    namespace_id                                      AS namespace_id,
    work_item_id                                      AS work_item_id,
    custom_field_id                                   AS custom_field_id,
    created_at                                        AS created_at,
    updated_at                                        AS updated_at,
    COALESCE(value, internal_value)                   AS value,
    uploaded_at                                       AS uploaded_at

  FROM all_rows_renamed
  LEFT JOIN internal_rows_renamed 
    ON all_rows_renamed.work_item_text_field_value_id = internal_rows_renamed.internal_work_item_text_field_value_id

)

SELECT *
FROM joined
