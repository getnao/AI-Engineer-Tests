WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_work_item_select_field_values_dedupe_source') }}

),

renamed AS (

  SELECT
    id::NUMBER                           AS id,
    namespace_id::NUMBER                 AS namespace_id,
    work_item_id::NUMBER                 AS work_item_id,
    custom_field_id::NUMBER              AS custom_field_id,
    custom_field_select_option_id::NUMBER AS custom_field_select_option_id,
    created_at::TIMESTAMP                AS created_at,
    updated_at::TIMESTAMP                AS updated_at,
    _uploaded_at::FLOAT                  AS uploaded_at

  FROM source

)


SELECT *
FROM renamed
