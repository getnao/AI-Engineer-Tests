WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_work_item_type_custom_fields_dedupe_source') }}

),

renamed AS (

  SELECT
    id::NUMBER               AS id,
    namespace_id::NUMBER     AS namespace_id,
    custom_field_id::NUMBER  AS custom_field_id,
    work_item_type_id::NUMBER AS work_item_type_id,
    created_at::TIMESTAMP    AS created_at,
    updated_at::TIMESTAMP    AS updated_at,
    _uploaded_at::FLOAT      AS uploaded_at

  FROM source

)


SELECT *
FROM renamed
