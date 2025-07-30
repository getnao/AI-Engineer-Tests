WITH all_rows_source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_custom_fields_dedupe_source') }}

), internal_rows_source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_custom_fields_internal_only_dedupe_source') }}

), all_rows_renamed AS (

  SELECT
    id::NUMBER                        AS custom_field_id,
    namespace_id::NUMBER              AS namespace_id,
    created_at::TIMESTAMP             AS created_at,
    updated_at::TIMESTAMP             AS updated_at,
    archived_at::TIMESTAMP            AS archived_at,
    field_type::NUMBER                AS field_type,
    name::VARCHAR                     AS name,
    created_by_id::NUMBER             AS created_by_id,
    updated_by_id::NUMBER             AS updated_by_id,
    _uploaded_at::FLOAT               AS uploaded_at

  FROM all_rows_source

), internal_rows_renamed AS (

  SELECT
    id::NUMBER                        AS internal_custom_field_id,
    namespace_id::NUMBER              AS internal_namespace_id,
    created_at::TIMESTAMP             AS internal_created_at,
    updated_at::TIMESTAMP             AS internal_updated_at,
    name::VARCHAR                     AS internal_name,
    _uploaded_at::FLOAT               AS internal_uploaded_at

  FROM internal_rows_source

), joined AS (

  SELECT

    custom_field_id                                   AS custom_field_id,
    namespace_id                                      AS namespace_id,
    created_at                                        AS created_at,
    updated_at                                        AS updated_at,
    archived_at                                       AS archived_at,
    field_type                                        AS field_type,
    COALESCE(name, internal_name)                     AS name,
    created_by_id                                     AS created_by_id,
    updated_by_id                                     AS updated_by_id,
    uploaded_at                                       AS uploaded_at

  FROM all_rows_renamed
  LEFT JOIN internal_rows_renamed 
    ON all_rows_renamed.custom_field_id = internal_rows_renamed.internal_custom_field_id

)

SELECT *
FROM joined
