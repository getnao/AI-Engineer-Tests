WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'project_groups') }}

),

renamed AS (

    SELECT

        --ids
        uuid                                                AS project_group_id,
        provider_uuid                                       AS provider_id,
        parent_group_uuid                                   AS parent_group_id,

        --fields
        path                                                AS group_path,
        provider_project_id                                 AS provider_project_id,
        public                                              AS is_public,

        --dates
        created_at,
        updated_at,
        loaded_at

    FROM source

)

SELECT *
FROM renamed