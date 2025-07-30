WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'ticket_form_history') }}

),

renamed AS (

    SELECT

        --ids
        id                                                  AS ticket_form_id,

        --fields
        name                                                AS form_name,
        display_name                                        AS form_display_name,
        raw_name                                            AS form_raw_name,
        raw_display_name                                    AS form_raw_display_name,
        end_user_visible                                    AS is_end_user_visible,
        active                                              AS is_active,
        in_all_brands                                       AS is_in_all_brands,
        position                                            AS form_position,
        url                                                 AS form_url,
        "DEFAULT"                                           AS is_default_form,

        --dates
        created_at,
        updated_at,

        --metadata
        _fivetran_deleted                                   AS is_deleted,
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed