{{ simple_cte([
    ('source', 'zendesk_fivetran_user_source')
]) }},

final AS (

  SELECT

      --ids
    user_id           AS dim_support_user_id,
    custom_role_id,
    default_group_id  AS dim_support_group_id,
    organization_id   AS dim_support_organization_id

  FROM source

)

SELECT *
FROM final