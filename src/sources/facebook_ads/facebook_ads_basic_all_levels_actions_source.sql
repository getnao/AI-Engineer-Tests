WITH source AS (
  SELECT *
  FROM {{ source('facebook_ads','basic_all_levels_actions') }}
),

renamed AS (

  SELECT
    ad_id::VARCHAR              AS ad_id,
    date::DATE                  AS ad_date,
    index::NUMBER               AS index, 
    action_type::VARCHAR        AS action_type,
    value::NUMBER               AS value,
    inline::NUMBER              AS inline_value,
    _7_d_click::NUMBER          AS clicks,
    _1_d_view::NUMBER           AS views, 
    _fivetran_id::VARCHAR       AS _fivetran_id,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
