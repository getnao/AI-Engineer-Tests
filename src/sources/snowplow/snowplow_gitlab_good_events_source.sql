{{ config({
    "alias": "snowplow_gitlab_good_events_source",
    "snowflake_warehouse": generate_warehouse_name('XL')
}) }}

WITH source AS (

  SELECT
    {{ dbt_utils.star(from=source('gitlab_snowplow', 'events'), except=['geo_zipcode', 'geo_latitude', 'geo_longitude', 'user_ipaddress']) }}
  FROM {{ source('gitlab_snowplow', 'events') }}
  -- This filter is to prevent querying of in process copies.
  WHERE uploaded_at < (SELECT uploaded_at FROM {{ source('gitlab_snowplow', 'events') }} ORDER BY uploaded_at DESC LIMIT 1)
),


final_source AS (
  SELECT *
  FROM source

  UNION ALL

  SELECT *
  FROM {{ ref( 'snowplow_gitlab_good_events_staging_source') }}
)

SELECT *
FROM final_source
