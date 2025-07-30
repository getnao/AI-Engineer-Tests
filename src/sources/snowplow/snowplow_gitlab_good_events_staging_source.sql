{{ config(materialized='view') }}

WITH source AS (

  SELECT
    {{ dbt_utils.star(from=source('gitlab_snowplow_staging', 'events'), except=['geo_zipcode', 'geo_latitude', 'geo_longitude', 'user_ipaddress']) }}
  FROM {{ source('gitlab_snowplow_staging', 'events') }}
  -- This filter is to prevent querying of in process copies.
  WHERE uploaded_at < (SELECT uploaded_at FROM {{ source('gitlab_snowplow_staging', 'events') }} ORDER BY uploaded_at DESC LIMIT 1)
)

SELECT *
FROM source
