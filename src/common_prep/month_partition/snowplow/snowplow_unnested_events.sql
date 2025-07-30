{{config({
    "materialized":"view"
  })
}}

WITH gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_events') }}


)

SELECT *
FROM gitlab
