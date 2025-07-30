{{ config(
    materialized="ephemeral"
    )
}}

WITH regular AS (
  SELECT
    availability_percentage,
    LAST_DAY(availability_date) AS availability_date, -- need to coerce to last day of month to match Dedicated
    tenant,
    datetime_recorded_s3,
    uploaded_at,
    'S3'                        AS data_source
  FROM {{ ref('gitlab_availability_gitlab_com_source') }}
),

static AS (
  SELECT
    availability_percentage,
    availability_date,
    tenant,
    NULL     AS datetime_recorded_s3,
    NULL     AS uploaded_at,
    'static' AS data_source
  FROM {{ ref('static_gitlab_availability_gitlab_com_source') }}
),

unioned AS (
  SELECT *
  FROM static
  UNION ALL
  SELECT *
  FROM regular
),

unioned_renamed AS (
  SELECT
    availability_percentage,
    availability_date,
    CASE WHEN tenant = 'global' THEN 'gitlab.com' ELSE tenant END AS tenant,
    datetime_recorded_s3,
    uploaded_at,
    data_source
  FROM unioned
)

SELECT *
FROM unioned_renamed
