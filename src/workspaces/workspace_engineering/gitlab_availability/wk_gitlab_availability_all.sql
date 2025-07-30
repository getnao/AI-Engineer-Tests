{{ config(
    materialized="table"
    )
}}

WITH gitlab_com AS (
  SELECT
    *,
    'GitLab.com' AS deployment_type
  FROM {{ ref('wk_gitlab_availability_gitlab_com') }}
),

dedicated AS (
  SELECT
    *,
    'Dedicated' AS deployment_type
  FROM {{ ref('wk_gitlab_availability_dedicated') }}
),

unioned AS (
  SELECT *
  FROM gitlab_com
  UNION ALL
  SELECT *
  FROM dedicated
),

unioned_date AS (
  SELECT
    unioned.*,
    dim_date.first_day_of_month                                             AS availability_month,
    dim_date.first_day_of_year                                              AS availability_year,
    COALESCE(unioned.availability_date = dim_date.last_day_of_month, FALSE) AS is_last_day_of_month
  FROM unioned
  INNER JOIN {{ ref('dim_date') }} AS dim_date
    ON unioned.availability_date = dim_date.date_day
)

SELECT *
FROM unioned_date
