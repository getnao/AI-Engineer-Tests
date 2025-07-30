{{ config(
    materialized="view"
    )
}}

SELECT
  availability_percentage::NUMBER(5, 4) AS availability_percentage,
  availability_date::DATE               AS availability_date,
  tenant::VARCHAR                       AS tenant
FROM {{ source('static_gitlab_availability', 'dedicated') }}
