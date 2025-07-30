{{ config(
    materialized="view"
    )
}}

SELECT
  availability_percentage::NUMBER(5, 4) AS availability_percentage,
  availability_date::DATE               AS availability_date,
  tenant::VARCHAR                       AS tenant,
  datetime_recorded::TIMESTAMP          AS datetime_recorded_s3,
  uploaded_at::TIMESTAMP                AS uploaded_at
FROM {{ source('gitlab_availability', 'dedicated') }}
