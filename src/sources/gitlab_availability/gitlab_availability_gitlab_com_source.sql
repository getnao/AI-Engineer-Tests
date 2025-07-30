{{ config(
    materialized="table"
    )
}}

SELECT
  (REPLACE(availability_percentage, '%', '') / 100)::NUMBER(5, 4) AS availability_percentage, -- udpate varchar to number
  availability_date::DATE                                         AS availability_date,
  tenant::VARCHAR                                                 AS tenant,
  datetime_recorded::TIMESTAMP                                    AS datetime_recorded_s3,
  uploaded_at::TIMESTAMP                                          AS uploaded_at
FROM {{ source('gitlab_availability', 'gitlab_com') }}
