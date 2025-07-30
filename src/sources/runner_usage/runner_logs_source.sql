WITH source AS (

    SELECT

        value['job']['duration_seconds']::FLOAT  AS job_duration_seconds,
        value['job']['failure_reason']::VARCHAR  AS job_failure_reason,
        value['job']['status']::VARCHAR          AS job_status ,
        value['job']['url']::VARCHAR             AS job_url,
        value['labels']::VARIANT                 AS labels,
        value['runner']['id']::VARCHAR           AS runner_id,
        value['runner']['name']::VARCHAR         AS runner_name,
        value['runner']['system_id']::VARCHAR    AS runner_system_id,
        value['timestamp']::TIMESTAMP_NTZ        AS timestamp,
        customer::VARCHAR                        AS customer,
        runner::VARCHAR                          AS runner,
        CONCAT_WS('-',_year, _month, _day)::DATE AS partition_date,
        _year::INT                               AS partition_year , 
        _month::INT                              AS partition_month,
        _day::INT                                AS partition_day,

    FROM {{ source('runner_usage','runner_logs') }}
)

SELECT *
FROM source