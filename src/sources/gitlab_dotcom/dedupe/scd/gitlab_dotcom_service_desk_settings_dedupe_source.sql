WITH base AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'service_desk_settings') }}

)

SELECT * FROM base