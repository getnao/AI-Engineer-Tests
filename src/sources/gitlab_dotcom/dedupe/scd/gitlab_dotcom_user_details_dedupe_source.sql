WITH base AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'user_details') }}

)

{{ scd_latest_state(primary_key='user_id') }}
