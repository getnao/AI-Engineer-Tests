WITH base AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'security_policy_requirements') }}

)

SELECT * FROM base