WITH base AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'approval_project_rules_protected_branches') }}

)

SELECT * FROM base