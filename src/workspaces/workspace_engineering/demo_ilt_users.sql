WITH source AS (

  SELECT *
  FROM {{ ref('ilt_users_source') }}

)

SELECT *
FROM source