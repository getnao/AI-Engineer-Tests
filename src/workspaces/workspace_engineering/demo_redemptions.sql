WITH source AS (

  SELECT *
  FROM {{ ref('redemptions_source') }}

)

SELECT *
FROM source