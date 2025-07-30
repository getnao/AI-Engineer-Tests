WITH source AS (

  SELECT *
  FROM {{ ref('rspec_flaky_tests_source') }}

)

SELECT *
FROM source
