WITH source AS (

  SELECT
    PARSE_JSON(payload) AS payload,
    uploaded_at
  FROM {{ source('flaky_tests','rspec_flaky_tests') }}

),

final AS (

  SELECT
    payload['created_at']::TIMESTAMP               AS created_at,
    payload['description']::VARCHAR                AS rspec_flaky_test_description,
    payload['feature_category']::VARCHAR           AS feature_category,
    payload['filename']::VARCHAR                   AS rspec_flaky_test_filename,
    payload['gitlab_project_id']::NUMBER           AS gitlab_project_id,
    payload['product_group']::VARCHAR              AS product_group,
    payload['hash']::VARCHAR                       AS rspec_flaky_test_hash,
    payload['id']::VARCHAR                         AS rspec_flaky_test_id,
    payload['issue_url']::VARCHAR                  AS issue_url,
    payload['job_id']::NUMBER                      AS job_id,
    payload['job_web_url']::VARCHAR                AS job_web_url,
    payload['pipeline_id']::NUMBER                 AS pipeline_id,
    payload['pipeline_ref']::VARCHAR               AS pipeline_ref,
    payload['pipeline_web_url']::VARCHAR           AS pipeline_web_url,
    payload['stacktrace']::VARCHAR                 AS stacktrace,
    payload['test_level']::VARCHAR                 AS test_level,
    uploaded_at                                    AS uploaded_at,
    {{ dbt_utils.generate_surrogate_key(['feature_category', 'created_at', 'rspec_flaky_test_description', 'rspec_flaky_test_filename', 'gitlab_project_id', 'rspec_flaky_test_id', 'product_group', 'rspec_flaky_test_hash', 'issue_url', 'job_id', 'job_web_url', 'pipeline_id', 'pipeline_ref', 'pipeline_web_url', 'stacktrace', 'test_level', 'uploaded_at']) }} AS combined_composite_keys
  FROM source
)

SELECT *
FROM final
QUALIFY ROW_NUMBER() OVER (PARTITION BY combined_composite_keys ORDER BY uploaded_at DESC) = 1
