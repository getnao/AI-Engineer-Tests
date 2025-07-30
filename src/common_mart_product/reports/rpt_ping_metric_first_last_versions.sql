{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table",
    snowflake_warehouse=generate_warehouse_name('XL')
) }}


{{ simple_cte([
    ('dim_gitlab_releases', 'dim_gitlab_releases'),
    ('dim_ping_metric','dim_ping_metric'),
    ('prep_ping_instance_flattened', 'prep_ping_instance_flattened')
    ])
}},

filter AS (
  SELECT
    prep_ping_instance_flattened.metrics_path,
    prep_ping_instance_flattened.main_edition                                                                                   AS ping_edition,
    IFF(prep_ping_instance_flattened.version ILIKE '%-pre', TRUE, FALSE)                                                        AS version_is_prerelease,
    (SPLIT_PART(REGEXP_REPLACE(NULLIF(prep_ping_instance_flattened.version, ''), '[^0-9.]+'), '.', 1)::NUMBER * 100 +
                     SPLIT_PART(REGEXP_REPLACE(NULLIF(prep_ping_instance_flattened.version, ''), '[^0-9.]+'), '.', 2)::NUMBER)  AS version_number,
    (SPLIT_PART(REGEXP_REPLACE(NULLIF(prep_ping_instance_flattened.version, ''), '[^0-9.]+'), '.', 1)::VARCHAR || '.' ||
                     SPLIT_PART(REGEXP_REPLACE(NULLIF(prep_ping_instance_flattened.version, ''), '[^0-9.]+'), '.', 2)::VARCHAR) AS version_string,
    prep_ping_instance_flattened.dim_installation_id
  FROM prep_ping_instance_flattened
  INNER JOIN dim_gitlab_releases
    ON version_string = dim_gitlab_releases.major_minor_version
  WHERE prep_ping_instance_flattened.dim_installation_id != 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
),

aggregation AS (
  SELECT
    metrics_path,
    ping_edition,
    version_is_prerelease,
    ARRAY_SORT(ARRAY_UNIQUE_AGG(version_number)) AS version_numbers,
    HLL(dim_installation_id)                     AS installation_counts_hll
  FROM filter
  GROUP BY 1, 2, 3
),

details AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['aggregation.metrics_path', 'aggregation.ping_edition', 'aggregation.version_is_prerelease']) }} AS ping_metric_first_last_versions_id,
    aggregation.metrics_path,
    aggregation.ping_edition,
    aggregation.version_is_prerelease,
    aggregation.version_numbers,
    GET(aggregation.version_numbers, 0)                                                               AS first_major_minor_version_id_with_counter,
    TRUNC(first_major_minor_version_id_with_counter / 100, 0)                                         AS first_major_version_with_counter,
    (first_major_minor_version_id_with_counter - (first_major_version_with_counter * 100))            AS first_minor_version_with_counter,
    CAST (first_major_version_with_counter AS VARCHAR) || '.'
    || CAST (first_minor_version_with_counter AS VARCHAR)                                             AS first_major_minor_version_with_counter,
    GET(aggregation.version_numbers, ARRAY_SIZE(aggregation.version_numbers) - 1)                     AS last_major_minor_version_id_with_counter,
    TRUNC(last_major_minor_version_id_with_counter / 100, 0)                                          AS last_major_version_with_counter,
    (last_major_minor_version_id_with_counter - (last_major_version_with_counter * 100))              AS last_minor_version_with_counter,
    CAST (last_major_version_with_counter AS VARCHAR) || '.'
    || CAST (last_minor_version_with_counter AS VARCHAR)                                              AS last_major_minor_version_with_counter,
    aggregation.installation_counts_hll                                                               AS dim_installation_count
  FROM aggregation
  INNER JOIN dim_ping_metric
    ON aggregation.metrics_path = dim_ping_metric.metrics_path
)

{{ dbt_audit(
    cte_ref="details",
    created_by="@icooper-acp",
    updated_by="@pempey",
    created_date="2022-04-07",
    updated_date="2025-06-17"
) }}
