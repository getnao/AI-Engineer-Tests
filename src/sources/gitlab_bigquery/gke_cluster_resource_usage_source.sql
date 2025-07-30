-- This will likely need to be made incremental.

WITH source AS (

    select
        date_part::DATE                       AS file_date_part,
        value['_partition_date']::DATE        AS partition_date,
        value['start_time']::NUMBER           AS start_time,
        value['end_time']::NUMBER             AS end_time,
        value['cloud_resource_size']::NUMBER  AS cloud_resource_size,
        value['cluster_location']::VARCHAR    AS cluster_location,
        value['cluster_name']::VARCHAR        AS cluster_name,
        value['fraction']::NUMBER             AS fraction,
        value['resource_name']::VARCHAR       AS resource_name,
        value['sku_id']::VARCHAR              AS sku_id,
        value['gcs_export_time']::NUMBER      AS gcs_export_time,
        value['namespace']::VARCHAR           AS namespace,
        value['project']['id']::VARCHAR       AS project_id,
        value['labels']::VARIANT              AS labels,
        value::VARIANT                        AS json_value

    FROM {{ source('gitlab_bigquery','gke_cluster_resource_usage') }}

)

SELECT *
FROM source