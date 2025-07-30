
WITH base AS (

  SELECT *
  FROM {{ source('clickhouse_billing', 'usage_cost') }}

), extracted AS (

  SELECT try_parse_json(jsontext) AS json_text,
         uploaded_at              AS uploaded_at
  FROM base

), flattened AS (

  SELECT json_text:"environment_name"       AS environment_name,
         json_text:"result":"grandTotalCHC" AS grand_total_chc,
         VALUE                              AS VALUE,
         index                              AS index,
         uploaded_at                        AS uploaded_at
  FROM extracted,
  LATERAL FLATTEN(input => json_text:result:costs)

), formatted AS (

  SELECT environment_name::VARCHAR                              AS environment_name,
         VALUE:"date"::DATE                                     AS cost_date,
         VALUE:"dataWarehouseId"::VARCHAR                       AS data_warehouse_id,
         index::NUMBER                                          AS index,
         VALUE:"serviceId"::VARCHAR                             AS service_id,
         VALUE:"entityType"::VARCHAR                            AS entity_type,
         VALUE:"entityId"::VARCHAR                              AS entity_id,
         VALUE:"entityName"::VARCHAR                            AS entity_name,
         VALUE:"organizationTier"::VARCHAR                      AS organization_tier,
         VALUE:"totalCHC"::FLOAT                                AS total_chc,
         VALUE:"discount"::FLOAT                                AS discount,
         VALUE:metrics:"interRegionTier1DataTransferCHC"::FLOAT AS inter_region_tier1_chc,
         VALUE:metrics:"interRegionTier2DataTransferCHC"::FLOAT AS inter_region_tier2_chc,
         VALUE:metrics:"interRegionTier3DataTransferCHC"::FLOAT AS inter_region_tier3_chc,
         VALUE:metrics:"interRegionTier4DataTransferCHC"::FLOAT AS inter_region_tier4_chc,
         VALUE:metrics:"publicDataTransferCHC"::FLOAT           AS public_data_transfer_chc,
         VALUE:metrics:"computeCHC"::FLOAT                      AS compute_chc,
         grand_total_chc::FLOAT                                 AS grand_total_chc,
         'USD'::VARCHAR                                         AS currency,
         uploaded_at::TIMESTAMP                                 AS uploaded_at
  FROM flattened

), deduped AS (

  SELECT *
  FROM formatted
  QUALIFY ROW_NUMBER() OVER (PARTITION BY environment_name,cost_date, data_warehouse_id, index ORDER BY uploaded_at DESC) = 1

)

  SELECT *
  FROM deduped