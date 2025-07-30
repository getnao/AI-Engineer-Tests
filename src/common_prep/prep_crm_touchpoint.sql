{{ config(
    tags=["mnpi_exception"],
    materialized="table"
) }}

WITH bizible_person_touchpoint_source AS (

    SELECT *
    FROM {{ref('sfdc_bizible_touchpoint_source')}}
    WHERE is_deleted = 'FALSE'
    
), bizible_person_touchpoint_base AS (

  SELECT DISTINCT 
    bizible_person_touchpoint_source.*,
    LOWER(bizible_form_url) AS bizible_form_url_prep,
    REPLACE(bizible_form_url_prep,'.html','') AS bizible_form_url_clean,
    pathfactory_content_type,
    prep_campaign.type
  FROM bizible_person_touchpoint_source
  LEFT JOIN {{ ref('sheetload_bizible_to_pathfactory_mapping') }}  
    ON bizible_form_url_clean=bizible_url
  LEFT JOIN {{ ref('prep_campaign') }}
      ON bizible_person_touchpoint_source.campaign_id = prep_campaign.dim_campaign_id

), final AS (

  SELECT
    bizible_person_touchpoint_base.*,
     {{ bizible_touchpoint_offer_type('bizible_touchpoint_type', 'bizible_ad_campaign_name', 'bizible_form_url_clean', 'bizible_marketing_channel', 'type', 'pathfactory_content_type', 'bizible_marketing_channel_path') }}
  FROM bizible_person_touchpoint_base

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2024-01-31",
    updated_date="2024-12-18"
) }}