{{ config(
    tags=["mnpi_exception"],
    materialized="table"
) }}

WITH prep_crm_person AS (

  SELECT
    dim_crm_person_id,
    sfdc_record_id
  FROM {{ ref('prep_crm_person') }}

), bizible_attribution_touchpoint_source AS (

    SELECT 
      sfdc_bizible_attribution_touchpoint_source.*,
      prep_crm_person.dim_crm_person_id,
      --UTMs not captured by the Bizible - Landing Page
    PARSE_URL(bizible_landing_page_raw)['parameters']['utm_campaign']::VARCHAR    AS bizible_landing_page_utm_campaign,
    PARSE_URL(bizible_landing_page_raw)['parameters']['utm_medium']::VARCHAR      AS bizible_landing_page_utm_medium,
    PARSE_URL(bizible_landing_page_raw)['parameters']['utm_source']::VARCHAR      AS bizible_landing_page_utm_source,
    --UTMs not captured by the Bizible - Form Page
    PARSE_URL(bizible_form_url_raw)['parameters']['utm_campaign']::VARCHAR        AS bizible_form_page_utm_campaign,
    PARSE_URL(bizible_form_url_raw)['parameters']['utm_medium']::VARCHAR          AS bizible_form_page_utm_medium,
    PARSE_URL(bizible_form_url_raw)['parameters']['utm_source']::VARCHAR          AS bizible_form_page_utm_source,

    --Final UTM Parameters
    COALESCE(bizible_landing_page_utm_campaign, bizible_form_page_utm_campaign)   AS utm_campaign,
    COALESCE(bizible_landing_page_utm_medium, bizible_form_page_utm_medium)       AS utm_medium,
    COALESCE(bizible_landing_page_utm_source, bizible_form_page_utm_source)       AS utm_source,
       {{ dbt_utils.generate_surrogate_key([
        'prep_crm_person.dim_crm_person_id',
        'bizible_marketing_channel',
        'bizible_ad_campaign_name',
        'bizible_touchpoint_source',
        'bizible_touchpoint_source_type',
        'utm_source',
        'utm_medium',
        'utm_campaign',
        'utm_content',
        'bizible_form_url'
    ]) }} AS touchpoint_composite_key
    FROM {{ ref('sfdc_bizible_attribution_touchpoint_source') }}
    LEFT JOIN prep_crm_person
      ON sfdc_bizible_attribution_touchpoint_source.bizible_contact=prep_crm_person.sfdc_record_id
    WHERE sfdc_bizible_attribution_touchpoint_source.is_deleted = 'FALSE'

), bizible_attribution_touchpoint_base AS (

  SELECT DISTINCT 
    bizible_attribution_touchpoint_source.*,
    REPLACE(LOWER(bizible_form_url),'.html','') AS bizible_form_url_clean,
    pathfactory_content_type,
    prep_campaign.type
  FROM bizible_attribution_touchpoint_source
  LEFT JOIN {{ ref('sheetload_bizible_to_pathfactory_mapping') }}  
    ON bizible_form_url_clean=bizible_url
  LEFT JOIN {{ ref('prep_campaign') }}
      ON bizible_attribution_touchpoint_source.campaign_id = prep_campaign.dim_campaign_id

), bizible_touchpoint_source_base AS (

  SELECT
    touchpoint_id AS dim_crm_touchpoint_id,
    prep_crm_person.dim_crm_person_id,
    bizible_touchpoint_date,
    bizible_touchpoint_position,
    bizible_marketing_channel,
    bizible_marketing_channel_path,
    bizible_touchpoint_source,
    PARSE_URL(bizible_landing_page_raw)['parameters']['utm_campaign']::VARCHAR    AS bizible_landing_page_utm_campaign,
    PARSE_URL(bizible_landing_page_raw)['parameters']['utm_medium']::VARCHAR      AS bizible_landing_page_utm_medium,
    PARSE_URL(bizible_landing_page_raw)['parameters']['utm_source']::VARCHAR      AS bizible_landing_page_utm_source,
    --UTMs not captured by the Bizible - Form Page
    PARSE_URL(bizible_form_url_raw)['parameters']['utm_campaign']::VARCHAR        AS bizible_form_page_utm_campaign,
    PARSE_URL(bizible_form_url_raw)['parameters']['utm_medium']::VARCHAR          AS bizible_form_page_utm_medium,
    PARSE_URL(bizible_form_url_raw)['parameters']['utm_source']::VARCHAR          AS bizible_form_page_utm_source,

    --Final UTM Parameters
    COALESCE(bizible_landing_page_utm_campaign, bizible_form_page_utm_campaign)   AS utm_campaign,
    COALESCE(bizible_landing_page_utm_medium, bizible_form_page_utm_medium)       AS utm_medium,
    COALESCE(bizible_landing_page_utm_source, bizible_form_page_utm_source)       AS utm_source,
    -- Create a comprehensive composite key using all relevant fields
    {{ dbt_utils.generate_surrogate_key([
        'prep_crm_person.dim_crm_person_id',
        'bizible_marketing_channel',
        'bizible_ad_campaign_name',
        'bizible_touchpoint_source',
        'bizible_touchpoint_source_type',
        'utm_source',
        'utm_medium',
        'utm_campaign',
        'utm_content',
        'bizible_form_url'
    ]) }} AS touchpoint_composite_key
    FROM {{ ref('sfdc_bizible_touchpoint_source') }}
    LEFT JOIN prep_crm_person
      ON sfdc_bizible_touchpoint_source.bizible_contact=prep_crm_person.sfdc_record_id

-- Map attribution touchpoints to base touchpoints
), touchpoint_mapping AS (

  SELECT
        bizible_attribution_touchpoint_source.touchpoint_id AS dim_crm_attribution_touchpoint_id,
        bizible_attribution_touchpoint_source.dim_crm_person_id AS attr_person_id,
        bizible_attribution_touchpoint_source.touchpoint_composite_key,
        bizible_attribution_touchpoint_source.bizible_touchpoint_date AS attr_date_time,
        bizible_attribution_touchpoint_source.bizible_touchpoint_position AS attr_position,
        bizible_attribution_touchpoint_source.bizible_marketing_channel AS attr_channel,
        bizible_attribution_touchpoint_source.bizible_marketing_channel_path AS attr_channel_path,
        bizible_attribution_touchpoint_source.bizible_touchpoint_source AS attr_source,
        bizible_attribution_touchpoint_source.bizible_form_url_raw,
        bizible_touchpoint_source_base.dim_crm_touchpoint_id,
        bizible_touchpoint_source_base.dim_crm_person_id AS touch_person_id,
        bizible_touchpoint_source_base.bizible_touchpoint_date AS touch_date_time,
        bizible_touchpoint_source_base.bizible_touchpoint_position AS touch_position,
        bizible_touchpoint_source_base.bizible_marketing_channel AS touch_channel,
        bizible_touchpoint_source_base.bizible_marketing_channel_path AS touch_channel_path,
        bizible_touchpoint_source_base.bizible_touchpoint_source AS touch_source,
        DATEDIFF('second',IFNULL(bizible_touchpoint_source_base.bizible_touchpoint_date,bizible_attribution_touchpoint_source.bizible_touchpoint_date),bizible_attribution_touchpoint_source.bizible_touchpoint_date) AS time_diff
    FROM bizible_attribution_touchpoint_source 
    LEFT JOIN bizible_touchpoint_source_base 
        ON bizible_attribution_touchpoint_source.touchpoint_composite_key = bizible_touchpoint_source_base.touchpoint_composite_key
    QUALIFY ROW_NUMBER() OVER (PARTITION BY bizible_attribution_touchpoint_source.touchpoint_id ORDER BY ABS(time_diff) ASC) = 1

), eliminate_problem_records AS (

  SELECT 
    dim_crm_attribution_touchpoint_id,
    COUNT(DISTINCT dim_crm_touchpoint_id) AS match_count
  FROM touchpoint_mapping
  GROUP BY 1
  HAVING match_count = 1

), final_touchpoint_join AS (

  SELECT
    eliminate_problem_records.dim_crm_attribution_touchpoint_id,
    dim_crm_touchpoint_id AS dim_crm_buyer_touchpoint_id
  FROM eliminate_problem_records
  LEFT JOIN touchpoint_mapping
    ON eliminate_problem_records.dim_crm_attribution_touchpoint_id = touchpoint_mapping.dim_crm_attribution_touchpoint_id

), final AS (

  SELECT
    bizible_attribution_touchpoint_base.*,
    final_touchpoint_join.dim_crm_buyer_touchpoint_id,
    {{ bizible_touchpoint_offer_type('bizible_touchpoint_type', 'bizible_ad_campaign_name', 'bizible_form_url_clean', 'bizible_marketing_channel', 'type', 'pathfactory_content_type', 'bizible_marketing_channel_path') }}
  FROM bizible_attribution_touchpoint_base
  LEFT JOIN final_touchpoint_join
    ON bizible_attribution_touchpoint_base.touchpoint_id=final_touchpoint_join.dim_crm_attribution_touchpoint_id

)

SELECT *
FROM final