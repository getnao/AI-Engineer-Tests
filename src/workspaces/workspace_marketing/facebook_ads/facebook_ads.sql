{{ simple_cte([
    ('facebook_ads_ad_history_source','facebook_ads_ad_history_source'),
    ('facebook_ads_creative_history_source','facebook_ads_creative_history_source'),
    ('facebook_ads_basic_all_levels_source','facebook_ads_basic_all_levels_source'),
    ('facebook_ads_basic_all_levels_actions_source','facebook_ads_basic_all_levels_actions_source')
  ]) 
}}

, find_current_ads AS (
  SELECT
    *,
    MAX(
      updated_time
    ) OVER (PARTITION BY ad_id ORDER BY updated_time DESC) AS latest_update,
    IFF(latest_update = updated_time, TRUE, FALSE) AS is_latest
  FROM facebook_ads_ad_history_source

), current_ads AS (

  SELECT *
  FROM find_current_ads
  WHERE is_latest

), find_current_creatives AS (

  SELECT
    *,
    MAX(
      _fivetran_synced
    ) OVER (PARTITION BY creative_id ORDER BY _fivetran_synced DESC) AS latest_update,
    IFF(latest_update = _fivetran_synced, TRUE, FALSE) AS is_latest
  FROM facebook_ads_creative_history_source

),current_creatives AS (

  SELECT *
  FROM find_current_creatives
  WHERE is_latest

), basic_all_levels as (

    SELECT * 
    FROM facebook_ads_basic_all_levels_source

), fb_pixel_source as (

    SELECT 
      ad_id,
      ad_date,
      SUM(value) AS pixel_value
    FROM facebook_ads_basic_all_levels_actions_source
    WHERE action_type = 'offsite_conversion.fb_pixel_custom'
    GROUP BY 1,2

), fb_landing_page_source as (

    SELECT 
      ad_id,
      ad_date,
      SUM(value) AS landing_page_value
    FROM facebook_ads_basic_all_levels_actions_source
    WHERE action_type = 'landing_page_view'
    GROUP BY 1,2

)

SELECT DISTINCT
    /* Account Info */
    basic_all_levels.account_id,
    
    /* Campaign Info */
    current_ads.campaign_id,
    basic_all_levels.campaign_name,
    basic_all_levels.adset_name,
    current_ads.ad_name              AS ad_name,
    current_ads.ad_status            AS ad_status,
    

    /* Creative Info */

    current_creatives.creative_name,
    current_creatives.object_type      AS creative_type,
    current_creatives.creative_status  AS creative_status,
    current_creatives.page_link,
    current_creatives.body        AS text_ad_text,
    current_creatives.title       AS text_ad_title,
    
    /* Creative Stats */
    basic_all_levels.ad_date         AS campaign_day,
    basic_all_levels.impressions,
    basic_all_levels.inline_link_clicks,
    basic_all_levels.spend,
    fb_pixel_source.pixel_value   AS fb_pixel_value,
    fb_landing_page_source.landing_page_value  AS fb_landing_page_value
FROM basic_all_levels
LEFT JOIN current_ads 
  ON basic_all_levels.ad_id = current_ads.ad_id
LEFT JOIN current_creatives 
  ON current_ads.creative_id = current_creatives.creative_id
LEFT JOIN fb_pixel_source
  ON basic_all_levels.ad_id = fb_pixel_source.ad_id
    AND basic_all_levels.ad_date = fb_pixel_source.ad_date
LEFT JOIN fb_landing_page_source
  ON basic_all_levels.ad_id = fb_landing_page_source.ad_id
    AND basic_all_levels.ad_date = fb_landing_page_source.ad_date