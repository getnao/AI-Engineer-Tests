WITH parse_keystone AS (

    SELECT *
    FROM {{ ref('content_keystone_source') }},
        
), union_types AS (

    SELECT
        content_name,
        gitlab_epic,
        language,
        gtm,
        type,
        url_slug,
        'form_urls' AS join_string_type,
        flattened_parsed_keystone.value::VARCHAR AS join_string
    FROM parse_keystone, 
        LATERAL FLATTEN(input => parse_keystone.full_value:form_urls) flattened_parsed_keystone
    UNION ALL
    SELECT
        content_name,
        gitlab_epic,
        language,
        gtm,
        type,
        url_slug,
        'landing_page_url' AS join_string_type,
        flattened_parsed_keystone.value::VARCHAR AS join_string
    FROM parse_keystone,
        LATERAL FLATTEN(input => parse_keystone.full_value:landing_page_urls) flattened_parsed_keystone
    UNION ALL
    SELECT
        content_name,
        gitlab_epic,
        language,
        gtm,
        type,
        url_slug,
        'utm_campaign_name' AS join_string_type,
        flattened_parsed_keystone.value::VARCHAR AS join_string
    FROM parse_keystone,
        LATERAL FLATTEN(input => parse_keystone.full_value:utm_campaign_name) flattened_parsed_keystone
    UNION ALL
    SELECT
        content_name,
        gitlab_epic,
        language,
        gtm,
        type,
        url_slug,
        'utm_content_name' AS join_string_type,
        flattened_parsed_keystone.value::VARCHAR AS join_string
    FROM parse_keystone,
        LATERAL FLATTEN(input => parse_keystone.full_value:utm_content_name) flattened_parsed_keystone
    UNION ALL
    SELECT
        content_name,
        gitlab_epic,
        language,
        gtm,
        type,
        url_slug,
        'sfdc_campaigns' AS join_string_type,
        flattened_parsed_keystone.value::VARCHAR AS join_string
    FROM parse_keystone,
        LATERAL FLATTEN(input => parse_keystone.full_value:sfdc_campaigns) flattened_parsed_keystone
    UNION ALL
    SELECT
        content_name,
        gitlab_epic,
        language,
        gtm,
        type,
        url_slug,
        'url_slug' AS join_string_type,
        url_slug AS join_string
    FROM parse_keystone

), prep_crm_unioned_touchpoint AS (

    SELECT
        touchpoint_id AS dim_crm_touchpoint_id,
        bizible_touchpoint_date,
        campaign_id AS dim_campaign_id,
        bizible_ad_campaign_name,
        bizible_form_url,
        bizible_landing_page,
        CASE WHEN CONTAINS(bizible_landing_page, 'learn.gitlab.com/') AND split_part(bizible_landing_page, '/', 3) = '' 
            THEN null
            ELSE split_part(bizible_landing_page, '/', 3)
        END AS path_factory_slug_landing_page,
        CASE WHEN CONTAINS(bizible_form_url, 'learn.gitlab.com/') AND split_part(bizible_form_url, '/', 3) = '' 
            THEN null
            ELSE split_part(bizible_form_url, '/', 3)
        END AS path_factory_slug_form_url,
        COALESCE(path_factory_slug_landing_page, path_factory_slug_form_url)          AS pathfactory_slug,
        PARSE_URL(bizible_landing_page_raw)['parameters']['utm_campaign']::VARCHAR    AS bizible_landing_page_utm_campaign,
        PARSE_URL(bizible_form_url_raw)['parameters']['utm_campaign']::VARCHAR        AS bizible_form_page_utm_campaign,
        PARSE_URL(bizible_landing_page_raw)['parameters']['utm_content']::VARCHAR     AS bizible_landing_page_utm_content,
        PARSE_URL(bizible_form_url_raw)['parameters']['utm_content']::VARCHAR         AS bizible_form_page_utm_content,
        COALESCE(bizible_landing_page_utm_campaign, bizible_form_page_utm_campaign)   AS utm_campaign,
        COALESCE(bizible_landing_page_utm_content, bizible_form_page_utm_content)     AS utm_content
    FROM {{ ref('prep_crm_attribution_touchpoint') }}
    UNION ALL
    SELECT
        touchpoint_id AS dim_crm_touchpoint_id,
        bizible_touchpoint_date,
        campaign_id AS dim_campaign_id,
        bizible_ad_campaign_name,
        bizible_form_url,
        bizible_landing_page,
        CASE WHEN CONTAINS(bizible_landing_page, 'learn.gitlab.com/') AND split_part(bizible_landing_page, '/', 3) = '' 
            THEN null
            ELSE split_part(bizible_landing_page, '/', 3)
        END AS path_factory_slug_landing_page,
        CASE WHEN CONTAINS(bizible_form_url, 'learn.gitlab.com/') AND split_part(bizible_form_url, '/', 3) = '' 
            THEN null
            ELSE split_part(bizible_form_url, '/', 3)
        END AS path_factory_slug_form_url,
        COALESCE(path_factory_slug_landing_page, path_factory_slug_form_url)          AS pathfactory_slug,
        PARSE_URL(bizible_landing_page_raw)['parameters']['utm_campaign']::VARCHAR    AS bizible_landing_page_utm_campaign,
        PARSE_URL(bizible_form_url_raw)['parameters']['utm_campaign']::VARCHAR        AS bizible_form_page_utm_campaign,
        PARSE_URL(bizible_landing_page_raw)['parameters']['utm_content']::VARCHAR     AS bizible_landing_page_utm_content,
        PARSE_URL(bizible_form_url_raw)['parameters']['utm_content']::VARCHAR         AS bizible_form_page_utm_content,
        COALESCE(bizible_landing_page_utm_campaign, bizible_form_page_utm_campaign)   AS utm_campaign,
        COALESCE(bizible_landing_page_utm_content, bizible_form_page_utm_content)     AS utm_content
    FROM {{ ref('prep_crm_touchpoint') }}

), content_types as (
    SELECT '/ebook_' as search_string UNION ALL
    SELECT '/report_' UNION ALL 
    SELECT '/onepager_' UNION ALL
    SELECT '/comparison_' UNION ALL
    SELECT '/assessment_' UNION ALL
    SELECT '/blog_' UNION ALL
    SELECT '/presentation_' UNION ALL
    SELECT '/whitepaper_' UNION ALL
    SELECT '/thesource-pf_' UNION ALL
    SELECT '/casestudy_' UNION ALL
    SELECT '/solutionbrief_' UNION ALL
    SELECT '/video_' UNION ALL

    SELECT '/ebook-' UNION ALL
    SELECT '/report-' UNION ALL 
    SELECT '/onepager-' UNION ALL
    SELECT '/comparison-' UNION ALL
    SELECT '/assessment-' UNION ALL
    SELECT '/blog-'      UNION ALL
    SELECT '/presentation-' UNION ALL
    SELECT '/whitepaper-' UNION ALL
    SELECT '/thesource-pf-' UNION ALL
    SELECT '/casestudy-' UNION ALL
    SELECT '/solutionbrief-' UNION ALL
    SELECT '/video-'

), offline_content_type as (
    SELECT '_ebook_' as search_string UNION ALL
    SELECT '_report_' UNION ALL 
    SELECT '_onepager_' UNION ALL
    SELECT '_comparison_' UNION ALL
    SELECT '_assessment_' UNION ALL
    SELECT '_blog_' UNION ALL
    SELECT '_presentation_' UNION ALL
    SELECT '_whitepaper_' UNION ALL
    SELECT '_thesource-pf_' UNION ALL
    SELECT '_casestudy_' UNION ALL
    SELECT '_solutionbrief_' UNION ALL
    SELECT '_video_' UNION ALL

    SELECT '_ebook-' UNION ALL
    SELECT '_report-' UNION ALL 
    SELECT '_onepager-' UNION ALL
    SELECT '_comparison-' UNION ALL
    SELECT '_assessment-' UNION ALL
    SELECT '_blog-' UNION ALL
    SELECT '_presentation-' UNION ALL
    SELECT '_whitepaper-' UNION ALL
    SELECT '_thesource-pf-' UNION ALL
    SELECT '_casestudy-' UNION ALL
    SELECT '_solutionbrief-' UNION ALL
    SELECT '_video-'

), content_languages as (
    SELECT '-fr-fr-' as search_language UNION ALL
    SELECT '-ja-jp-' UNION ALL
    SELECT '-it-it-' UNION ALL
    SELECT '-es-'    UNION ALL
    SELECT '-de-de-' UNION ALL
    SELECT '-pt-br-' UNION ALL
    SELECT '-ko-kr-' UNION ALL
    SELECT '-ru-ru-' UNION ALL
    SELECT '-zh-cn-' UNION ALL
    SELECT '-zh-tw-' UNION ALL
    SELECT '-zh-hk-'

), offline_content_languages as (
    SELECT '_fr-fr_' as search_language UNION ALL
    SELECT '_ja-jp_' UNION ALL
    SELECT '_it-it_' UNION ALL
    SELECT '_es_'  UNION ALL
    SELECT '_de-de_' UNION ALL
    SELECT '_pt-br_' UNION ALL
    SELECT '_ko-kr_' UNION ALL
    SELECT '_ru-ru_' UNION ALL
    SELECT '_zh-cn_' UNION ALL
    SELECT '_zh-tw_' UNION ALL
    SELECT '_zh-hk_'
), create_content_code_offline as (
    //offline touchpoints
    SELECT
        dim_crm_touchpoint_id,
        bizible_ad_campaign_name,
        search_string || SUBSTRING(
            bizible_ad_campaign_name,
            POSITION(search_string IN bizible_ad_campaign_name) + LENGTH(search_string)
        ) AS full_content_string,

        REPLACE(REPLACE(search_string, '/', ''), '_', '') as content_type,

        CASE WHEN offline_content_languages.search_language IS NULL
            THEN 'en' ELSE TRIM(offline_content_languages.search_language, '_') END AS content_language,

        CASE 
        WHEN offline_content_languages.search_language IS NULL THEN
            REPLACE(full_content_string, search_string, '')
        WHEN right(full_content_string, 5) = 'ja-jp' THEN
            //support legacy naming
            TRIM(REPLACE(REPLACE(full_content_string, search_string, ''), content_language, ''), '_') || 'ja-jp'
        ELSE 
            TRIM(REPLACE(REPLACE(full_content_string, search_string, ''), content_language, ''), '_')
        END AS content_key

    FROM prep_crm_unioned_touchpoint
    JOIN offline_content_type
        ON CONTAINS(bizible_ad_campaign_name, search_string)
    LEFT JOIN offline_content_languages
        ON CONTAINS(bizible_ad_campaign_name, search_language)
    WHERE
    bizible_form_url IS NULL
    -- we created the content code mapping system at the start of FY26
    AND bizible_touchpoint_date > '2025-02-01'

), create_content_code_online AS (
    SELECT 
        dim_crm_touchpoint_id,

        search_string || SUBSTRING(
            bizible_form_url,
            POSITION(search_string IN bizible_form_url) + LENGTH(search_string)
        ) AS full_content_form_string,
        
        REPLACE(REPLACE(search_string, '/', ''), '-', '') AS content_type,

        CASE WHEN content_languages.search_language IS NULL
            THEN 'en' ELSE TRIM(content_languages.search_language, '-') END AS content_language,

        CASE WHEN content_languages.search_language IS NULL THEN 
            REPLACE(full_content_form_string, search_string, '')
        ELSE 
            TRIM(REPLACE(REPLACE(full_content_form_string, search_string, ''), content_language, ''), '-')
        END AS content_key_form_prep,
        CASE WHEN CONTAINS(content_key_form_prep, '/') THEN NULL ELSE content_key_form_prep END AS content_key,

        FROM prep_crm_unioned_touchpoint
        JOIN content_types
            ON CONTAINS(bizible_form_url, search_string) 
        LEFT JOIN content_languages
            ON CONTAINS(bizible_form_url, search_language) 
    WHERE
    -- we created the content code mapping system at the start of FY26
    bizible_touchpoint_date > '2025-02-01'
    AND content_key IS NOT NULL

 ), combined_model AS (
 
    SELECT
        prep_crm_unioned_touchpoint.dim_crm_touchpoint_id,
        COALESCE(
            sfdc_campaigns.content_name,
            form_urls.content_name,
            pathfactory_slug.content_name,
            create_content_code_online.content_key,
            create_content_code_offline.content_key
        ) AS content_name,
        COALESCE(
            sfdc_campaigns.gitlab_epic,
            form_urls.gitlab_epic,
            pathfactory_slug.gitlab_epic
        ) AS gitlab_epic,
        COALESCE(
            sfdc_campaigns.language,
            form_urls.language,
            pathfactory_slug.language,
            create_content_code_online.content_language,
            create_content_code_offline.content_language
        ) AS language,
        COALESCE(
            sfdc_campaigns.gtm,
            form_urls.gtm,
            pathfactory_slug.gtm
        ) AS gtm,
        COALESCE(
            sfdc_campaigns.url_slug,
            form_urls.url_slug,
            pathfactory_slug.url_slug,
            create_content_code_online.content_key,
            create_content_code_offline.content_key
        ) AS url_slug,
        COALESCE(
            sfdc_campaigns.type,
            form_urls.type,
            pathfactory_slug.type,
            create_content_code_online.content_type,
            create_content_code_offline.content_type
        ) AS type
    FROM prep_crm_unioned_touchpoint
    LEFT JOIN union_types sfdc_campaigns
        ON sfdc_campaigns.join_string_type = 'sfdc_campaigns'
        AND prep_crm_unioned_touchpoint.dim_campaign_id = sfdc_campaigns.join_string
    LEFT JOIN union_types form_urls
        ON form_urls.join_string_type = 'form_urls'
        AND prep_crm_unioned_touchpoint.bizible_form_url = form_urls.join_string
    LEFT JOIN union_types pathfactory_slug
        ON pathfactory_slug.join_string_type = 'url_slug'
        AND prep_crm_unioned_touchpoint.pathfactory_slug = pathfactory_slug.join_string
    LEFT JOIN create_content_code_offline
        ON create_content_code_offline.dim_crm_touchpoint_id = prep_crm_unioned_touchpoint.dim_crm_touchpoint_id
    LEFT JOIN create_content_code_online
        ON create_content_code_online.dim_crm_touchpoint_id = prep_crm_unioned_touchpoint.dim_crm_touchpoint_id

)

SELECT *
FROM combined_model
WHERE content_name IS NOT NULL
QUALIFY ROW_NUMBER () OVER (PARTITION BY dim_crm_touchpoint_id ORDER BY content_name,type ) = 1