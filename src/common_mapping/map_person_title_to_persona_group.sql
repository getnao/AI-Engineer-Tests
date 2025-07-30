{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('prep_crm_person', 'prep_crm_person'),
    ('persona_mapping_rules', 'persona_mapping_rules'),
    ('sheetload_persona_mapping_keywords_source', 'sheetload_persona_mapping_keywords_source')
]) }}

, build_persona_matrix AS (

    SELECT 
        dim_crm_person_id,
        lower(prep_crm_person.title) as lower_title,
        COALESCE(
            keyword_c_level.title_group,
            keyword_short_c_level.title_group, 
            keyword_exact_c_level.title_group,
            keyword_contains.title_group
            ) as title_group,
        COALESCE(
            keyword_c_level.title_keyword,
            keyword_short_c_level.title_keyword,
            keyword_exact_c_level.title_keyword,
            keyword_contains.title_keyword
        ) as title_keyword,
    FROM prep_crm_person
    LEFT JOIN sheetload_persona_mapping_keywords_source keyword_contains
        ON CONTAINS(lower_title, keyword_contains.title_keyword)
        AND keyword_contains.title_group IN ('security_titles', 'upper_management_titles', 
                            'release_change_mgmt_titles', 'edu_titles', 'program_mgmt_titles',
                            'ic_developer_titles', 'non_dev_bakeyword_contains_office_titles', 'platform_ops_titles',
                            'not_enough_info_titles', 'sales_exclusions')
    
    LEFT JOIN sheetload_persona_mapping_keywords_source keyword_c_level
        ON CONTAINS(lower_title, keyword_c_level.title_keyword)
        AND keyword_c_level.title_group = 'c_level_titles'
        AND NOT contains(lower_title, 'vice')
    
    LEFT JOIN sheetload_persona_mapping_keywords_source keyword_short_c_level
        ON keyword_short_c_level.title_group = 'short_c_level_titles'
        AND REGEXP_LIKE(lower_title, '\\b' || keyword_short_c_level.title_keyword || '\\b')
    
    LEFT JOIN sheetload_persona_mapping_keywords_source keyword_exact_c_level
        ON keyword_exact_c_level.title_group = 'exact_c_level_titles'
        AND lower_title = keyword_exact_c_level.title_keyword
    WHERE
        prep_crm_person.created_date >= '2023-01-01'
        AND LENGTH(title) > 1
    GROUP BY ALL

), link_to_rankings AS (
    SELECT
        build_persona_matrix.dim_crm_person_id,
        build_persona_matrix.lower_title,
        build_persona_matrix.title_group,
        build_persona_matrix.title_keyword,
        persona_mapping_rules.persona_category,
       
        MAX(persona_mapping_rules.is_management_indicator) 
            OVER (PARTITION BY build_persona_matrix.dim_crm_person_id
            ) AS is_management,
       
        ROW_NUMBER() 
            OVER (PARTITION BY build_persona_matrix.dim_crm_person_id 
                    ORDER BY persona_mapping_rules.priority_order ASC
            ) AS priority_rank,
        
        MAX(CASE WHEN build_persona_matrix.title_group = 'sales_exclusions' 
                THEN TRUE ELSE FALSE END) 
                OVER (PARTITION BY dim_crm_person_id
            ) AS has_sales_exclusion,

        CASE WHEN persona_mapping_rules.requires_not_sales_exclusion AND has_sales_exclusion 
                THEN FALSE ELSE TRUE END
            AS is_valid_persona_match

    FROM build_persona_matrix
    INNER JOIN persona_mapping_rules
        ON build_persona_matrix.title_group = persona_mapping_rules.title_group
    QUALIFY priority_rank = 1 AND is_valid_persona_match
)

select
*
from
link_to_rankings
