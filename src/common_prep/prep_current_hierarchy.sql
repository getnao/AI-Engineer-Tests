WITH workday_hcm_organization_source AS (

    SELECT *
    FROM {{ ref('workday_hcm_organization_source') }} 
  
  ),

  workday_hcm_worker_source AS (

    SELECT *
    FROM {{ ref('workday_hcm_worker_source') }}
  
  ),

  workday_hcm_person_name_source AS (

    SELECT *
    FROM {{ ref('workday_hcm_person_name_source') }}
  ),

supervisory_org_stage AS (
    SELECT
        id                                             AS team_id,
        organization_code                              AS team_code,
        name                                           AS team_name,
        manager_id                                     AS team_manager_id,
        is_active                                      AS is_team_active,
        superior_organization_id                       AS superior_team_id,
        IFF(top_level_organization_id = id,TRUE,FALSE) AS is_top_level,
        top_level_organization_id,
        CURRENT_DATE                                   AS report_date
    FROM workday_hcm_organization_source
    WHERE type = 'Supervisory'
      AND sub_type IN ('Team', 'Top Level')
      AND availability_date::DATE <= CURRENT_DATE
      AND _fivetran_deleted = FALSE
),

worker_id_map AS (
    SELECT
      id           AS worker_id,
      employee_id      AS employee_id,
      first_name   AS preferred_first_name,
      last_name    AS preferred_last_name
    FROM workday_hcm_worker_source
    LEFT JOIN workday_hcm_person_name_source ON id = personal_info_system_id 
      AND 'preferred' = type
),

supervisory_org AS (
    SELECT
        supervisory_org_stage.team_id,
        supervisory_org_stage.team_code,
        supervisory_org_stage.team_name,
        supervisory_org_stage.team_manager_id                AS team_manager_workday_id,
        worker_id_map.employee_id                            AS team_manager_employee_id,
        concat(preferred_first_name,' ',preferred_last_name) AS team_manager_name,
        superior_team_id,
        top_level_organization_id,
        is_top_level,
        is_team_active,
        report_date
    FROM supervisory_org_stage
    LEFT JOIN worker_id_map ON supervisory_org_stage.team_manager_id = worker_id_map.worker_id
),

-- Store all teams for later lookup
all_teams AS (
    SELECT * FROM supervisory_org
),

-- Recursive CTE to build the hierarchy
hierarchy_path AS (
    -- Base case: start with top-level teams
    SELECT
        team_id,
        team_code,
        team_name,
        team_manager_employee_id,
        team_manager_name,
        is_team_active,
        1 AS hierarchy_level,
        ARRAY_CONSTRUCT(team_id) AS team_ids,
        ARRAY_CONSTRUCT(team_code) AS team_codes,
        ARRAY_CONSTRUCT(team_name) AS team_names,
        ARRAY_CONSTRUCT(team_manager_employee_id) AS team_manager_employee_ids,
        ARRAY_CONSTRUCT(team_manager_name) AS team_manager_names
    FROM supervisory_org
    WHERE is_top_level = TRUE
    
    UNION ALL
    
    -- Recursive case: join child teams to their parents
    SELECT
        child.team_id,
        child.team_code,
        child.team_name,
        child.team_manager_employee_id,
        child.team_manager_name,
        child.is_team_active,
        parent.hierarchy_level + 1 AS hierarchy_level,
        ARRAY_APPEND(parent.team_ids, child.team_id) AS team_ids,
        ARRAY_APPEND(parent.team_codes, child.team_code) AS team_codes,
        ARRAY_APPEND(parent.team_names, child.team_name) AS team_names,
        ARRAY_APPEND(parent.team_manager_employee_ids, child.team_manager_employee_id) AS team_manager_employee_ids,
        ARRAY_APPEND(parent.team_manager_names, child.team_manager_name) AS team_manager_names
    FROM supervisory_org AS child
    INNER JOIN hierarchy_path AS parent ON child.superior_team_id = parent.team_id
    WHERE parent.hierarchy_level < 10  -- Limit to 10 levels
),
--
-- Final result with all hierarchy levels
final AS (
    SELECT 
        -- Macro to handle up to 10 levels dynamically
        team_id,
        team_code,
        team_name,
        team_manager_employee_id,
        team_manager_name,
        is_team_active,
        hierarchy_level,
        {% for level in range(10) %}
        {% set level_num = level + 1  %}
        team_names[{{ level }}]::VARCHAR AS lvl_{{ level_num }}_team_name,
        team_manager_names[{{ level }}]::VARCHAR AS lvl_{{ level_num }}_team_manager_name,
        team_manager_employee_ids[{{ level }}]::VARCHAR AS lvl_{{ level_num }}_team_manager_employee_id,
        team_codes[{{ level }}]::VARCHAR AS lvl_{{ level_num }}_team_code,
        team_ids[{{ level }}]::VARCHAR AS lvl_{{ level_num }}_team_id
        {% if not loop.last %},{% endif %}
        {% endfor %}

    FROM hierarchy_path
)

SELECT * FROM final
