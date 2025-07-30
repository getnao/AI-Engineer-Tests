WITH prep_current_hierarchy AS (

  SELECT *
  FROM {{ ref('prep_current_hierarchy') }}
  
),

hierarchy_array AS (

  SELECT
    team_id,
    hierarchy_level,
    ARRAY_CONSTRUCT(
      lvl_1_team_id,
      lvl_2_team_id,
      lvl_3_team_id,
      lvl_4_team_id,
      lvl_5_team_id,
      lvl_6_team_id,
      lvl_7_team_id,
      lvl_8_team_id,
      lvl_9_team_id,
      lvl_10_team_id) AS team_ids,
  FROM prep_current_hierarchy
    
),

flattened AS (

  SELECT 
    team_id,
    hierarchy_level,
    seq.index + 1                 AS level,
    team_ids[seq.index]::VARCHAR  AS level_team_id
  FROM hierarchy_array,
    LATERAL FLATTEN(ARRAY_CONSTRUCT(0,1,2,3,4,5,6,7,8,9)) seq
  WHERE seq.index+1 <= hierarchy_level 

)

SELECT
  flattened.team_id,
  flattened.level,
  flattened.level_team_id,
  prep_current_hierarchy.team_code                   AS level_team_code,
  prep_current_hierarchy.team_manager_name           AS level_team_manager_name,
  prep_current_hierarchy.team_manager_employee_id    AS level_team_manager_employee_id,
  prep_current_hierarchy.team_name                   AS level_team_name,
  prep_current_hierarchy.is_team_active              AS level_is_team_active
FROM flattened
LEFT JOIN prep_current_hierarchy
  ON flattened.level_team_id = prep_current_hierarchy.team_id
ORDER BY 
  flattened.hierarchy_level,
  flattened.team_id,
  flattened.level

