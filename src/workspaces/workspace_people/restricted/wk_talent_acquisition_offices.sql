WITH offices_source AS (

  SELECT *
  FROM {{ ref('greenhouse_offices_source') }}

),

jobs_source AS (

  SELECT *
  FROM {{ ref('greenhouse_jobs_source') }}

),

job_offices_source AS (

  SELECT *
  FROM {{ ref('greenhouse_jobs_offices_source') }}

),

job_offices_filtered AS (
  --higher levels of office hierarchy are automatically assigned and can show same office_id multiple times for job_id
  SELECT *
  FROM job_offices_source
  QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id, office_id ORDER BY job_office_updated_at DESC, job_office_id ASC) = 1

),

-- Recursive CTE to build the hierarchy
hierarchy_path AS (
  -- Base case: start with top-level offices
  SELECT
    office_id,
    organization_id,
    office_parent_id,
    office_name,
    office_created_at,
    office_updated_at,
    1                            AS office_hierarchy_level,
    ARRAY_CONSTRUCT(office_id)   AS office_ids,
    ARRAY_CONSTRUCT(office_name) AS office_names,
    ARRAY_CONSTRUCT(1)           AS hierarchy_levels
  FROM offices_source
  WHERE office_parent_id IS NULL

  UNION ALL

  -- Recursive case: join child offices to their parents
  SELECT
    child.office_id,
    child.organization_id,
    child.office_parent_id,
    child.office_name,
    child.office_created_at,
    child.office_updated_at,
    parent.office_hierarchy_level + 1                                        AS office_hierarchy_level,
    ARRAY_APPEND(parent.office_ids, child.office_id)                         AS office_ids,
    ARRAY_APPEND(parent.office_names, child.office_name)                     AS office_names,
    ARRAY_APPEND(parent.hierarchy_levels, parent.office_hierarchy_level + 1) AS hierarchy_levels
  FROM offices_source AS child
  INNER JOIN hierarchy_path AS parent ON child.office_parent_id = parent.office_id
  WHERE parent.office_hierarchy_level <= 3 -- Limit to 3 levels
),

offices_hierarchy AS (

  SELECT
    office_id,
    office_name,
    office_hierarchy_level,
    office_parent_id,
    office_created_at,
    office_updated_at,

    {% for level in range(3) %}
      {% set level_num = level + 1 %}
      office_names[{{ level }}]::VARCHAR AS lvl_{{ level_num }}_office_name,
      office_ids[{{ level }}]::VARCHAR   AS lvl_{{ level_num }}_office_id,
      hierarchy_levels[{{ level }}]::INT AS lvl_{{ level_num }}_level
      {% if not loop.last %},{% endif %}
    {% endfor %}

  FROM hierarchy_path

),

final AS (

  SELECT
    jobs_source.job_id,
    offices_hierarchy.*,
    --Identify deepest level of office hierarchy to remove redundant higher level rows
    COUNT(DISTINCT lvl_3_level) OVER (PARTITION BY jobs_source.job_id, COALESCE(lvl_2_office_id, lvl_1_office_id)) > 0 AS has_level_3,
    COUNT(DISTINCT lvl_2_level) OVER (PARTITION BY jobs_source.job_id, lvl_1_office_id) > 0                            AS has_level_2,
    CASE
      WHEN has_level_3 > 0 THEN 3
      WHEN has_level_2 > 0 THEN 2
      ELSE 1
    END                                                                                                                AS hierarchy_level_filter
  FROM jobs_source
  LEFT JOIN job_offices_filtered ON jobs_source.job_id = job_offices_filtered.job_id
  LEFT JOIN offices_hierarchy ON job_offices_filtered.office_id = offices_hierarchy.office_id
  QUALIFY COALESCE(office_hierarchy_level, 1) = hierarchy_level_filter

)

SELECT
  job_id,
  office_id,
  office_name,
  office_hierarchy_level,
  office_parent_id,
  office_created_at,
  office_updated_at,
  lvl_1_office_name,
  lvl_1_office_id,
  lvl_2_office_name,
  lvl_2_office_id,
  lvl_3_office_name,
  lvl_3_office_id
FROM final
