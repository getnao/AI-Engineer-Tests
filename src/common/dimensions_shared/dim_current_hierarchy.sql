WITH source AS (

  SELECT *
  FROM {{ ref('prep_current_hierarchy') }}

),
final AS (
    SELECT
        -- Primary key
        team_id AS team_dim_id,
        
        -- Team attributes
        team_code,
        team_name,
        team_manager_employee_id,
        team_manager_name,
        is_team_active,
        hierarchy_level,
        
        -- Hierarchy information
        {% for level in range(1,11) %}
        lvl_{{ level }}_team_id,
        lvl_{{ level }}_team_code,
        lvl_{{ level }}_team_name,
        lvl_{{ level }}_team_manager_employee_id,
        lvl_{{ level }}_team_manager_name
        {% if not loop.last %},{% endif %}
        {% endfor %}

    FROM source
)

SELECT * FROM final
