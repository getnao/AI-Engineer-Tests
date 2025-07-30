WITH prep_team_member_org_history AS (

  SELECT
    -- Primary keys
    team_member_org_pk,
    -- Surrogate Keys
    dim_team_member_sk,

    -- Team member org history attributes
    employee_id,
    hire_rank,
    valid_from,
    valid_to,
    org_combination_id,
    department_id,
    department_id_type,
    department,
    division,
    cost_center,
    entity,
    employee_type
  FROM {{ ref('prep_team_member_org_history') }}

)

SELECT *
FROM prep_team_member_org_history

