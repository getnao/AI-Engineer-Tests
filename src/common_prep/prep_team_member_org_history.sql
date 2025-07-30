{{ simple_cte([
    ('dates', 'dim_date'),
    ('employment_dates', 'fct_team_member_history'),
    ('staffing_history_approved_source', 'staffing_history_approved_source'),
    ('bamboohr_job_info', 'bamboohr_job_info'),
    ('bhr_job_role', 'bamboohr_job_role'),
    ('cost_center_prior_to_bamboo', 'cost_center_division_department_mapping'),
    ('cost_centers_historical', 'blended_cost_centers_source')
]) }},

department_mapping AS ( -- Department name mapping for BambooHR to Workday transition

  SELECT 'Meltano' AS old_department_name, 'Meltano Department' AS new_department_name
  UNION ALL
  SELECT 'Recruiting', 'Talent Acquisition'  
  UNION ALL
  SELECT 'People Ops', 'People Operations'
  
),

staffing_history AS ( -- Workday department changes

  SELECT 
    *,
    LAG(effective_date,1,'2099-01-01') OVER(PARTITION BY  employee_id ORDER BY effective_date DESC, date_time_initiated DESC)::DATE AS next_effective_date
  FROM staffing_history_approved_source

),

bhr_job_info as ( -- BambooHR department, division, and entity changes

  SELECT *
  FROM bamboohr_job_info
  WHERE effective_date < '2022-06-16' --Workday cutover date

),

bhr_wd_map as ( -- map Workday department ids to BambooHR data if department name matches

  SELECT *
  FROM cost_centers_historical
  QUALIFY ROW_NUMBER() OVER(PARTITION BY department ORDER BY valid_from DESC, valid_to DESC, cost_center DESC) = 1

), 

org_stage AS ( -- blend Workday and BambooHR data

  SELECT
    employment_dates.employee_id,
    dates.date_actual,
    employment_dates.hire_date,
    employment_dates.hire_rank_asc                                                                                   AS hire_rank,
    employment_dates.term_date,
    employment_dates.last_date,
    staffing_history.department_workday_id_current                                                                   AS department_workday_id,
    COALESCE(staffing_history.entity_current, bhr_job_info.entity)                                                   AS blended_entity,
    staffing_history.employee_type_current                                                                           AS employee_type,
    COALESCE(cost_centers_historical.department, bhr_job_info.department)                                            AS blended_department,
    COALESCE(department_mapping.new_department_name, blended_department)                                             AS lkup_department,
    COALESCE(cost_centers_historical.division, bhr_job_info.division)                                                AS blended_division,
    COALESCE(cost_centers_historical.cost_center, bhr_job_role.cost_center, cost_center_prior_to_bamboo.cost_center) AS blended_cost_center,
    {{ dbt_utils.generate_surrogate_key([
        'employment_dates.employee_id',
        'employment_dates.hire_rank_asc',
        'department_workday_id',
        'blended_entity',
        'employee_type',
        'blended_department',
        'blended_division',
        'blended_cost_center']) }}                                                                                   AS org_unique_key,
        LEAD(org_unique_key, 1, org_unique_key) OVER (PARTITION BY employment_dates.employee_id, employment_dates.hire_rank_asc ORDER BY dates.date_actual DESC )::TEXT AS pr_org_unique_key
  FROM dates
  INNER JOIN employment_dates -- show all employment dates for team members
    ON dates.date_actual >= employment_dates.hire_date
      AND dates.date_actual <= last_date
  LEFT JOIN  staffing_history -- Workday department changes on or after 2022-06-16
    ON dates.date_actual >= staffing_history.effective_date
      AND dates.date_actual < staffing_history.next_effective_date
      AND employment_dates.employee_id = staffing_history.employee_id
      AND dates.date_actual >= '2022-06-16'::DATE
  LEFT JOIN bhr_job_info -- BambooHR department,division,entity changes on or before 2022-06-15
    ON dates.date_actual >= bhr_job_info.effective_date
      AND dates.date_actual <= LEAST(COALESCE(effective_end_date,'2022-06-15'),'2022-06-15')
      AND employment_dates.employee_id = bhr_job_info.employee_id
  LEFT JOIN cost_centers_historical -- map historical department, division, and cost center based on department workday id
    ON staffing_history.department_workday_id_current = cost_centers_historical.dept_workday_id
      AND dates.date_actual >= cost_centers_historical.valid_from
      AND dates.date_actual < cost_centers_historical.valid_to   
  LEFT JOIN bhr_job_role -- BambooHR cost center on or before 2022-06-15
    ON dates.date_actual >= bhr_job_role.effective_date  
      AND dates.date_actual <= COALESCE(bhr_job_role.next_effective_date,'2022-06-16')
      AND employment_dates.employee_id = bhr_job_role.employee_id
      AND dates.date_actual < '2022-06-16'
  LEFT JOIN cost_center_prior_to_bamboo -- cost center on or before 2022-06-15 if missing in BambooHR
    ON bhr_job_info.department = cost_center_prior_to_bamboo.department
      AND bhr_job_info.division = cost_center_prior_to_bamboo.division
      AND dates.date_actual BETWEEN cost_center_prior_to_bamboo.effective_start_date
      AND COALESCE(cost_center_prior_to_bamboo.effective_end_date, '2020-05-07')
  LEFT JOIN department_mapping -- apply department name mapping to current Workday department to get IDs
    ON bhr_job_info.department = department_mapping.old_department_name        
  QUALIFY pr_org_unique_key != org_unique_key -- only show rows that changed from prior day or hire date
    OR employment_dates.hire_date = dates.date_actual 
    
), 

final AS (

  SELECT
   -- primary keys
    {{ dbt_utils.generate_surrogate_key(['employee_id', 'hire_rank', 'date_actual']) }}                  AS team_member_org_pk,

    -- foreign keys
    {{ get_keyed_nulls(dbt_utils.generate_surrogate_key(['employee_id'])) }}                             AS dim_team_member_sk,
    
    -- org history attributes
    employee_id,
    hire_rank,
    date_actual                                                                                          AS valid_from,
    LAG(date_actual-1,1,last_date) OVER(PARTITION BY employee_id, hire_rank ORDER BY date_actual DESC)   AS valid_to,
    org_unique_key                                                                                       AS org_combination_id,
    CASE
      WHEN date_actual < '2022-06-16'
        THEN COALESCE(bhr_wd_map.dept_workday_id,
        {{ dbt_utils.generate_surrogate_key(['blended_department']) }}
        )
    ELSE org_stage.department_workday_id
    END                                                                                                  AS department_id,
    CASE
      WHEN COALESCE(org_stage.department_workday_id, bhr_wd_map.dept_workday_id) IS NOT NULL
        THEN 'workday'
    ELSE 'legacy'
    END                                                                                                  AS department_id_type,
    blended_department                                                                                   AS department,
    blended_division                                                                                     AS division,
    blended_cost_center                                                                                  AS cost_center,
    blended_entity                                                                                       AS entity,
    employee_type
  FROM org_stage
  LEFT JOIN bhr_wd_map on lkup_department = bhr_wd_map.department -- maps old BambooHR department names to current Workday department IDs
    AND date_actual < '2022-06-16'
  ORDER BY employee_id, hire_rank DESC, valid_from DESC

)

SELECT *
FROM final

