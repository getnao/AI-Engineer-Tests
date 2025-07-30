{{ config(
    materialized="incremental",
    unique_key="dim_ci_build_id",
    snowflake_warehouse=generate_warehouse_name('XL')

) }}

{{ simple_cte([
   ('dim_namespace', 'dim_namespace'),
   ('dim_project', 'dim_project'),
   ('dim_date', 'dim_date'),
   ('dim_ci_build', 'dim_ci_build'),
   ('dim_ci_pipeline', 'dim_ci_pipeline')
]) }}

, ci_runner_activity AS (

    SELECT 
      fct_ci_runner_activity.*,
      dim_date.date_day          AS ci_build_created_date,
    FROM {{ ref('fct_ci_runner_activity') }}
    LEFT JOIN dim_date
      ON fct_ci_runner_activity.ci_build_created_date_id = dim_date.date_id
    {% if is_incremental() %}

    WHERE ci_build_created_date > (SELECT MAX(ci_build_created_date) FROM {{this}})

    {% endif %}

), joined AS (

   SELECT

    -- PRIMARY KEY
      ci_runner_activity.dim_ci_build_id,

     -- FOREIGN KEYS
     dim_project.dim_project_id,
     dim_namespace.dim_namespace_id,
     dim_namespace.ultimate_parent_namespace_id,
     dim_namespace.gitlab_plan_id                                      AS dim_plan_id,
     ci_runner_activity.dim_ci_runner_id,
     ci_runner_activity.dim_ci_pipeline_id,
     ci_runner_activity.dim_ci_stage_id,
     ci_runner_activity.dim_user_id,

     -- DATES
     ci_runner_activity.ci_build_created_date,
     dim_date.date_day                                                 AS ci_build_start_date,
     ci_runner_activity.ci_build_created_date_id,
     ci_runner_activity.ci_build_started_at,
     ci_runner_activity.ci_build_finished_at,
     dim_ci_pipeline.created_at                                        AS ci_pipeline_created_at,

     -- CI RUNNER METRICS
     ci_runner_activity.ci_build_duration_in_s,
     ci_runner_activity.public_projects_minutes_cost_factor,
     ci_runner_activity.private_projects_minutes_cost_factor,

     -- CI RUNNER ACTIVITY METADATA
     ci_runner_activity.is_paid_by_gitlab,
     dim_project.visibility_level                                      AS project_visibility_level,
     dim_project.project_path,
     dim_namespace.namespace_is_internal,
     dim_namespace.gitlab_plan_title                                   AS ultimate_parent_plan_title,
     IFF(scheduling_type = 1, TRUE, FALSE)                             AS is_dag_pipeline,
     CASE 
       WHEN dim_ci_pipeline.ref LIKE '%/merge' 
         AND dim_ci_pipeline.ci_pipeline_source = 'merge_request_event'
         THEN 'merged_results'
       WHEN dim_ci_pipeline.ref LIKE '%/train' 
         AND dim_ci_pipeline.ci_pipeline_source = 'merge_request_event'
         THEN 'merge_train'
       WHEN dim_ci_pipeline.ci_pipeline_source = 'merge_request_event'
         THEN 'detached_merge_request'
       ELSE dim_ci_pipeline.ci_pipeline_source 
     END                                                                AS ci_pipeline_source

   FROM ci_runner_activity
   LEFT JOIN dim_project
     ON ci_runner_activity.dim_project_id = dim_project.dim_project_id
   LEFT JOIN dim_namespace
     ON ci_runner_activity.dim_namespace_id = dim_namespace.dim_namespace_id
   LEFT JOIN dim_ci_build
     ON ci_runner_activity.dim_ci_build_id = dim_ci_build.dim_ci_build_id
   LEFT JOIN dim_ci_pipeline
     ON ci_runner_activity.dim_ci_pipeline_id = dim_ci_pipeline.dim_ci_pipeline_id
   LEFT JOIN dim_date
      ON TO_DATE(ci_runner_activity.ci_build_started_at) = dim_date.date_day

)

SELECT *
FROM joined