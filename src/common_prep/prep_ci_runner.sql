{{ config(
    tags=["product"],
    snowflake_warehouse=generate_warehouse_name('XL')
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_ci_runner_sk",
    "on_schema_change": "sync_all_columns"
    })
}}

{{ simple_cte([
    ('prep_date', 'prep_date'),
    ('sheetload_ci_runner_machine_type_mapping_source', 'sheetload_ci_runner_machine_type_mapping_source'),
    ('gitlab_dotcom_ci_runner_machines_source', 'gitlab_dotcom_ci_runner_machines_source')

]) }},

gitlab_dotcom_ci_runners_source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_runners_source') }}
  {% if is_incremental() %}

    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})

  {% endif %}

),

latest_ci_runner_machine AS (

  SELECT *
  FROM gitlab_dotcom_ci_runner_machines_source
  QUALIFY ROW_NUMBER() OVER (PARTITION BY runner_id ORDER BY updated_at DESC) = 1

),

final AS (

  SELECT

    -- SURROGATE KEY
    {{ dbt_utils.generate_surrogate_key(['gitlab_dotcom_ci_runners_source.runner_id']) }}        AS dim_ci_runner_sk,

    --NATURAL KEY
    gitlab_dotcom_ci_runners_source.runner_id                                                    AS ci_runner_id,

    --LEGACY NATURAL KEY
    gitlab_dotcom_ci_runners_source.runner_id                                                    AS dim_ci_runner_id,

    -- FOREIGN KEYS
    prep_date.date_id                                                                            AS created_date_id,

    gitlab_dotcom_ci_runners_source.created_at,
    gitlab_dotcom_ci_runners_source.updated_at,
    gitlab_dotcom_ci_runners_source.description                                                  AS ci_runner_description,
    gitlab_dotcom_ci_runners_source.contacted_at,
    gitlab_dotcom_ci_runners_source.is_active,
    gitlab_dotcom_ci_runners_source.version                                                      AS ci_runner_version,
    gitlab_dotcom_ci_runners_source.revision,
    gitlab_dotcom_ci_runners_source.platform,
    gitlab_dotcom_ci_runners_source.is_untagged,
    gitlab_dotcom_ci_runners_source.is_locked,
    gitlab_dotcom_ci_runners_source.access_level,
    gitlab_dotcom_ci_runners_source.maximum_timeout,
    gitlab_dotcom_ci_runners_source.runner_type                                                  AS ci_runner_type,
    gitlab_dotcom_ci_runners_source.public_projects_minutes_cost_factor,
    gitlab_dotcom_ci_runners_source.private_projects_minutes_cost_factor,
    COALESCE(sheetload_ci_runner_machine_type_mapping_source.ci_runner_machine_type, 'Other')    AS ci_runner_machine_type,
    COALESCE(sheetload_ci_runner_machine_type_mapping_source.cost_factor, 0)                     AS cost_factor,
    CASE gitlab_dotcom_ci_runners_source.runner_type
      WHEN 1 
        THEN 'shared'
      WHEN 2 
        THEN 'group-runner-hosted runners'
      WHEN 3 
        THEN 'project-runner-hosted runners'
    END                                                                                          AS ci_runner_type_summary,
    CASE
      --- Private Runners
      WHEN gitlab_dotcom_ci_runners_source.description ILIKE '%private%manager%'
        THEN 'private-runner-mgr'
      --- Linux Runners
      WHEN gitlab_dotcom_ci_runners_source.description ILIKE 'shared-runners-manager%'
        THEN 'linux-runner-mgr'
      WHEN gitlab_dotcom_ci_runners_source.description ILIKE '%.shared.runners-manager.%'
        THEN 'linux-runner-mgr'
      WHEN gitlab_dotcom_ci_runners_source.description ILIKE '%saas-linux-%-amd64%'
        AND gitlab_dotcom_ci_runners_source.description NOT ILIKE '%shell%'
        THEN 'linux-runner-mgr'
      --- Internal GitLab Runners
      WHEN gitlab_dotcom_ci_runners_source.description ILIKE 'gitlab-shared-runners-manager%'
        THEN 'gitlab-internal-runner-mgr'
      --- Window Runners
      WHEN gitlab_dotcom_ci_runners_source.description ILIKE 'windows-shared-runners-manager%'
        THEN 'windows-runner-mgr'
      --- Shared Runners
      WHEN gitlab_dotcom_ci_runners_source.description ILIKE '%.shared-gitlab-org.runners-manager.%'
        THEN 'shared-gitlab-org-runner-mgr'
      --- macOS Runners
      WHEN LOWER(gitlab_dotcom_ci_runners_source.description) ILIKE '%macos%'
        THEN 'macos-runner-mgr'
      --- Other
      ELSE 'Other'
    END                                                                                         AS ci_runner_manager,
    CASE latest_ci_runner_machine.executor_type
      WHEN 0 
        THEN '<unknown>'
      WHEN 1 
        THEN 'custom'
      WHEN 2 
        THEN 'shell'
      WHEN 3 
        THEN 'docker'
      WHEN 4 
        THEN 'docker+windows'
      WHEN 5 
        THEN 'docker+ssh'
      WHEN 6 
        THEN 'ssh'
      WHEN 7 
        THEN 'parallels'
      WHEN 8 
        THEN 'virtualbox'
      WHEN 9 
        THEN 'docker+machine'
      WHEN 10 
        THEN 'docker+ssh+machine'
      WHEN 11 
        THEN 'kubernetes'
      WHEN 12 
        THEN 'docker+autoscaler'
      WHEN 13 
        THEN 'instance'
      ELSE to_char(executor_type, '999')
    END                                                                                         AS executor_type


  FROM gitlab_dotcom_ci_runners_source
  LEFT JOIN prep_date
    ON TO_DATE(gitlab_dotcom_ci_runners_source.created_at) = prep_date.date_day
  LEFT JOIN sheetload_ci_runner_machine_type_mapping_source
    ON gitlab_dotcom_ci_runners_source.description LIKE sheetload_ci_runner_machine_type_mapping_source.ci_runner_description_mapping
  LEFT JOIN latest_ci_runner_machine
    ON gitlab_dotcom_ci_runners_source.runner_id = latest_ci_runner_machine.runner_id
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@michellecooper",
    created_date="2021-06-23",
    updated_date="2025-04-04"
) }}
