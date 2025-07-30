{{ simple_cte([
    ('sla_names', 'zendesk_fivetran_sla_policy_history_source'),
    ('sla_metrics', 'zendesk_fivetran_sla_policy_metric_history_source')
]) }},

recent_sla_metrics AS (

  SELECT
    sla_policy_id                         AS dim_support_sla_policy_id,
    metric_index,

    --fields
    metric_priority,
    metric_type,
    target_hours,
    uses_business_hours,
  FROM sla_metrics
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dim_support_sla_policy_id
    ORDER BY metric_index DESC
    ) = 1 

),

final AS (
  
  SELECT
    sla_names.sla_policy_id               AS dim_support_sla_policy_id,

    --fields
    sla_names.sla_policy_title,
    sla_names.sla_policy_description,

    --fields
    recent_sla_metrics.metric_priority,
    recent_sla_metrics.metric_type,
    recent_sla_metrics.target_hours,
    recent_sla_metrics.uses_business_hours
  FROM sla_names
  LEFT JOIN recent_sla_metrics
    ON sla_names.sla_policy_id = recent_sla_metrics.dim_support_sla_policy_id
)

SELECT *
FROM final