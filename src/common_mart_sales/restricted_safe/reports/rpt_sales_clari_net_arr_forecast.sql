{{ config(
    tags=["mnpi"]
    )
}}

{{ simple_cte([
    ('net_arr_entries', 'clari_net_arr_entries_source'),
    ('users', 'clari_users_source'),
    ('fields', 'clari_fields_source'),
    ('time_frames', 'clari_net_arr_time_frames_source'),
    ('new_logos', 'clari_new_logo_source')
]) }},

net_arr_forecast AS (

  SELECT
    net_arr_entries.forecast_id,
    users.user_full_name,
    users.user_email,
    users.crm_user_id,
    users.sales_team_role,
    users.parent_role,
    net_arr_entries.fiscal_quarter,
    fields.field_name,
    time_frames.week_number,
    time_frames.week_start_date,
    time_frames.week_end_date,
    fields.field_type,
    net_arr_entries.forecast_value,
    net_arr_entries.is_updated
  FROM net_arr_entries
  INNER JOIN users ON net_arr_entries.user_id = users.user_id
  INNER JOIN fields ON net_arr_entries.field_id = fields.field_id
  INNER JOIN time_frames ON net_arr_entries.time_frame_id = time_frames.time_frame_id
  -- multiple user_id's per crm_user_id, keep latest entry only
  -- could add `forecast_id` to QUALIFY, but better to alert if there are dups to fix underlying problem
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY net_arr_entries.fiscal_quarter, time_frames.week_start_date, users.crm_user_id, net_arr_entries.field_id
    ORDER BY net_arr_entries.uploaded_at DESC
  ) = 1

),

logo_forecast AS (

  SELECT
    new_logos.forecast_id,
    users.user_full_name,
    users.user_email,
    users.crm_user_id,
    users.sales_team_role,
    users.parent_role,
    new_logos.fiscal_quarter,
    IFF(fields.field_name = 'Base - New Logo Forecast', 'New Logo Forecast', fields.field_name),
    time_frames.week_number,
    time_frames.week_start_date,
    time_frames.week_end_date,
    fields.field_type,
    new_logos.forecast_value,
    new_logos.is_updated
  FROM new_logos
  INNER JOIN users ON new_logos.user_id = users.user_id
  INNER JOIN fields ON new_logos.field_id = fields.field_id
  INNER JOIN time_frames ON new_logos.time_frame_id = time_frames.time_frame_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY new_logos.fiscal_quarter, time_frames.week_start_date, users.crm_user_id, new_logos.field_id
    ORDER BY new_logos.uploaded_at DESC
  ) = 1

),

combined_forecast AS (

  SELECT * FROM net_arr_forecast

  UNION ALL

  SELECT * FROM logo_forecast

),

final AS (
  SELECT *
  FROM combined_forecast

  UNION
  -- Since the API isn't idempotent, using data from Driveload process
  SELECT
    forecast_id,
    user_full_name,
    user_email,
    crm_user_id,
    sales_team_role,
    parent_role,
    fiscal_quarter,
    field_name,
    week_number,
    week_start_date,
    week_end_date,
    field_type,
    forecast_value,
    is_updated
  FROM {{ source('clari_static', 'wk_sales_clari_net_arr_forecast_historical') }}
)

SELECT *
FROM final
