{{ simple_cte([
    ('prep_charge_mrr_daily', 'prep_charge_mrr_daily_with_namespace_and_installation'),
    ('prep_product_detail', 'prep_product_detail'),
    ('prep_ping_instance', 'prep_ping_instance'),
    ('prep_subscription', 'prep_subscription'),
    ('customers_db_license_seat_links_source', 'customers_db_license_seat_links_source'),
    ('prep_order', 'prep_order'),
    ('prep_host', 'prep_host'), 
    ('prep_add_on_assignment_license_billable_user', 'prep_add_on_assignment_license_billable_user'),
    ('gitlab_dotcom_gitlab_subscriptions', 'gitlab_dotcom_gitlab_subscriptions'),
    ('map_namespace_subscription_product', 'map_namespace_subscription_product'),
    ('prep_date', 'prep_date')

])}},

zuora_base AS (

-- Only licence users are avaiable from zuora

  SELECT
    prep_charge_mrr_daily.date_actual::DATE                                                                                                            AS report_date,
    prep_charge_mrr_daily.dim_subscription_id,
    prep_subscription.dim_subscription_id_original,
    prep_charge_mrr_daily.dim_namespace_id::INT                                                                                                        AS dim_namespace_id,
    prep_charge_mrr_daily.dim_installation_id::VARCHAR                                                                                                 AS dim_installation_id,
    prep_product_detail.product_deployment_type,
    prep_product_detail.dim_product_tier_id,
    prep_product_detail.assignable_feature_set,
    SUM(prep_charge_mrr_daily.quantity)                                                                                                                AS license_users
  FROM prep_charge_mrr_daily
  INNER JOIN prep_product_detail
    ON prep_charge_mrr_daily.dim_product_detail_id = prep_product_detail.dim_product_detail_id
  LEFT JOIN prep_subscription
    ON prep_charge_mrr_daily.dim_subscription_id = prep_subscription.dim_subscription_id
  WHERE prep_product_detail.is_licensed_user = TRUE
    AND (
        prep_charge_mrr_daily.dim_namespace_id IS NOT NULL 
          OR prep_charge_mrr_daily.dim_installation_id IS NOT NULL
        )
  GROUP BY ALL

), 

seat_link AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['prep_host.dim_host_id', 'customers_db_license_seat_links_source.uuid'])}}      AS dim_installation_id,
      customers_db_license_seat_links_source.zuora_subscription_id                                                         AS dim_subscription_id,
      customers_db_license_seat_links_source.zuora_subscription_name                                                       AS subscription_name,
      prep_subscription.dim_subscription_id_original,
      customers_db_license_seat_links_source.hostname                                                                      AS host_name,
      prep_host.dim_host_id,
      customers_db_license_seat_links_source.uuid                                                                          AS dim_instance_id,
      {{ get_keyed_nulls('prep_product_detail.dim_product_tier_id') }}                                                     AS dim_product_tier_id,
      prep_product_detail.product_delivery_type,
      prep_product_detail.product_deployment_type,
      prep_product_detail.assignable_feature_set,
      customers_db_license_seat_links_source.report_date,
      customers_db_license_seat_links_source.created_at,
      customers_db_license_seat_links_source.updated_at,
      customers_db_license_seat_links_source.active_user_count,
      customers_db_license_seat_links_source.license_user_count,
      customers_db_license_seat_links_source.max_historical_user_count,
      customers_db_license_seat_links_source.add_on_metrics_user_count
    FROM customers_db_license_seat_links_source
    INNER JOIN prep_order
      ON customers_db_license_seat_links_source.order_id = prep_order.internal_order_id
    LEFT JOIN prep_host
      ON customers_db_license_seat_links_source.hostname = prep_host.host_name
    LEFT OUTER JOIN prep_product_detail
      ON prep_order.product_rate_plan_id = prep_product_detail.product_rate_plan_id
    LEFT JOIN prep_subscription
      ON customers_db_license_seat_links_source.zuora_subscription_id = prep_subscription.dim_subscription_id
    WHERE prep_host.dim_host_id IS NOT NULL AND customers_db_license_seat_links_source.uuid IS NOT NULL

), 

add_on_records AS (

  SELECT
    seat_link.dim_installation_id,
    seat_link.dim_subscription_id,
    seat_link.dim_subscription_id_original,
    seat_link.subscription_name,
    seat_link.host_name,
    seat_link.dim_host_id,
    seat_link.dim_instance_id,
    seat_link.dim_product_tier_id,
    seat_link.product_delivery_type,
    seat_link.product_deployment_type,
    seat_link.report_date,
    seat_link.created_at,
    seat_link.updated_at,
    CASE 
      WHEN TRIM(unnest.value['add_on_type'], '"') = 'code_suggestions'
        THEN 'Duo Pro'
      WHEN TRIM(unnest.value['add_on_type'], '"') = 'duo_enterprise'
        THEN 'Duo Enterprise'
       WHEN TRIM(unnest.value['add_on_type'], '"') = 'duo_amazon_q'
        THEN 'Duo with Amazon Q'
      ELSE TRIM(unnest.value['add_on_type'], '"')
    END                                                                 AS assignable_feature_set,
    unnest.value['assigned_seats']                                      AS active_user_count,
    unnest.value['purchased_seats']                                     AS license_user_count,
    NULL                                                                AS max_historical_user_count
  FROM seat_link
  LEFT JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(seat_link.add_on_metrics_user_count), OUTER => FALSE) AS unnest

), 

base_tier_records AS (

  SELECT
    dim_installation_id,
    dim_subscription_id,
    dim_subscription_id_original,
    subscription_name,
    host_name,
    dim_host_id,
    dim_instance_id,
    dim_product_tier_id,
    product_delivery_type,
    product_deployment_type,
    report_date,
    created_at,
    updated_at,
    'GitLab'                AS assignable_feature_set,
    active_user_count,
    license_user_count,
    max_historical_user_count
  FROM seat_link

), 

unioned_seat_link AS (

  SELECT *
  FROM base_tier_records
  UNION ALL
  SELECT *
  FROM add_on_records

), 

seat_link_base AS (

  SELECT
    report_date,
    dim_installation_id,
    dim_subscription_id,
    dim_subscription_id_original,
    assignable_feature_set,
    dim_product_tier_id,
    product_deployment_type,
    license_user_count        AS license_users,
    active_user_count         AS billable_users
  FROM unioned_seat_link
  QUALIFY ROW_NUMBER() OVER (PARTITION BY report_date, dim_installation_id, assignable_feature_set ORDER BY updated_at DESC, active_user_count DESC) = 1


),

most_recent_installation_ping AS (

  SELECT 
    prep_ping_instance.*,
    prep_ping_instance.ping_deployment_type AS product_deployment_type,
    prep_subscription.dim_subscription_id_original
  FROM prep_ping_instance
  LEFT JOIN prep_subscription
    ON prep_ping_instance.dim_subscription_id = prep_subscription.dim_subscription_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      ping_created_at::DATE,
      dim_installation_id
    ORDER BY ping_created_at DESC
  ) = 1

),

service_ping_base AS (

  SELECT
    ping_created_at::DATE                    AS report_date,
    dim_subscription_id,
    dim_subscription_id_original,
    dim_installation_id,
    dim_product_tier_id,
    product_deployment_type,
    'Duo Enterprise'                         AS assignable_feature_set,
    duo_enterprise_purchased_seats           AS license_users,
    duo_enterprise_assigned_seats            AS billable_users
  FROM most_recent_installation_ping
  WHERE dim_installation_id IS NOT NULL
    AND (
      duo_enterprise_purchased_seats IS NOT NULL
      OR duo_enterprise_assigned_seats IS NOT NULL
    )

  UNION ALL

  SELECT
    ping_created_at::DATE                    AS report_date,
    dim_subscription_id,
    dim_subscription_id_original,
    dim_installation_id,
    dim_product_tier_id,
    product_deployment_type,
    'Duo Pro'                                AS assignable_feature_set,
    duo_pro_purchased_seats                  AS license_users,
    duo_pro_assigned_seats                   AS billable_users
  FROM most_recent_installation_ping
  WHERE dim_installation_id IS NOT NULL
    AND (
      duo_pro_purchased_seats IS NOT NULL
      OR duo_pro_assigned_seats IS NOT NULL
    )

  UNION ALL

  SELECT
    ping_created_at::DATE                    AS report_date,
    dim_subscription_id,
    dim_subscription_id_original,
    dim_installation_id,
    dim_product_tier_id,
    product_deployment_type,
    'Duo with Amazon Q'                      AS assignable_feature_set,
    duo_pro_purchased_seats                  AS license_users,
    duo_pro_assigned_seats                   AS billable_users
  FROM most_recent_installation_ping
  WHERE dim_installation_id IS NOT NULL
    AND (
      duo_amazon_q_purchased_seats IS NOT NULL
      OR duo_amazon_q_assigned_seats IS NOT NULL
    )

  UNION ALL

  SELECT
    ping_created_at::DATE                    AS report_date,
    dim_subscription_id,
    dim_subscription_id_original,
    dim_installation_id,
    dim_product_tier_id,
    product_deployment_type,
    'GitLab'                                 AS assignable_feature_set,
    license_user_count                       AS license_users,
    license_billable_users                   AS billable_users
  FROM most_recent_installation_ping
  WHERE dim_installation_id IS NOT NULL
    AND (
      license_billable_users IS NOT NULL
      OR license_user_count IS NOT NULL
    )

),

map_subscription_namespace_product_gitlab AS (

  SELECT DISTINCT
    map_namespace_subscription_product.date_actual,
    map_namespace_subscription_product.dim_namespace_id,
    map_namespace_subscription_product.dim_subscription_id,
    map_namespace_subscription_product.dim_subscription_id_original,
    prep_product_detail.assignable_feature_set,
    prep_product_detail.dim_product_tier_id,
    prep_product_detail.product_deployment_type
  FROM map_namespace_subscription_product
  LEFT JOIN prep_product_detail
    ON map_namespace_subscription_product.dim_product_detail_id = prep_product_detail.dim_product_detail_id
  WHERE prep_product_detail.product_category = 'Base Products'

),

saas_subscriptions AS (

  SELECT
    prep_date.date_actual                                                   AS report_date,
    map_subscription_namespace_product_gitlab.dim_subscription_id,
    map_subscription_namespace_product_gitlab.dim_subscription_id_original,
    gitlab_dotcom_gitlab_subscriptions.namespace_id                         AS dim_namespace_id,
    map_subscription_namespace_product_gitlab.dim_product_tier_id,
    map_subscription_namespace_product_gitlab.product_deployment_type,
    map_subscription_namespace_product_gitlab.assignable_feature_set,
    gitlab_dotcom_gitlab_subscriptions.seats                                AS license_users,
    gitlab_dotcom_gitlab_subscriptions.seats_in_use                         AS billable_users
  FROM prod.legacy.gitlab_dotcom_gitlab_subscriptions
  INNER JOIN prod.common_prep.prep_date
    ON prep_date.date_actual >= DATE(gitlab_dotcom_gitlab_subscriptions.valid_from)
    AND (prep_date.date_actual < DATE(gitlab_dotcom_gitlab_subscriptions.valid_to) 
      OR gitlab_dotcom_gitlab_subscriptions.valid_to IS NULL)
    AND prep_date.date_actual < CURRENT_DATE()
  LEFT JOIN map_subscription_namespace_product_gitlab
    ON prep_date.date_actual = map_subscription_namespace_product_gitlab.date_actual
      AND gitlab_dotcom_gitlab_subscriptions.namespace_id = map_subscription_namespace_product_gitlab.dim_namespace_id
  WHERE map_subscription_namespace_product_gitlab.dim_subscription_id IS NOT NULL

),

joined AS (

  SELECT DISTINCT
    COALESCE(
      zuora_base.report_date,
      prep_add_on_assignment_license_billable_user.report_date,
      seat_link_base.report_date,
      service_ping_base.report_date
    )::DATE                                                                    AS report_date,

    COALESCE(
      zuora_base.dim_installation_id,
      seat_link_base.dim_installation_id,
      service_ping_base.dim_installation_id
    )::VARCHAR                                                                 AS dim_installation_id,

    COALESCE(
      zuora_base.dim_namespace_id,
      prep_add_on_assignment_license_billable_user.dim_namespace_id,
      saas_subscriptions.dim_namespace_id
    )::INT                                                                     AS dim_namespace_id,

    COALESCE(
      zuora_base.dim_subscription_id,
      seat_link_base.dim_subscription_id,
      service_ping_base.dim_subscription_id,
      prep_add_on_assignment_license_billable_user.dim_subscription_id,
      saas_subscriptions.dim_subscription_id
    )::VARCHAR                                                                 AS dim_subscription_id,

    COALESCE(
      zuora_base.dim_product_tier_id,
      seat_link_base.dim_product_tier_id,
      service_ping_base.dim_product_tier_id,
      saas_subscriptions.dim_product_tier_id
    )::VARCHAR                                                                 AS dim_product_tier_id,

    -- License Users
    COALESCE(
      zuora_base.license_users,
      prep_add_on_assignment_license_billable_user.license_users,
      service_ping_base.license_users,
      seat_link_base.license_users,
      saas_subscriptions.license_users
    )::INT                                                                     AS license_users,

    -- Billable Users
    COALESCE(
      prep_add_on_assignment_license_billable_user.billable_users,
      service_ping_base.billable_users,
      seat_link_base.billable_users,
      saas_subscriptions.billable_users
    )::INT                                                                     AS billable_users,

    -- License Users Source
    CASE
      WHEN zuora_base.license_users IS NOT NULL 
        THEN 'zuora'
      WHEN prep_add_on_assignment_license_billable_user.license_users IS NOT NULL 
        THEN 'dotcom_assignment'
      WHEN service_ping_base.license_users IS NOT NULL 
        THEN 'service_ping'
      WHEN seat_link_base.license_users IS NOT NULL 
        THEN 'seat_link'
      WHEN saas_subscriptions.license_users IS NOT NULL
        THEN 'gitlab_dotcom_gitlab_subscriptions'
      ELSE 'unknown'
    END                                                                        AS license_users_source,

    -- Billable Users Source
    CASE
      WHEN prep_add_on_assignment_license_billable_user.billable_users IS NOT NULL 
        THEN 'dotcom_assignment'
      WHEN service_ping_base.billable_users IS NOT NULL 
        THEN 'service_ping'
      WHEN seat_link_base.billable_users IS NOT NULL 
        THEN 'seat_link'
      WHEN saas_subscriptions.billable_users IS NOT NULL
        THEN 'gitlab_dotcom_gitlab_subscriptions'
      ELSE 'unknown'
    END                                                                        AS billable_users_source,

    COALESCE(
      zuora_base.assignable_feature_set,
      prep_add_on_assignment_license_billable_user.assignable_feature_set,
      service_ping_base.assignable_feature_set,
      seat_link_base.assignable_feature_set,
      saas_subscriptions.assignable_feature_set
    )::VARCHAR                                                                 AS assignable_feature_set,

    COALESCE(
      zuora_base.product_deployment_type,
      prep_add_on_assignment_license_billable_user.product_deployment_type,
      service_ping_base.product_deployment_type,
      seat_link_base.product_deployment_type,
      saas_subscriptions.product_deployment_type
    )::VARCHAR                                                                 AS product_deployment_type

  FROM zuora_base
  FULL OUTER JOIN seat_link_base
    ON zuora_base.report_date = seat_link_base.report_date
      AND zuora_base.dim_installation_id = seat_link_base.dim_installation_id
        AND zuora_base.assignable_feature_set = seat_link_base.assignable_feature_set
          AND zuora_base.dim_subscription_id_original = seat_link_base.dim_subscription_id_original
  FULL OUTER JOIN service_ping_base
    ON COALESCE(zuora_base.report_date, seat_link_base.report_date) = service_ping_base.report_date
      AND COALESCE(zuora_base.dim_installation_id, seat_link_base.dim_installation_id) = service_ping_base.dim_installation_id
        AND COALESCE(zuora_base.assignable_feature_set, seat_link_base.assignable_feature_set) = service_ping_base.assignable_feature_set
          AND COALESCE(zuora_base.dim_subscription_id_original, seat_link_base.dim_subscription_id_original) = service_ping_base.dim_subscription_id_original
  FULL OUTER JOIN prep_add_on_assignment_license_billable_user
    ON zuora_base.report_date = prep_add_on_assignment_license_billable_user.report_date
      AND zuora_base.dim_namespace_id = prep_add_on_assignment_license_billable_user.dim_namespace_id
        AND zuora_base.assignable_feature_set = prep_add_on_assignment_license_billable_user.assignable_feature_set
  FULL OUTER JOIN saas_subscriptions
    ON zuora_base.report_date = saas_subscriptions.report_date
      AND zuora_base.dim_namespace_id = saas_subscriptions.dim_namespace_id
      AND zuora_base.dim_subscription_id = saas_subscriptions.dim_subscription_id

)

SELECT
  joined.report_date,
  joined.dim_installation_id,
  joined.dim_namespace_id,
  joined.dim_subscription_id,
  prep_subscription.dim_subscription_id_original,
  prep_subscription.dim_crm_account_id,
  joined.assignable_feature_set                                                         AS assignable_feature_set,
  joined.dim_product_tier_id,
  joined.product_deployment_type,
  joined.license_users_source,
  joined.billable_users_source,
  joined.license_users,
  joined.billable_users,
  DIV0(joined.billable_users, joined.license_users) AS utilization_rate,

FROM joined
LEFT JOIN prep_subscription
  ON joined.dim_subscription_id = prep_subscription.dim_subscription_id
WHERE report_date <= CURRENT_DATE
QUALIFY ROW_NUMBER() OVER (PARTITION BY joined.report_date, joined.dim_installation_id, joined.dim_namespace_id, joined.assignable_feature_set ORDER BY prep_subscription.is_trial_subscription ASC) = 1
