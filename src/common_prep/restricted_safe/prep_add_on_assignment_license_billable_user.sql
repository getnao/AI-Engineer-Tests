{{
  config(
    materialized='table'
  )
}}

{{ simple_cte([
    ('gitlab_dotcom_subscription_add_on_purchases_snapshots_base', 'gitlab_dotcom_subscription_add_on_purchases_snapshots_base'),
    ('gitlab_dotcom_subscription_user_add_on_assignment_versions', 'gitlab_dotcom_subscription_user_add_on_assignment_versions_source'),
    ('gitlab_dotcom_subscription_user_add_on_assignments', 'gitlab_dotcom_subscription_user_add_on_assignments_source'),
    ('gitlab_dotcom_memberships', 'gitlab_dotcom_memberships'),
    ('dim_date', 'dim_date'),
    ('prep_trial', 'prep_trial'),
    ('map_namespace_subscription_product', 'map_namespace_subscription_product'),
    ('prep_subscription', 'prep_subscription'),
    ('prep_product_detail', 'prep_product_detail')
])}},

namespace_subscription_mapping AS (

    SELECT 
      map_namespace_subscription_product.*,
      prep_subscription.subscription_name,
      prep_product_detail.dim_product_tier_id,
      prep_product_detail.assignable_feature_set
    FROM map_namespace_subscription_product
    LEFT JOIN prep_subscription
      ON map_namespace_subscription_product.dim_subscription_id = prep_subscription.dim_subscription_id
    LEFT JOIN prep_product_detail
      ON map_namespace_subscription_product.dim_product_detail_id = prep_product_detail.dim_product_detail_id

), 

dotcom_add_on_purchases AS (

  SELECT 
    gitlab_dotcom_subscription_add_on_purchases_snapshots_base.id,
    gitlab_dotcom_subscription_add_on_purchases_snapshots_base.namespace_id,
    gitlab_dotcom_subscription_add_on_purchases_snapshots_base.subscription_add_on_id,
    gitlab_dotcom_subscription_add_on_purchases_snapshots_base.quantity,
    gitlab_dotcom_subscription_add_on_purchases_snapshots_base.created_at,
    gitlab_dotcom_subscription_add_on_purchases_snapshots_base.purchase_xid,
    gitlab_dotcom_subscription_add_on_purchases_snapshots_base.corrected_valid_from_date,
    gitlab_dotcom_subscription_add_on_purchases_snapshots_base.corrected_valid_to_date,
    CASE 
      WHEN gitlab_dotcom_subscription_add_on_purchases_snapshots_base.subscription_add_on_id = 1000002
        THEN 'Duo Pro'
      WHEN gitlab_dotcom_subscription_add_on_purchases_snapshots_base.subscription_add_on_id = 1000035
        THEN 'Duo Enterprise'
      WHEN gitlab_dotcom_subscription_add_on_purchases_snapshots_base.subscription_add_on_id = 2000068
        THEN 'Duo Core'
    END                                                               AS assignable_feature_set,
    'GitLab.com'                                                      AS product_deployment_type
  FROM gitlab_dotcom_subscription_add_on_purchases_snapshots_base
  -- get the most recent record for each namespace/subscription combination.
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      gitlab_dotcom_subscription_add_on_purchases_snapshots_base.namespace_id,
      gitlab_dotcom_subscription_add_on_purchases_snapshots_base.subscription_add_on_id
    ORDER BY gitlab_dotcom_subscription_add_on_purchases_snapshots_base.valid_from DESC
  ) = 1

),

dotcom_add_on_deleted_assignments AS (

  SELECT
    item_id,
    user_id,
    created_at AS deleted_at
  FROM gitlab_dotcom_subscription_user_add_on_assignment_versions
  WHERE event = 'destroy'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY 
      item_id
    ORDER BY uploaded_at DESC
  ) = 1

),

dotcom_add_on_assignments AS (

  SELECT
    DISTINCT -- Unique assignments per namespace_id
    gitlab_dotcom_memberships.namespace_id,
    gitlab_dotcom_subscription_user_add_on_assignments.add_on_purchase_id,
    gitlab_dotcom_subscription_user_add_on_assignments.user_id,
    gitlab_dotcom_subscription_user_add_on_assignments.created_at,
    CASE
      WHEN dotcom_add_on_deleted_assignments.deleted_at IS NOT NULL 
        THEN dotcom_add_on_deleted_assignments.deleted_at::DATE
      WHEN gitlab_dotcom_subscription_user_add_on_assignments.pgp_is_deleted = TRUE 
        THEN NULL
      ELSE CURRENT_DATE()
    END                                                                                 AS effective_end_date
  FROM gitlab_dotcom_subscription_user_add_on_assignments
  INNER JOIN dotcom_add_on_purchases
    ON gitlab_dotcom_subscription_user_add_on_assignments.add_on_purchase_id = dotcom_add_on_purchases.id
     AND gitlab_dotcom_subscription_user_add_on_assignments.created_at::DATE BETWEEN LEAST(dotcom_add_on_purchases.corrected_valid_from_date::DATE, dotcom_add_on_purchases.created_at::DATE) AND dotcom_add_on_purchases.corrected_valid_to_date::DATE
  INNER JOIN gitlab_dotcom_memberships
    ON gitlab_dotcom_memberships.user_id = gitlab_dotcom_subscription_user_add_on_assignments.user_id
    AND gitlab_dotcom_memberships.namespace_id = dotcom_add_on_purchases.namespace_id
  -- Track deleted assignments, we can only track the exact deleted_at date for the records after 2024-11-08, 
  -- before that date the deleted assignments are simply exluded from the calculation
  -- Related issue: https://gitlab.com/gitlab-org/gitlab/-/issues/508335
  LEFT JOIN dotcom_add_on_deleted_assignments
    ON gitlab_dotcom_subscription_user_add_on_assignments.id = dotcom_add_on_deleted_assignments.item_id
    AND gitlab_dotcom_subscription_user_add_on_assignments.user_id = dotcom_add_on_deleted_assignments.user_id

),

dotcom_add_on_daily_purchases_assignments AS ( 

  -- CTE to calculate daily assigned seats per namespace ID
  -- Seat count updates daily to reflect new assignments and unassignments

  SELECT
    -- Use COALESCE to handle cases where assignments might be NULL
    COALESCE(dotcom_add_on_assignments.namespace_id, dotcom_add_on_purchases.namespace_id)                AS namespace_id,
    dotcom_add_on_purchases.purchase_xid,
    dim_date.date_day,
    dotcom_add_on_purchases.subscription_add_on_id,
    dotcom_add_on_purchases.product_deployment_type,
    dotcom_add_on_purchases.assignable_feature_set,
    -- Use MAX as a selector to pick the single quantity value for each unique combination of 
    -- namespace, date, and subscription add-on
    MAX(dotcom_add_on_purchases.quantity)                                                                 AS purchased_seats,
    -- If effective_end_date IS NULL (PaperTrial has no deletion record but the pgp_is_deleted is TRUE), 
    -- it doesn't count the assignments but still include those purchases
    -- For namespaces that have no assignments, the assigned seats should be 0 rather than NULL.
    -- For namespaces with assignments: Include dates within the assignment period
    COUNT(DISTINCT 
            IFF(
                dotcom_add_on_assignments.effective_end_date IS NOT NULL
                -- Only count from when the assignment was created till the end date
                  AND dim_date.date_actual >= DATE(dotcom_add_on_assignments.created_at)
                  AND dim_date.date_actual <= dotcom_add_on_assignments.effective_end_date,
                dotcom_add_on_assignments.user_id,
                NULL))                                                                                    AS assigned_seats
  FROM dotcom_add_on_purchases
  -- LEFT OUTER JOIN to include namespaces that have no assignments
  LEFT OUTER JOIN dotcom_add_on_assignments
    ON dotcom_add_on_assignments.namespace_id = dotcom_add_on_purchases.namespace_id
      AND dotcom_add_on_assignments.add_on_purchase_id = dotcom_add_on_purchases.id
  CROSS JOIN dim_date
  WHERE 
    -- Main date range: from purchase date to expiration date (or current date if no expiration)
    dim_date.date_actual BETWEEN DATE(dotcom_add_on_purchases.corrected_valid_from_date::DATE) 
                             AND COALESCE(dotcom_add_on_purchases.corrected_valid_to_date::DATE, CURRENT_DATE())
  GROUP BY ALL

),

dotcom_assignment_base AS (

  SELECT
    -- Compared to the FulfillmentProvisionGitLab_comDuoProSeatAdoption Dashboard this logic contains upgrades/downgrades from Duo Pro and has all the inactive susbcriptions (records which have an expires_on > CURRENT_DATE())
    dotcom_add_on_daily_purchases_assignments.date_day::DATE                                                    AS report_date,
    dotcom_add_on_daily_purchases_assignments.namespace_id::INT                                                 AS dim_namespace_id,
    namespace_subscription_mapping.dim_subscription_id,
    prep_trial.trial_pk,
    namespace_subscription_mapping.dim_product_tier_id,
    dotcom_add_on_daily_purchases_assignments.product_deployment_type,
    dotcom_add_on_daily_purchases_assignments.assignable_feature_set,
    -- MAX is used here to select the non-NULL value for each product type (Duo Pro or Duo Enterprise) within each namespace and date group.
    MAX(dotcom_add_on_daily_purchases_assignments.purchased_seats)                                              AS license_users,
    MAX(dotcom_add_on_daily_purchases_assignments.assigned_seats)                                               AS billable_users
  FROM dotcom_add_on_daily_purchases_assignments
  -- Exclude assignment records where subscription_add_on_id is null because either the associated purchase got filtered in the dotcom_add_on_purchases CTE due to the purchase_xid criteria but dotcom_add_on_assignments has data because the ID for the other purchase was same 
  -- OR the subscription was upgraded, but no new assignment record was created after the upgrade date. This prevents attributing old assignments to a new subscription type.
  LEFT JOIN namespace_subscription_mapping
    ON dotcom_add_on_daily_purchases_assignments.purchase_xid = namespace_subscription_mapping.subscription_name
      AND dotcom_add_on_daily_purchases_assignments.date_day = namespace_subscription_mapping.date_actual
        AND dotcom_add_on_daily_purchases_assignments.assignable_feature_set = namespace_subscription_mapping.assignable_feature_set
  LEFT JOIN prep_trial
    ON SPLIT_PART(dotcom_add_on_daily_purchases_assignments.purchase_xid, '-', -1)::VARCHAR = prep_trial.internal_order_id::VARCHAR
      AND dotcom_add_on_daily_purchases_assignments.assignable_feature_set = prep_trial.trial_type_name
  WHERE subscription_add_on_id IS NOT NULL
  GROUP BY ALL

)

SELECT *
FROM dotcom_assignment_base