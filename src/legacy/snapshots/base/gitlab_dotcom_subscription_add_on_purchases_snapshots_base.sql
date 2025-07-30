{{ config({
    "alias": "gitlab_dotcom_subscription_add_on_purchases_snapshots"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'gitlab_dotcom_subscription_add_on_purchases_snapshots') }}
    
), renamed AS (

  SELECT
    id::NUMBER                                                      AS id,
    created_at::TIMESTAMP                                           AS created_at,
    updated_at::TIMESTAMP                                           AS updated_at,
    subscription_add_on_id::NUMBER                                  AS subscription_add_on_id,
    namespace_id::NUMBER                                            AS namespace_id,
    quantity::NUMBER                                                AS quantity,
    expires_on::TIMESTAMP                                           AS expires_on,
    purchase_xid::VARCHAR                                           AS purchase_xid,
    dbt_valid_from::TIMESTAMP                                       AS valid_from,
    dbt_valid_to::TIMESTAMP                                         AS valid_to
  FROM source
    
), final AS (

  SELECT 
    renamed.id,
    renamed.created_at,
    renamed.updated_at,
    renamed.subscription_add_on_id,
    renamed.namespace_id,
    renamed.quantity,
    renamed.expires_on,
    CASE
      -- Handle specific cases where upgrades occurred before snapshot creation
      WHEN renamed.purchase_xid IN ('A-S00017713','A-S00031556','A-S00043856','A-S00078868','A-S00096791','A-S00107013') THEN renamed.updated_at
      -- Fix first snapshot records where valid_from ≠ created_at (e.g., Namespace ID: 83893222)
      -- Ensures valid period starts from actual creation date
      WHEN ROW_NUMBER() OVER (PARTITION BY renamed.namespace_id ORDER BY renamed.updated_at ASC) = 1
      AND DATE(renamed.valid_from) != DATE(created_at) THEN renamed.created_at
      ELSE renamed.valid_from
    END                                                             AS corrected_valid_from_date,
    CASE
      -- For records with a valid_to timestamp (indicating a subscription renewal or upgrade/downgrade), use that date minus 1 day
      -- This creates exclusive end dates that don't overlap with the next subscription's start date
      WHEN renamed.valid_to IS NOT NULL THEN (renamed.valid_to::DATE - INTERVAL '1 day')
      -- For expired subscriptions without a valid_to (e.g., non-renewed subscriptions), use the expires_on date
      WHEN renamed.expires_on <= CURRENT_DATE() THEN renamed.expires_on::DATE
      -- For active subscriptions without a defined end date, use the current date
      ELSE CURRENT_DATE()
    END                                                             AS corrected_valid_to_date,
    renamed.purchase_xid,
    renamed.valid_from,
    renamed.valid_to
  FROM renamed

)

SELECT *
FROM final

