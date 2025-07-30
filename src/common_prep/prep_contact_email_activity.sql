{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{
    simple_cte([
        ('marketo_activity_send_email', 'marketo_activity_send_email_source'),
        ('marketo_activity_open_email', 'marketo_activity_open_email_source'),
        ('marketo_activity_click_email', 'marketo_activity_click_email_source'),
        ('marketo_activity_email_bounced', 'marketo_activity_email_bounced_source'),
        ('marketo_activity_email_bounced_soft', 'marketo_activity_email_bounced_soft_source'),
        ('marketo_activity_unsubscribe_email', 'marketo_activity_unsubscribe_email_source'),
        ('email_logs', 'email_logs'),
        ('level_up_email_activity_source', 'level_up_email_activity_source'),
        ('zuora_invoice_source', 'zuora_invoice_source'),
        ('zuora_contact_source', 'zuora_contact_source'),
        ('zendesk_tickets_source', 'zendesk_tickets_source'),
        ('zendesk_users_source', 'zendesk_users_source'),
        ('prep_marketo_person_source', 'prep_marketo_person'),
        ('marketo_lead_source', 'marketo_lead_source'),
        ('mailgun_events', 'mailgun_events')
    ])
}},

/*
MODEL PURPOSE: Unified email activity tracking across all GitLab email systems
GRAIN: One row per unique combination of (contact_email_address, activity_date, email_activity_type, source_system, email_activity_id)
DATA SOURCES: Marketo, Gainsight, Zuora, Zendesk, Levelup, Mailgun */ 

marketo_emails_base AS (

  SELECT
    'Marketo'                      AS source_system,
    'Send'                         AS email_activity_type,
    lead_id                        AS dim_marketo_person_id,
    marketo_activity_send_email_id AS email_activity_id,
    activity_date,
    campaign_id,
    primary_attribute_value        AS email_name,
    primary_attribute_value_id     AS marketo_email_id
  FROM marketo_activity_send_email
  WHERE email_name IS NOT NULL
    AND activity_date IS NOT NULL
),

marketo_activity_open_email_source AS (
  SELECT
    'Marketo'                      AS source_system,
    'Open'                         AS email_activity_type,
    lead_id                        AS dim_marketo_person_id,
    marketo_activity_open_email_id AS email_activity_id,
    activity_date,
    campaign_id,
    primary_attribute_value        AS email_name,
    primary_attribute_value_id     AS marketo_email_id
  FROM marketo_activity_open_email
  WHERE email_name IS NOT NULL
    AND activity_date IS NOT NULL
),

marketo_activity_click_email_source AS (
  SELECT
    'Marketo'                       AS source_system,
    'Click'                         AS email_activity_type,
    lead_id                         AS dim_marketo_person_id,
    marketo_activity_click_email_id AS email_activity_id,
    activity_date,
    campaign_id,
    primary_attribute_value         AS email_name,
    primary_attribute_value_id      AS marketo_email_id
  FROM marketo_activity_click_email
  WHERE email_name IS NOT NULL
    AND activity_date IS NOT NULL
),

marketo_activity_email_bounced_source AS (
  SELECT
    'Marketo'                         AS source_system,
    'Bounce'                          AS email_activity_type,
    lead_id                           AS dim_marketo_person_id,
    marketo_activity_email_bounced_id AS email_activity_id,
    activity_date,
    campaign_id,
    primary_attribute_value           AS email_name,
    primary_attribute_value_id        AS marketo_email_id
  FROM marketo_activity_email_bounced
  WHERE email_name IS NOT NULL
    AND activity_date IS NOT NULL
),

marketo_activity_email_bounced_soft_source AS (
  SELECT
    'Marketo'                              AS source_system,
    'Soft Bounce'                          AS email_activity_type,
    lead_id                                AS dim_marketo_person_id,
    marketo_activity_email_bounced_soft_id AS email_activity_id,
    activity_date,
    campaign_id,
    primary_attribute_value                AS email_name,
    primary_attribute_value_id             AS marketo_email_id
  FROM marketo_activity_email_bounced_soft
  WHERE email_name IS NOT NULL
    AND activity_date IS NOT NULL
),

marketo_activity_unsubscribe_email_source AS (
  SELECT
    'Marketo'                             AS source_system,
    'Unsubscribe'                         AS email_activity_type,
    lead_id                               AS dim_marketo_person_id,
    marketo_activity_unsubscribe_email_id AS email_activity_id,
    activity_date,
    campaign_id,
    primary_attribute_value               AS email_name,
    primary_attribute_value_id            AS marketo_email_id
  FROM marketo_activity_unsubscribe_email
  WHERE email_name IS NOT NULL
    AND activity_date IS NOT NULL
)
,

gainsight_sends AS (
  SELECT
    'Gainsight'        AS source_system,
    'Send'             AS email_activity_type,
    id                 AS email_activity_id,
    person_id          AS gainsight_person_id,
    email_address      AS gainsight_email_address,
    triggered_on::DATE AS activity_date,
    batch_name,
    template_id,
    template_name
  FROM email_logs
  WHERE sent = 1
),

gainsight_opens AS (
  SELECT
    'Gainsight'     AS source_system,
    'Open'          AS email_activity_type,
    id              AS email_activity_id,
    person_id       AS gainsight_person_id,
    email_address   AS gainsight_email_address,
    opened_on::DATE AS activity_date,
    batch_name,
    template_id,
    template_name
  FROM email_logs
  WHERE opened = 1
),

gainsight_clicks AS (
  SELECT
    'Gainsight'      AS source_system,
    'Click'          AS email_activity_type,
    id               AS email_activity_id,
    person_id        AS gainsight_person_id,
    email_address    AS gainsight_email_address,
    clicked_on::DATE AS activity_date,
    batch_name,
    template_id,
    template_name
  FROM email_logs
  WHERE clicked = 1
),

gainsight_bounces AS (
  SELECT
    'Gainsight'          AS source_system,
    'Bounce'             AS email_activity_type,
    id                   AS email_activity_id,
    person_id            AS gainsight_person_id,
    email_address        AS gainsight_email_address,
    hard_bounce_on::DATE AS activity_date,
    batch_name,
    template_id,
    template_name
  FROM email_logs
  WHERE hard_bounced = 1
),

gainsight_soft_bounces AS (
  SELECT
    'Gainsight'          AS source_system,
    'Soft Bounce'        AS email_activity_type,
    id                   AS email_activity_id,
    person_id            AS gainsight_person_id,
    email_address        AS gainsight_email_address,
    soft_bounce_on::DATE AS activity_date,
    batch_name,
    template_id,
    template_name
  FROM email_logs
  WHERE soft_bounced = 1
),

gainsight_unsubscribes AS (
  SELECT
    'Gainsight'           AS source_system,
    'Unsubscribe'         AS email_activity_type,
    id                    AS email_activity_id,
    person_id             AS gainsight_person_id,
    email_address         AS gainsight_email_address,
    unsubscribed_on::DATE AS activity_date,
    batch_name,
    template_id,
    template_name
  FROM email_logs
  WHERE unsubscribed = 1
),

levelup_bounce AS (

  SELECT
    'Levelup'            AS source_system,
    'Bounce'             AS email_activity_type,
    email                AS levelup_email_address,
    timestamp_time::DATE AS activity_date
  FROM level_up_email_activity_source
  WHERE event = 'bounce'
--Levelup source captures the events: deferred, group_resubscribe, group_unsubscribe, delivered and bounce

),

levelup_unsubscribe AS (

  SELECT
    'Levelup'            AS source_system,
    'Unsubscribe'        AS email_activity_type,
    email                AS levelup_email_address,
    timestamp_time::DATE AS activity_date
  FROM level_up_email_activity_source
  WHERE event = 'group_unsubscribe'
--Levelup source captures the events: deferred, group_resubscribe, group_unsubscribe, delivered and bounce

),

levelup_emails AS (
  SELECT DISTINCT *
  FROM levelup_bounce

  UNION ALL

  SELECT DISTINCT *
  FROM levelup_unsubscribe
),

mailgun_unsubscribe AS (

    SELECT
    'Mailgun'            AS source_system,
    'Unsubscribe'        AS email_activity_type,
    CASE
        WHEN message_headers_to LIKE '%emailtosalesforc%' THEN recipient
        WHEN recipient LIKE '%emailtosalesforc%' THEN message_headers_to
        ELSE recipient
    END                 AS mailgun_email_address,
    timestamp_updated::DATE AS activity_date
  FROM mailgun_events
  WHERE event = 'unsubscribed'

),
mailgun_delivered AS (

    SELECT
    'Mailgun'            AS source_system,
    'Delivered'          AS email_activity_type,
    CASE
        WHEN message_headers_to LIKE '%emailtosalesforc%' THEN recipient
        WHEN recipient LIKE '%emailtosalesforc%' THEN message_headers_to
        ELSE recipient
        END             AS mailgun_email_address,
    timestamp_updated::DATE AS activity_date
  FROM mailgun_events
  WHERE event = 'delivered'

),
mailgun_failed AS (

    SELECT
    'Mailgun'            AS source_system,
    'Failed'             AS email_activity_type,
    CASE
        WHEN message_headers_to LIKE '%emailtosalesforc%' THEN recipient
        WHEN recipient LIKE '%emailtosalesforc%' THEN message_headers_to
        ELSE recipient
        END             AS mailgun_email_address,
    timestamp_updated::DATE AS activity_date
  FROM mailgun_events
  WHERE event = 'failed'

),

zuora_emails AS (

  SELECT
    'Zuora'                                         AS source_system,
    'Send'                                          AS email_activity_type,
    zuora_contact_source.contact_id                 AS zuora_person_id,
    zuora_contact_source.work_email                 AS zuora_work_email_address,
    zuora_invoice_source.invoice_id                 AS email_activity_id,
    zuora_invoice_source.last_email_sent_date::DATE AS activity_date
  FROM zuora_invoice_source
  LEFT JOIN zuora_contact_source
    ON zuora_invoice_source.account_id = zuora_contact_source.account_id
  WHERE zuora_contact_source.work_email IS NOT NULL 
  -- Only latest contact record per email to avoid duplicates from contact updates
  QUALIFY ROW_NUMBER () OVER (PARTITION BY zuora_contact_source.work_email ORDER BY zuora_contact_source.updated_date DESC) = 1

),

zendesk_emails AS (

  SELECT
    'Zendesk'                                      AS source_system,
    'Send'                                         AS email_activity_type,
    zendesk_tickets_source.requester_id            AS zendesk_person_id,
    zendesk_tickets_source.ticket_id               AS email_activity_id,
    zendesk_tickets_source.ticket_created_at::DATE AS activity_date,
    zendesk_tickets_source.ticket_subject          AS email_subject_line,
    user.email                                     AS from_email,
    requester.email                                AS zendesk_email_address
  FROM zendesk_tickets_source
  LEFT JOIN zendesk_users_source AS user
    ON zendesk_tickets_source.assignee_id = user.user_id
  LEFT JOIN zendesk_users_source AS requester
    ON zendesk_tickets_source.requester_id = requester.user_id
),

prep_marketo_person AS (
  SELECT DISTINCT
    prep_marketo_person_source.dim_marketo_person_id,
    marketo_lead_source.email AS marketo_email_address
  FROM prep_marketo_person_source
  LEFT JOIN marketo_lead_source
    ON prep_marketo_person_source.email_hash = marketo_lead_source.email_hash
),

marketo_emails AS (

  SELECT
    marketo_emails_base.*,
    prep_marketo_person.marketo_email_address
  FROM marketo_emails_base
  LEFT JOIN prep_marketo_person
    ON marketo_emails_base.dim_marketo_person_id = prep_marketo_person.dim_marketo_person_id

),

gainsight_emails AS (

  SELECT * FROM gainsight_sends
  UNION ALL
  SELECT * FROM gainsight_opens
  UNION ALL
  SELECT * FROM gainsight_clicks
  UNION ALL
  SELECT * FROM gainsight_bounces
  UNION ALL
  SELECT * FROM gainsight_soft_bounces
  UNION ALL
  SELECT * FROM gainsight_unsubscribes

),
mailgun_emails AS
(
    SELECT DISTINCT 
    * FROM mailgun_unsubscribe

    UNION ALL

    SELECT DISTINCT 
    * FROM mailgun_delivered

    UNION ALL

    SELECT DISTINCT
    * FROM mailgun_failed
),
-- All our CTEs up to marketo_emails, gainsight_emails, zuora_emails, zendesk_emails, levelup_emails and mailgun_emails are up until here 

combined_emails AS (
  -- Handling Marketo separately since it's the largest
  WITH marketo_base AS (
    SELECT DISTINCT
      source_system,
      email_activity_type,
      activity_date
    FROM marketo_emails
  ),

  -- Combining all other smaller sources below
  other_sources AS (
    SELECT DISTINCT
      source_system,
      email_activity_type,
      activity_date
    FROM (
      SELECT
        source_system,
        email_activity_type,
        activity_date
      FROM gainsight_emails

      UNION ALL

      SELECT
        source_system,
        email_activity_type,
        activity_date
      FROM zuora_emails

      UNION ALL

      SELECT
        source_system,
        email_activity_type,
        activity_date
      FROM zendesk_emails

      UNION ALL

      SELECT
        source_system,
        email_activity_type,
        activity_date
      FROM levelup_emails

      UNION ALL

      SELECT
        source_system,
        email_activity_type,
        activity_date
      FROM mailgun_emails
    )
  )

  SELECT DISTINCT * FROM marketo_base
  UNION ALL
  SELECT DISTINCT * FROM other_sources
),

final AS (
  SELECT
    combined_emails.source_system,
    combined_emails.email_activity_type,
    combined_emails.activity_date,
    COALESCE(marketo_emails.marketo_email_address, gainsight_emails.gainsight_email_address, zuora_emails.zuora_work_email_address, levelup_emails.levelup_email_address, zendesk_emails.zendesk_email_address,mailgun_emails.mailgun_email_address) AS contact_email_address,
    CASE 
          WHEN marketo_emails.email_activity_id  IS NOT NULL
            THEN marketo_emails.email_activity_id::VARCHAR
          WHEN gainsight_emails.email_activity_id IS NOT NULL
            THEN gainsight_emails.email_activity_id::VARCHAR
          WHEN zuora_emails.email_activity_id IS NOT NULL
            THEN zuora_emails.email_activity_id::VARCHAR
          WHEN zendesk_emails.email_activity_id IS NOT NULL
            THEN zendesk_emails.email_activity_id::VARCHAR
        END AS email_activity_id, 
      CASE 
          WHEN marketo_emails.dim_marketo_person_id  IS NOT NULL
            THEN marketo_emails.dim_marketo_person_id::VARCHAR
          WHEN gainsight_emails.gainsight_person_id IS NOT NULL
            THEN gainsight_emails.gainsight_person_id::VARCHAR
          WHEN zuora_emails.zuora_person_id IS NOT NULL
            THEN zuora_emails.zuora_person_id::VARCHAR
          WHEN zendesk_emails.zendesk_person_id IS NOT NULL
            THEN zendesk_emails.zendesk_person_id::VARCHAR
        END AS person_id,
        CASE 
          WHEN   marketo_emails.email_name  IS NOT NULL
            THEN   marketo_emails.email_name
          WHEN gainsight_emails.batch_name   IS NOT NULL
            THEN gainsight_emails.batch_name 
          WHEN zendesk_emails.email_subject_line  IS NOT NULL
            THEN zendesk_emails.email_subject_line 
        END AS email_subject_line
--***** The below fields have been removed for the time being to limit the number of fields****
-- marketo_emails.campaign_id
-- marketo_emails.marketo_email_id,
-- gainsight_emails.template_id,
-- gainsight_emails.template_name,
-- zendesk_emails.from_email 
  FROM combined_emails
  LEFT JOIN marketo_emails
    ON combined_emails.source_system = marketo_emails.source_system
      AND combined_emails.email_activity_type = marketo_emails.email_activity_type
      AND combined_emails.activity_date = marketo_emails.activity_date
  LEFT JOIN gainsight_emails
    ON combined_emails.source_system = gainsight_emails.source_system
      AND combined_emails.email_activity_type = gainsight_emails.email_activity_type
      AND combined_emails.activity_date = gainsight_emails.activity_date
  LEFT JOIN zuora_emails
    ON combined_emails.source_system = zuora_emails.source_system
      AND combined_emails.email_activity_type = zuora_emails.email_activity_type
      AND combined_emails.activity_date = zuora_emails.activity_date
  LEFT JOIN zendesk_emails
    ON combined_emails.source_system = zendesk_emails.source_system
      AND combined_emails.email_activity_type = zendesk_emails.email_activity_type
      AND combined_emails.activity_date = zendesk_emails.activity_date
  LEFT JOIN levelup_emails
    ON combined_emails.source_system = levelup_emails.source_system
      AND combined_emails.email_activity_type = levelup_emails.email_activity_type
      AND combined_emails.activity_date = levelup_emails.activity_date
  LEFT JOIN mailgun_emails
    ON combined_emails.source_system = mailgun_emails.source_system
      AND combined_emails.email_activity_type = mailgun_emails.email_activity_type
      AND combined_emails.activity_date = mailgun_emails.activity_date
)

SELECT 
-- Primary key
{{ dbt_utils.generate_surrogate_key([
    'source_system', 
    'email_activity_type', 
    'activity_date', 
    'contact_email_address',
    'email_activity_id'
]) }} AS email_activity_pk,

-- Foreign key
{{ get_keyed_nulls(dbt_utils.generate_surrogate_key(['person_id'])) }} AS dim_person_sk,
final.*
FROM final
WHERE NOT contact_email_address IS NULL