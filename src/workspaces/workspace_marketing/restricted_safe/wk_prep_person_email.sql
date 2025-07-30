{{ config(
    materialized="table"
) }}

{{ simple_cte([
    ('marketo_lead_source','marketo_lead_source'),
    ('iterable_user_history_source','iterable_user_history_source'), 
    ('sfdc_contact_source','sfdc_contact_source'),
    ('sfdc_lead_source','sfdc_lead_source'), 
    ('commonroom_community_members_source','commonroom_community_members_source'), 
    ('gitlab_dotcom_users_source','gitlab_dotcom_users_source'), 
    ('gitlab_dotcom_user_details_source','gitlab_dotcom_user_details_source'),
    ('zuora_contact_source','zuora_contact_source'), 
    ('customers_db_customers_source','customers_db_customers_source'), 
    ('zendesk_users_masked_source','zendesk_users_masked_source'), 
    ('qualtrics_mailing_contacts','qualtrics_mailing_contacts'), 
    ('email_log_v2','email_log_v2'),
    ('company_person','company_person'),
    ('rally_marketing_list','rally_marketing_list'),
    ('level_up_users_source','level_up_users_source'),
    ('mailgun_events','mailgun_events'),
    ('prep_crm_account','prep_crm_account')
]) }}


, marketo_person AS (

    SELECT
        'Marketo' AS source_system,
        marketo_lead_id AS dim_marketo_person_id,
        email,
        job_title,
        account_id_18_c AS dim_crm_account_id,
        company_name AS account_name,
        first_name AS marketo_first_name,
        last_name AS marketo_last_name,
        is_opt_in,
        opt_in_date,
        opt_out_date,
        CASE
            WHEN opt_out_date IS NOT NULL
                AND (opt_out_date >= opt_in_date
                    OR opt_in_date IS NULL)
            THEN TRUE
            ELSE FALSE
        END AS is_opted_out,
        phone,
        address AS street,
        state,
        country,
        postal_code,
        city,
        created_at::DATE AS created_date
    FROM marketo_lead_source
    WHERE email IS NOT NULL 
      AND created_at IS NOT NULL
    QUALIFY ROW_NUMBER () OVER (PARTITION BY email ORDER BY updated_at DESC) = 1

), iterable_person AS (

    SELECT
        'Iterable' AS source_system,
        iterable_user_first_name AS iterable_first_name,
        iterable_user_last_name AS iterable_last_name,
        iterable_user_phone_number AS phone,
        iterable_user_signup_date AS created_date,
        iterable_user_fivetran_id AS iterable_user_fivetran_id,
        iterable_user_email AS email
    FROM iterable_user_history_source
    WHERE iterable_user_email IS NOT NULL 
    QUALIFY ROW_NUMBER () OVER (PARTITION BY iterable_user_email ORDER BY iterable_user_updated_at DESC) = 1

), crm_person_prep AS (

    SELECT
        'SFDC' AS source_system,
        contact_id AS sfdc_record_id,
        contact_email AS email,
        account_id AS dim_crm_account_id,
        CASE 
          WHEN zoominfo_contact_id IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS is_zoominfo_enriched,
        CASE 
          WHEN cognism_employee_count IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS is_cognism_enriched,
        contact_first_name AS crm_first_name,
        contact_last_name AS crm_last_name,
        contact_status AS person_status,
        created_date,
        mailing_city AS city,
        mailing_country AS country,
        mailing_zip_code AS postal_code,
        mailing_state AS state,
        mailing_address AS street,
        mobile_phone AS phone,
        contact_title AS job_title,
        has_opted_out_email AS is_opted_out,
        zoominfo_contact_id,
        zoominfo_contact_city,
        zoominfo_contact_country,
        zoominfo_contact_state,
        zoominfo_mobile_phone_number,
        zoominfo_phone_number,
        NULL AS cognism_city,
        NULL AS cognism_country,
        NULL AS cognism_state,
        last_modified_date
    FROM sfdc_contact_source
    WHERE is_deleted = FALSE
      AND contact_email IS NOT NULL 
    
    UNION ALL
    
    SELECT
        'SFDC' AS source_system,
        lead_id AS sfdc_record_id,
        lead_email AS email,
        lean_data_matched_account AS dim_crm_account_id,
        CASE 
          WHEN zoominfo_company_id IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS is_zoominfo_enriched,
        CASE 
          WHEN cognism_country IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS is_cognism_enriched,
        lead_first_name AS crm_first_name,
        lead_last_name AS crm_last_name,
        lead_status AS person_status,
        created_date,
        city,
        country,
        postal_code AS postal_code,
        state,
        street,
        zoominfo_phone_number AS phone,
        title AS job_title,
        has_opted_out_email AS is_opted_out,
        NULL AS zoominfo_contact_id,
        zoominfo_contact_city,
        zoominfo_contact_country,
        zoominfo_contact_state,
        zoominfo_mobile_phone_number,
        zoominfo_phone_number,
        cognism_city,
        cognism_country,
        cognism_state,
        last_modified_date
    FROM sfdc_lead_source
    WHERE is_deleted = FALSE
        AND is_converted = FALSE
        AND lead_email IS NOT NULL 

), crm_person AS (

    SELECT *
    FROM crm_person_prep
    QUALIFY ROW_NUMBER () OVER (PARTITION BY email ORDER BY last_modified_date DESC) = 1

), commonroom_person AS (

    SELECT
        'CommonRoom' AS source_system,
        primary_key AS commonroom_id,
        primary_email AS email,
        job_title,
        full_name,
        first_activity_date AS created_date,
        organization_name AS account_name
    FROM commonroom_community_members_source
    WHERE primary_email IS NOT NULL 
    QUALIFY ROW_NUMBER () OVER (PARTITION BY primary_email ORDER BY last_activity_date DESC) = 1

), gitlab_dotcom_person AS (

    SELECT 
       'Gitlab.com' AS source_system,
        email,
        gitlab_dotcom_users_source.user_id                                          AS dim_user_id,
        SPLIT_PART(users_name, ' ', 1)                                              AS first_name,
        SPLIT_PART(users_name, ' ', 2)                                              AS last_name,
        user_name                                                                   AS full_name,
        organization                                                                AS company_name,
        gitlab_dotcom_user_details_source.role                                      AS job_title,
        created_at::DATE                                                            AS created_date,
        gitlab_dotcom_user_details_source.initial_email_opt_in_value                AS is_opted_in
    FROM gitlab_dotcom_users_source
    LEFT JOIN gitlab_dotcom_user_details_source
        ON gitlab_dotcom_users_source.user_id=gitlab_dotcom_user_details_source.user_id
    WHERE email IS NOT NULL
    AND email != ''
    AND state = 'active'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY gitlab_dotcom_users_source.email ORDER BY gitlab_dotcom_users_source.updated_at DESC)=1

), zuora_person AS (

    SELECT 
        'Zuora' AS source_system,
        contact_id AS zuora_contact_id,
        account_id AS zuora_account_id,
        work_email AS email,
        work_phone AS phone,
        first_name AS zuora_first_name,
        last_name AS zuora_last_name,
        created_date,
        street_address AS street,
        city,
        country,
        state,
        postal_code
    FROM zuora_contact_source
    WHERE work_email IS NOT NULL 
    QUALIFY ROW_NUMBER () OVER (PARTITION BY work_email ORDER BY updated_date DESC) = 1

), customer_dot_person AS (

    SELECT
        'CustomersDot' AS source_system,
        customer_first_name,
        company,
        customer_id AS customer_dot_person_id,
        country,
        customer_created_at::DATE AS created_date,
        state,
        city,
        sfdc_account_id AS dim_crm_account_id,
        customer_phone_number AS phone,
        customer_last_name,
        customer_email AS email
    FROM customers_db_customers_source
    WHERE customer_email IS NOT NULL
    QUALIFY ROW_NUMBER () OVER (PARTITION BY customer_email ORDER BY customer_updated_at DESC) = 1

), zendesk_person AS (

    SELECT
        'Zendesk' AS source_system,
        email,
        created_at::DATE AS created_date,
        user_id AS zendesk_person_id,
        organization_id AS zendesk_company_id,
        name,
        phone
    FROM zendesk_users_masked_source
    WHERE email IS NOT NULL
    QUALIFY ROW_NUMBER () OVER (PARTITION BY email ORDER BY updated_at DESC) = 1

), qualtrics_person AS (

    SELECT
        'Qualtrics' AS source_system,
        contact_id AS qualtrics_person_id,
        contact_email AS email,
        contact_first_name AS qualtrics_first_name,
        contact_last_name AS qualtrics_last_name,
        contact_phone AS phone,
        is_unsubscribed
    FROM qualtrics_mailing_contacts
    WHERE contact_email IS NOT NULL
    QUALIFY ROW_NUMBER () OVER (PARTITION BY contact_email ORDER BY mailing_list_membership_observed_at DESC) = 1

), gainsight_person AS (

    SELECT
        'Gainsight' AS source_system,
        email_log_v2.gs_person_id AS gainsight_person_id,
        email_log_v2.created_date,
        email_log_v2.name,
        email_log_v2.email_id AS email,
        company_person.email_opt_out_gc AS is_opted_out,
        company_person.sfdc_account_id AS dim_crm_account_id,
        company_person.title,
        company_person.gsid AS gainsight_company_id
    FROM email_log_v2
    LEFT JOIN company_person
      ON email_log_v2.gs_company_person_id=company_person.person_id
    WHERE email_log_v2.email_id IS NOT NULL
    QUALIFY ROW_NUMBER () OVER (PARTITION BY email_log_v2.email_id ORDER BY company_person.modified_date DESC) = 1

), level_up_person AS (

    SELECT 
        'LevelUp' AS source_system,
        country,
        created_at::DATE AS created_date,
        first_name AS level_up_first_name,
        user_id AS levelup_person_id,
        last_name AS level_up_last_name,
        ref4_user_company AS company,
        state,
        zip_code AS postal_code,
        username AS email
    FROM level_up_users_source
    WHERE username IS NOT NULL
    QUALIFY ROW_NUMBER () OVER (PARTITION BY username ORDER BY last_active_at DESC) = 1

), rally_person AS (

    SELECT
      'Rally' AS source_system,
      email_address AS email,
      job_title,
      is_opted_out
    FROM rally_marketing_list 
    
),

mailgun_person AS (
    SELECT
        'Mailgun' AS source_system,
        CASE
        WHEN message_headers_to LIKE '%emailtosalesforc%' THEN recipient
        WHEN recipient LIKE '%emailtosalesforc%' THEN message_headers_to
        ELSE recipient
        END       AS email
    FROM mailgun_events
),

 email_unioned_base AS (

    SELECT DISTINCT
        email,
        source_system,
        1 AS source_rank
    FROM marketo_person
   
    UNION ALL
    
    SELECT DISTINCT
        email,
        source_system,
        2 AS source_rank
    FROM iterable_person
    
    UNION ALL
    
    SELECT DISTINCT
        email,
        source_system,
        3 AS source_rank
    FROM crm_person
    
    UNION ALL
    
    SELECT DISTINCT
        email,
        source_system,
        4 AS source_rank
    FROM commonroom_person
    
    UNION ALL
    
    SELECT DISTINCT
        email,
        source_system,
        5 AS source_rank
    FROM gitlab_dotcom_person
    
    UNION ALL
    
    SELECT DISTINCT
        email,
        source_system,
        6 AS source_rank
    FROM zuora_person
   
    UNION ALL
    
    SELECT DISTINCT
        email,
        source_system,
        7 AS source_rank
    FROM customer_dot_person
    
    UNION ALL
    
    SELECT DISTINCT
        email,
        source_system,
        8 AS source_rank
    FROM zendesk_person
    
    UNION ALL
    
    SELECT DISTINCT
        email,
        source_system,
        9 AS source_rank
    FROM qualtrics_person
    
    UNION ALL
    
    SELECT DISTINCT
        email,
        source_system,
        10 AS source_rank
    FROM gainsight_person
   
    UNION ALL
    
    SELECT DISTINCT
        email,
        source_system,
        11 AS source_rank
    FROM level_up_person
    
    UNION ALL
    
    SELECT
      email,
      source_system,
      12 AS source_rank
    FROM rally_person

    UNION ALL
    SELECT DISTINCT
        email,
        source_system,
        13 AS source_rank
    FROM mailgun_person

), email_unioned AS (

    SELECT DISTINCT
        email,
        REGEXP_REPLACE(SPLIT_PART(email, '@', 2) , '\.[^.]*$', '') as domain_name,
        LISTAGG(source_system, ', ')
     WITHIN GROUP (ORDER BY source_rank ASC) AS source_system_array
    FROM email_unioned_base
    GROUP BY 1,2
    
), coalesce_stage AS (

    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['email_unioned.email']) }}                                AS dim_marketing_contact_id,
        email_unioned.email,
        email_unioned.domain_name,
        CASE 
          WHEN crm_person.job_title IS NOT NULL
            THEN crm_person.job_title
          WHEN marketo_person.job_title IS NOT NULL
            THEN marketo_person.job_title
          WHEN commonroom_person.job_title IS NOT NULL
            THEN commonroom_person.job_title
          WHEN gitlab_dotcom_person.job_title IS NOT NULL
            THEN gitlab_dotcom_person.job_title
          WHEN gainsight_person.title IS NOT NULL
            THEN gainsight_person.title
          WHEN rally_person.job_title IS NOT NULL
            THEN rally_person.job_title
        END AS job_title,
        CASE 
          WHEN crm_person.crm_first_name IS NOT NULL
            THEN crm_person.crm_first_name
          WHEN marketo_person.marketo_first_name IS NOT NULL
            THEN marketo_person.marketo_first_name
          WHEN iterable_person.iterable_first_name IS NOT NULL
            THEN iterable_person.iterable_first_name
          WHEN gitlab_dotcom_person.first_name IS NOT NULL
            THEN gitlab_dotcom_person.first_name
          WHEN zuora_person.zuora_first_name IS NOT NULL
            THEN zuora_person.zuora_first_name
          WHEN customer_dot_person.customer_first_name IS NOT NULL
            THEN customer_dot_person.customer_first_name
          WHEN level_up_person.level_up_first_name IS NOT NULL
            THEN level_up_person.level_up_first_name
          WHEN qualtrics_person.qualtrics_first_name IS NOT NULL
            THEN qualtrics_person.qualtrics_first_name
        END AS first_name,
        CASE 
          WHEN crm_person.crm_last_name IS NOT NULL
            THEN crm_person.crm_last_name
          WHEN marketo_person.marketo_last_name IS NOT NULL
            THEN marketo_person.marketo_last_name
          WHEN iterable_person.iterable_last_name IS NOT NULL
            THEN iterable_person.iterable_last_name
          WHEN gitlab_dotcom_person.last_name IS NOT NULL
            THEN gitlab_dotcom_person.last_name
          WHEN zuora_person.zuora_last_name IS NOT NULL
            THEN zuora_person.zuora_last_name
          WHEN customer_dot_person.customer_last_name IS NOT NULL
            THEN customer_dot_person.customer_last_name
          WHEN level_up_person.level_up_last_name IS NOT NULL
            THEN level_up_person.level_up_last_name
          WHEN qualtrics_person.qualtrics_last_name IS NOT NULL
            THEN qualtrics_person.qualtrics_last_name
        END AS last_name,
        CASE 
          WHEN gitlab_dotcom_person.full_name IS NOT NULL
            THEN gitlab_dotcom_person.full_name
          WHEN zendesk_person.name IS NOT NULL
            THEN zendesk_person.name
          WHEN gainsight_person.name IS NOT NULL
            THEN gainsight_person.name
        END AS full_name_prep,
        CASE
          WHEN full_name_prep IS NULL
            THEN CONCAT(first_name,' ',last_name)
            ELSE full_name_prep
        END AS full_name,
        CASE 
          WHEN crm_person.created_date IS NOT NULL
            THEN crm_person.created_date
          WHEN marketo_person.created_date IS NOT NULL
            THEN marketo_person.created_date
          WHEN iterable_person.created_date IS NOT NULL
            THEN iterable_person.created_date
          WHEN commonroom_person.created_date IS NOT NULL
            THEN commonroom_person.created_date
          WHEN gitlab_dotcom_person.created_date IS NOT NULL
            THEN gitlab_dotcom_person.created_date
          WHEN zuora_person.created_date IS NOT NULL
            THEN zuora_person.created_date
          WHEN customer_dot_person.created_date IS NOT NULL
            THEN customer_dot_person.created_date
          WHEN zendesk_person.created_date IS NOT NULL
            THEN zendesk_person.created_date
          WHEN gainsight_person.created_date IS NOT NULL
            THEN gainsight_person.created_date
          WHEN level_up_person.created_date IS NOT NULL
            THEN level_up_person.created_date
        END AS created_date,
        CASE 
          WHEN crm_person.street IS NOT NULL
            THEN crm_person.street
          WHEN marketo_person.street IS NOT NULL
            THEN marketo_person.street
          WHEN zuora_person.street IS NOT NULL
            THEN zuora_person.street
        END AS street,
        CASE 
          WHEN crm_person.city IS NOT NULL
            THEN crm_person.city
          WHEN marketo_person.city IS NOT NULL
            THEN marketo_person.city
          WHEN zuora_person.city IS NOT NULL
            THEN zuora_person.city
          WHEN customer_dot_person.city IS NOT NULL
            THEN customer_dot_person.city
        END AS city,
        CASE 
          WHEN crm_person.state IS NOT NULL
            THEN crm_person.state
          WHEN marketo_person.state IS NOT NULL
            THEN marketo_person.state
          WHEN zuora_person.state IS NOT NULL
            THEN zuora_person.state
          WHEN customer_dot_person.state IS NOT NULL
            THEN customer_dot_person.state
          WHEN level_up_person.state IS NOT NULL
            THEN level_up_person.state
        END AS state,
        CASE 
          WHEN crm_person.country IS NOT NULL
            THEN crm_person.country
          WHEN marketo_person.country IS NOT NULL
            THEN marketo_person.country
          WHEN zuora_person.country IS NOT NULL
            THEN zuora_person.country
          WHEN customer_dot_person.country IS NOT NULL
            THEN customer_dot_person.country
          WHEN level_up_person.country IS NOT NULL
            THEN level_up_person.country
        END AS country,
        CASE 
          WHEN crm_person.postal_code IS NOT NULL
            THEN crm_person.postal_code
          WHEN marketo_person.postal_code IS NOT NULL
            THEN marketo_person.postal_code
          WHEN zuora_person.postal_code IS NOT NULL
            THEN zuora_person.postal_code
          WHEN level_up_person.postal_code IS NOT NULL
            THEN level_up_person.postal_code
        END AS postal_code,
        CASE 
          WHEN prep_crm_account.crm_account_name IS NOT NULL
            THEN prep_crm_account.crm_account_name
          WHEN marketo_person.account_name IS NOT NULL
            THEN marketo_person.account_name
          WHEN commonroom_person.account_name IS NOT NULL
            THEN commonroom_person.account_name
          WHEN customer_dot_person.company IS NOT NULL
            THEN customer_dot_person.company
          WHEN level_up_person.company IS NOT NULL
            THEN level_up_person.company
        END AS account_name,
        CASE 
          WHEN crm_person.phone IS NOT NULL
            THEN crm_person.phone
          WHEN marketo_person.phone IS NOT NULL
            THEN marketo_person.phone
          WHEN iterable_person.phone IS NOT NULL
            THEN iterable_person.phone
          WHEN zuora_person.phone IS NOT NULL
            THEN zuora_person.phone
          WHEN customer_dot_person.phone IS NOT NULL
            THEN customer_dot_person.phone
          WHEN zendesk_person.phone IS NOT NULL
            THEN zendesk_person.phone
          WHEN qualtrics_person.phone IS NOT NULL
            THEN qualtrics_person.phone
        END AS phone,
        CASE 
          WHEN crm_person.is_opted_out IS NOT NULL
            THEN crm_person.is_opted_out
          WHEN marketo_person.is_opted_out IS NOT NULL
            THEN marketo_person.is_opted_out
          WHEN gainsight_person.is_opted_out IS NOT NULL
            THEN gainsight_person.is_opted_out
          WHEN rally_person.is_opted_out IS NOT NULL
            THEN rally_person.is_opted_out
        END AS is_opted_out,
        gitlab_dotcom_person.is_opted_in AS is_gldc_opted_in,
        IFF(crm_person.email IS NOT NULL, TRUE, FALSE) AS is_in_sfdc,
        crm_person.is_zoominfo_enriched AS is_zoominfo_enriched_sfdc,
        crm_person.is_cognism_enriched AS is_cognism_enriched_sfdc,
        crm_person.zoominfo_contact_id AS zoominfo_contact_id_sfdc,
        crm_person.zoominfo_contact_city AS zoominfo_contact_city_sfdc,
        crm_person.zoominfo_contact_country AS zoominfo_contact_country_sfdc,
        crm_person.zoominfo_contact_state AS zoominfo_contact_state_sfdc,
        crm_person.zoominfo_mobile_phone_number AS zoominfo_mobile_phone_number_sfdc,
        crm_person.zoominfo_phone_number AS zoominfo_phone_number_sfdc,
        crm_person.cognism_city AS cognism_city_sfdc,
        crm_person.cognism_country AS cognism_country_sfdc,
        crm_person.cognism_state AS cognism_state_sfdc,
        IFF(marketo_person.email IS NOT NULL, TRUE, FALSE) AS is_in_marketo,
        iterable_person.iterable_user_fivetran_id,
        IFF(iterable_person.email IS NOT NULL, TRUE, FALSE) AS is_in_iterable,
        IFF(commonroom_person.email IS NOT NULL, TRUE, FALSE) AS is_in_commonroom,
        gitlab_dotcom_person.dim_user_id,
        IFF(gitlab_dotcom_person.email IS NOT NULL, TRUE, FALSE) AS is_in_gitlabdotcom,
        zuora_person.zuora_account_id,
        IFF(zuora_person.email IS NOT NULL, TRUE, FALSE) AS is_in_zuora,
        customer_dot_person.customer_dot_person_id,
        IFF(customer_dot_person.email IS NOT NULL, TRUE, FALSE) AS is_in_customerdot,
        zendesk_person.zendesk_person_id,
        zendesk_person.zendesk_company_id,
        IFF(zendesk_person.email IS NOT NULL, TRUE, FALSE) AS is_in_zendesk,
        qualtrics_person.qualtrics_person_id,
        IFF(qualtrics_person.email IS NOT NULL, TRUE, FALSE) AS is_in_qualtrics,
        IFF(gainsight_person.email IS NOT NULL, TRUE, FALSE) AS is_in_gainsight,
        level_up_person.levelup_person_id,
        IFF(level_up_person.email IS NOT NULL, TRUE, FALSE) AS is_in_levelup,
        IFF(rally_person.email IS NOT NULL, TRUE, FALSE) AS is_in_rally,
        IFF(mailgun_person.email IS NOT NULL, TRUE, FALSE) AS is_in_mailgun,
        email_unioned.source_system_array
    FROM email_unioned
    LEFT JOIN marketo_person
        ON email_unioned.email=marketo_person.email
    LEFT JOIN iterable_person
        ON email_unioned.email=iterable_person.email
    LEFT JOIN crm_person
        ON email_unioned.email=crm_person.email
    LEFT JOIN commonroom_person
        ON email_unioned.email=commonroom_person.email
    LEFT JOIN gitlab_dotcom_person
        ON email_unioned.email=gitlab_dotcom_person.email
    LEFT JOIN zuora_person 
        ON email_unioned.email=zuora_person .email
    LEFT JOIN customer_dot_person
        ON email_unioned.email=customer_dot_person.email
    LEFT JOIN zendesk_person
        ON email_unioned.email=zendesk_person.email
    LEFT JOIN qualtrics_person
        ON email_unioned.email=qualtrics_person.email
    LEFT JOIN gainsight_person
        ON email_unioned.email=gainsight_person.email
    LEFT JOIN level_up_person
        ON email_unioned.email=level_up_person.email
    LEFT JOIN rally_person
        ON email_unioned.email=rally_person.email
    LEFT JOIN mailgun_person
        ON email_unioned.email=mailgun_person.email
    LEFT JOIN prep_crm_account
        ON crm_person.dim_crm_account_id=prep_crm_account.dim_crm_account_id

),

final AS (
SELECT * EXCLUDE(full_name),
IFF(full_name IS NULL and first_name IS NOT NULL,CONCAT(first_name,' ',last_name),full_name) AS full_name
FROM coalesce_stage)

SELECT * 
FROM final

