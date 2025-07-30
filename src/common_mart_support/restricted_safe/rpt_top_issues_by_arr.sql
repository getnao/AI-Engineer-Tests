{% set ticket_lookback_days = 60 %}

{{ simple_cte([
  ('dim_date', 'dim_date'),
  ('mart_arr', 'mart_arr'),
  ('dim_support_organization', 'dim_support_organization'),
  ('dim_crm_account', 'dim_crm_account'),
  ('dim_issue', 'dim_issue'),
  ('fct_support_ticket', 'fct_support_ticket'),
  ('dim_support_ticket', 'dim_support_ticket')
]) }},

current_fiscal_year_cte AS (

  SELECT  current_fiscal_year
  FROM dim_date
  LIMIT 1

),

arr_by_customer AS (
    -- Step 1: Get current ARR for each account
    SELECT  
      dim_crm_account_id,
      ROUND(SUM(arr), 0)                       AS total_arr
    FROM mart_arr
    INNER JOIN current_fiscal_year_cte
      ON mart_arr.fiscal_year = current_fiscal_year_cte.current_fiscal_year 
    WHERE LOWER(subscription_status) = 'active'
    GROUP BY dim_crm_account_id

),

customer_support_orgs AS (
    -- Step 2: Map top customers to their support organizations
    SELECT 
      arr_by_customer.dim_crm_account_id                    AS dim_crm_account_id,
      arr_by_customer.total_arr                             AS total_arr,
      dim_support_organization.dim_support_organization_id  AS dim_support_organization_id,
      dim_support_organization.organization_name            AS organization_name,
      dim_support_organization.salesforce_account_id        AS salesforce_account_id,
      dim_crm_account.crm_account_name                      AS crm_account_name
    FROM arr_by_customer
    INNER JOIN dim_support_organization
        ON arr_by_customer.dim_crm_account_id = dim_support_organization.salesforce_account_id
    LEFT JOIN dim_crm_account
        ON arr_by_customer.dim_crm_account_id = dim_crm_account.dim_crm_account_id
),

customer_tickets AS (
    -- Step 3: Get all support tickets for these customers within lookback period
    -- Only include tickets that have gitlab-org tags to reduce processing
    SELECT 
        customer_support_orgs.dim_crm_account_id        AS dim_crm_account_id,
        customer_support_orgs.total_arr                 AS total_arr,
        customer_support_orgs.organization_name         AS organization_name,
        customer_support_orgs.crm_account_name          AS crm_account_name,
        dim_support_ticket.dim_support_ticket_id        AS dim_support_ticket_id,
        dim_support_ticket.ticket_tags_array            AS ticket_tags_array,
        fct_support_ticket.created_at                   AS created_at
    FROM customer_support_orgs
    INNER JOIN fct_support_ticket
        ON customer_support_orgs.dim_support_organization_id = fct_support_ticket.dim_support_organization_id
    INNER JOIN dim_support_ticket
        ON fct_support_ticket.dim_support_ticket_id = dim_support_ticket.dim_support_ticket_id
    WHERE fct_support_ticket.created_at >= CURRENT_DATE - INTERVAL '{{ ticket_lookback_days }} days'
        AND dim_support_ticket.ticket_tags_array IS NOT NULL
        AND ARRAY_TO_STRING(dim_support_ticket.ticket_tags_array, ',') LIKE '%gitlab-org%'
),

gitlab_issue_extracts AS (
  -- Step 4: Extract GitLab issue references from ticket tags using array functions
  -- Parse issue IDs, project IDs, and project paths from various tag formats
  SELECT 
    customer_tickets.dim_crm_account_id             AS dim_crm_account_id,
    customer_tickets.total_arr                      AS total_arr,
    customer_tickets.organization_name              AS organization_name,
    customer_tickets.crm_account_name               AS crm_account_name,
    customer_tickets.dim_support_ticket_id          AS dim_support_ticket_id,
    customer_tickets.created_at                     AS created_at,
    
    -- Extract individual tags that match GitLab issue patterns from array
    tag_value.value::STRING                         AS tag_name,
    
    -- Parse the issue IID from different tag formats using regex
    CASE 
      WHEN tag_name LIKE 'gitlab-org_gitlab_issues_%' 
        THEN REGEXP_SUBSTR(tag_name, 'gitlab-org_gitlab_issues_([0-9]+)', 1, 1, 'i', 1)
      WHEN tag_name LIKE 'gitlab-org_customers-gitlab-com_issues_%' 
        THEN REGEXP_SUBSTR(tag_name, 'gitlab-org_customers-gitlab-com_issues_([0-9]+)', 1, 1, 'i', 1)
      WHEN tag_name LIKE 'gitlab-org_%_issues_%' 
        THEN REGEXP_SUBSTR(tag_name, '_issues_([0-9]+)', 1, 1, 'i', 1)
      -- NEW: Handle tags like 'issue_278964_5014' format (project_id_issue_iid)
      WHEN tag_name LIKE 'issue_%_%' AND tag_name NOT LIKE '%gitlab-org%'
        THEN REGEXP_SUBSTR(tag_name, 'issue_[0-9]+_([0-9]+)', 1, 1, 'i', 1)
      ELSE NULL
    END                                             AS issue_iid_from_tag,
    
    -- NEW: Parse project_id from tags like 'issue_278964_5014' format
    CASE 
      WHEN tag_name LIKE 'issue_%_%' AND tag_name NOT LIKE '%gitlab-org%'
        THEN REGEXP_SUBSTR(tag_name, 'issue_([0-9]+)_[0-9]+', 1, 1, 'i', 1)::NUMBER
      ELSE NULL
    END                                             AS project_id_from_tag,
    
    -- Determine the project path from tag pattern (keeping existing logic for gitlab-org patterns)
    CASE 
      WHEN tag_name LIKE 'gitlab-org_gitlab_issues_%' THEN 'gitlab'
      WHEN tag_name LIKE 'gitlab-org_customers-gitlab-com_issues_%' THEN 'customers-gitlab-com'
      WHEN tag_name LIKE 'gitlab-org_cli_issues_%' THEN 'cli'
      WHEN tag_name LIKE 'gitlab-org_omnibus-gitlab_issues_%' THEN 'omnibus-gitlab'
      WHEN tag_name LIKE 'gitlab-org_gitlab-runner_issues_%' THEN 'gitlab-runner'
      -- NEW: For project_id_issue_iid format, we'll get project_path from dim_issue table
      WHEN tag_name LIKE 'issue_%_%' AND tag_name NOT LIKE '%gitlab-org%' THEN 'from_project_id'
      ELSE 'other'
    END                                             AS project_path_from_tag,
    
    -- Classify reference type (issue vs merge request)
    CASE 
      WHEN tag_name LIKE '%_issues_%' THEN 'issue'
      WHEN tag_name LIKE '%_mergerequests_%' THEN 'merge_request'
      WHEN tag_name LIKE 'issue_%_%' AND tag_name NOT LIKE '%gitlab-org%' THEN 'issue'
      ELSE 'other'
    END                                             AS reference_type
  FROM customer_tickets,
  LATERAL FLATTEN(customer_tickets.ticket_tags_array) AS tag_value
  WHERE (tag_name LIKE '%gitlab-org%' 
    AND (tag_name LIKE '%_issues_%' OR tag_name LIKE '%_mergerequests_%'))
    OR (tag_name LIKE 'issue_%_%' AND tag_name NOT LIKE '%gitlab-org%')
),

tickets_with_gitlab_issues AS (
  -- Step 5: Filter and clean the extracted issue references
  -- Only keep records with valid issue IIDs
  SELECT 
    gitlab_issue_extracts.dim_crm_account_id        AS dim_crm_account_id,
    gitlab_issue_extracts.total_arr                 AS total_arr,
    gitlab_issue_extracts.organization_name         AS organization_name,
    gitlab_issue_extracts.crm_account_name          AS crm_account_name,
    gitlab_issue_extracts.dim_support_ticket_id     AS dim_support_ticket_id,
    gitlab_issue_extracts.created_at                AS created_at,
    gitlab_issue_extracts.tag_name                  AS gitlab_tag,
    gitlab_issue_extracts.issue_iid_from_tag::NUMBER AS issue_iid,
    gitlab_issue_extracts.project_id_from_tag       AS project_id_from_tag,
    gitlab_issue_extracts.project_path_from_tag     AS project_path_from_tag,
    gitlab_issue_extracts.reference_type            AS reference_type
  FROM gitlab_issue_extracts
  WHERE gitlab_issue_extracts.issue_iid_from_tag IS NOT NULL
      AND gitlab_issue_extracts.issue_iid_from_tag != ''
),

issue_customer_summary AS (
  -- Step 6: Aggregate by issue - count mentions and sum ARR (without ranking yet)
  -- Create lists of support tickets and customers for each issue
  SELECT 
      tickets_with_gitlab_issues.issue_iid                                 AS issue_iid,
      tickets_with_gitlab_issues.project_id_from_tag                       AS project_id_from_tag,
      tickets_with_gitlab_issues.project_path_from_tag                     AS project_path_from_tag,
      tickets_with_gitlab_issues.reference_type                            AS reference_type,
      
      -- Count metrics
      COUNT(DISTINCT tickets_with_gitlab_issues.dim_support_ticket_id)     AS total_mentions,
      COUNT(DISTINCT tickets_with_gitlab_issues.dim_crm_account_id)        AS unique_customers,
      
      -- ARR metrics (actual values for ranking)
      SUM(tickets_with_gitlab_issues.total_arr)                            AS total_customer_arr,
      
      -- Lists of related tickets and customers (now showing customer names)
      LISTAGG(DISTINCT tickets_with_gitlab_issues.dim_support_ticket_id::STRING, ', ') 
          WITHIN GROUP (ORDER BY (SELECT NULL))                            AS support_ticket_ids,
      LISTAGG(DISTINCT COALESCE(tickets_with_gitlab_issues.crm_account_name, 'Unknown Customer'), ', ') 
          WITHIN GROUP (ORDER BY (SELECT NULL))                            AS mentioning_customer_names,
          
      -- Date range of mentions
      MIN(tickets_with_gitlab_issues.created_at)                           AS first_mentioned_date,
      MAX(tickets_with_gitlab_issues.created_at)                           AS last_mentioned_date
  FROM tickets_with_gitlab_issues
  GROUP BY 1,2,3,4
),

label_parsing AS (
  -- Step 7: Parse labels from dim_issue to extract group and section values
  SELECT 
    dim_issue.issue_id,
    dim_issue.issue_internal_id,
    dim_issue.dim_project_sk,
    dim_issue.issue_title,
    dim_issue.issue_type,
    dim_issue.severity,
    dim_issue.priority,
    dim_issue.milestone_title,
    dim_issue.weight,
    dim_issue.labels,
    dim_issue.created_at,
    dim_issue.updated_at,
    dim_issue.issue_closed_at,
    dim_issue.issue_url,
    
    -- Extract group, section, and category from labels
    MAX(CASE 
        WHEN label_value.value::STRING LIKE 'group::%' 
        THEN REGEXP_SUBSTR(label_value.value::STRING, 'group::(.+)', 1, 1, 'i', 1)
    END) AS group_value,
    
    MAX(CASE 
        WHEN label_value.value::STRING LIKE 'section::%' 
        THEN REGEXP_SUBSTR(label_value.value::STRING, 'section::(.+)', 1, 1, 'i', 1)
    END) AS section_value,
    
    -- Extract all categories and combine them
    LISTAGG(
        CASE 
            WHEN label_value.value::STRING LIKE 'category:%' 
            THEN REGEXP_SUBSTR(label_value.value::STRING, 'category:(.+)', 1, 1, 'i', 1)
        END, ', '
    ) WITHIN GROUP (ORDER BY label_value.value::STRING) AS category_value
    
  FROM dim_issue,
  LATERAL FLATTEN(dim_issue.labels) AS label_value
  GROUP BY 
    dim_issue.issue_id,
    dim_issue.issue_internal_id,
    dim_issue.dim_project_sk,
    dim_issue.issue_title,
    dim_issue.issue_type,
    dim_issue.severity,
    dim_issue.priority,
    dim_issue.milestone_title,
    dim_issue.weight,
    dim_issue.labels,
    dim_issue.created_at,
    dim_issue.updated_at,
    dim_issue.issue_closed_at,
    dim_issue.issue_url
),

final AS (
  -- Step 8: Join with parsed issue data to get full details and calculate final ranking
  -- Enrich customer data with GitLab issue metadata and calculated fields
  SELECT 
    RANK() OVER (
      ORDER BY issue_customer_summary.total_customer_arr DESC
    )                                                                     AS arr_rank,
    
    label_parsing.issue_id                                                AS issue_id,
    COALESCE(issue_customer_summary.project_id_from_tag, 
            label_parsing.dim_project_sk)                                 AS project_id,
    issue_customer_summary.issue_iid                                      AS issue_iid,
    label_parsing.issue_title                                             AS issue_title,

    -- Customer names and ticket details
    issue_customer_summary.total_mentions                                 AS total_mentions,
    issue_customer_summary.unique_customers                               AS unique_customers,
    issue_customer_summary.mentioning_customer_names                      AS mentioning_customer_names,
    issue_customer_summary.support_ticket_ids                             AS support_ticket_ids,

    -- Classify issue type based on labels 
    CASE 
      WHEN ARRAY_TO_STRING(label_parsing.labels, ',') LIKE '%bug%' 
        THEN 'Bug'
      WHEN ARRAY_TO_STRING(label_parsing.labels, ',') LIKE '%feature%' 
        THEN 'Feature'
      WHEN ARRAY_TO_STRING(label_parsing.labels, ',') LIKE '%enhancement%' 
        THEN 'Enhancement'
      ELSE label_parsing.issue_type
    END                                                                   AS issue_type,

    -- Issue categorisation
    label_parsing.severity                                                AS severity,
    label_parsing.priority                                                AS priority,
    label_parsing.milestone_title                                         AS milestone,
    label_parsing.section_value                                           AS section,
    label_parsing.group_value                                             AS group_label,
    label_parsing.category_value                                          AS category_label,
    label_parsing.weight                                                  AS weight,
    issue_customer_summary.project_path_from_tag                          AS project_path,
    issue_customer_summary.reference_type                                 AS reference_type,
    
    -- Include labels column from dim_issue
    label_parsing.labels                                                  AS issue_labels,

    -- Date fields
    label_parsing.created_at                                              AS issue_created_at,
    label_parsing.updated_at                                              AS issue_updated_at,
    label_parsing.issue_closed_at                                         AS issue_closed_at,
    -- Calculate days the issue has been open and cater to tickets
    --  that are still open where the closed_at is null
    COALESCE(
      DATEDIFF('day', label_parsing.created_at, label_parsing.issue_closed_at),
      DATEDIFF('day', label_parsing.created_at, CURRENT_DATE)
    )                                                                     AS days_open,
    issue_customer_summary.first_mentioned_date                           AS first_mentioned_date,
    issue_customer_summary.last_mentioned_date                            AS last_mentioned_date,
    label_parsing.issue_url                                               AS issue_url

  FROM issue_customer_summary
  LEFT JOIN label_parsing
      ON issue_customer_summary.issue_iid = label_parsing.issue_internal_id 
         AND issue_customer_summary.project_id_from_tag = label_parsing.dim_project_sk
      
  WHERE label_parsing.issue_title IS NOT NULL  -- Only include non-masked (i.e. non-confidential) issues

)

SELECT *
FROM final