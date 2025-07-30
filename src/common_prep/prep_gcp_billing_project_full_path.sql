WITH project_ancestory AS (

  SELECT
    source_primary_key,
    LISTAGG(folder_name, '/') WITHIN GROUP (ORDER BY hierarchy_level DESC) AS full_path
  FROM {{ ref('prep_gcp_billing_project_ancestry') }}
  WHERE uploaded_at >= '2022-01-01' 
    AND hierarchy_level > 1
  GROUP BY 1

), gcp_billing_summary AS (

  SELECT * 
  FROM {{ ref('summary_gcp_billing_source') }}
  WHERE invoice_month >= '2022-01-01'

), renamed AS (

  SELECT DISTINCT
    gcp_billing_summary.usage_start_time,
    gcp_billing_summary.project_id          AS gcp_project_id,
    project_ancestory.full_path
  FROM project_ancestory
  INNER JOIN gcp_billing_summary
    ON project_ancestory.source_primary_key = gcp_billing_summary.primary_key

)

SELECT
  gcp_project_id,
  full_path,
  MIN(usage_start_time)                                                                                                     AS first_created_at,
  MAX(usage_start_time)                                                                                                     AS last_updated_at,
  IFF(ROW_NUMBER() OVER (PARTITION BY gcp_project_id ORDER BY last_updated_at DESC,first_created_at DESC) = 1, TRUE, FALSE) AS is_most_recent
FROM renamed
GROUP BY 1, 2