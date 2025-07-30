{% docs rpt_license_billable_users_daily %}

**Description:**
This model provides a comprehensive view of license and billable users for GitLab Duo Pro, GitLab Duo Enterprise, and Enterprise Agile Planning add-ons across different data sources (Zuora, Seat Link, Service Ping, and Postgres Assignment tables for GitLab.com). It combines data to offer insights for Self-Managed, Dedicated and GitLab.com deployments, including daily license and billable user counts for GitLab.com Duo Pro and Duo Enterprise add-ons.

**Data Grain:**
- reporting_date
- dim_installation_id
- dim_namespace_id

**Filters Applied to Model:**
- Excludes records where both dim_installation_id and dim_namespace_id are NULL
- Includes data for GitLab Duo Pro, GitLab Duo Enterprise and Enterprise Agile Planning add-ons only

**Business Logic in this Model:**
1. Combines license user data from Zuora, Seat Link, Service Ping, and Postgres Assignment tables (for GitLab.com)
2. Includes billable user data from Seat Link and Service Ping and Postgres Assignment tables (for GitLab.com)
3. Handles both Self-Managed, Dedicated (using dim_installation_id) and GitLab.com (using dim_namespace_id) deployments
4. For Self-Managed instances, preserves data points from both Seat Link and Service Ping when available on the same day

**Other Comments:**
- This model **should not** be used to calculate the total number of billable or license seats across Self-Managed installations, as it will lead to overcounting if a single license key is used for multiple installations, as described [here](https://about.gitlab.com/pricing/licensing-faq/#multiple-instances)
- Self-Managed customers can apply their license key to multiple installations, resulting in the same number of purchased seats for every installation with the same license key applied. However, each of those installations may have a different number of assigned seats. Rather than summing purchased or assigned seats across installations (which would produce an incorrect result), the correct method is to pick one record for each customer. Generally, the preferred method is to pick the production installation.
- Some installations or namespaces may have NULL values, which are excluded from the final output.
- The model is intended for analyzing license and billable user counts at the installation or namespace level for GitLab Duo Pro, GitLab Duo Enterprise and Enterprise Agile Planning add-ons. It can be used to track license utilization, identify renewal risks, and identify upsell opportunities for these products.
- Currently, Enterprise Agile Planning does not have any assigned seats metrics instrumented. As the data for those starts flowing, they will be added to this table.

{% enddocs %}


{% docs rpt_user_request_crm_account_issue_epic %}

Aggregation of mart_user_request, which is at the issue/epic||link grain at the issue/epic||crm_account grain. This model connects customer account information with their associated issues and epics, allowing for analysis of customer requests and priorities across different dimensions. The model includes account metrics like ARR, growth scores, and priority calculations.

{% enddocs %}


{% docs rpt_user_request_issue_epic %}

This report model aggregates mart_user_request at the issue/epic level, combining data from potentially multiple linked customer accounts. It summarizes issue and epic information, prioritization scores, and customer context for product requests in the GitLab product. The model calculates priority metrics based on customer ARR impact, opportunity value, and growth/retention potential for each issue or epic. This report is used to prioritize product development efforts by providing visibility into which feature requests have the highest business impact.

{% enddocs %}


{% docs rpt_product_usage_health_score_account_calcs %}

This table calculates account-level scores for % of ARR reporting usage data, license utilization, User Engagement, SCM adoption, CI adoption, CD adoption, and Security Adoption. Its parent model, `rpt_product_usage_health_score` calculates these metrics at the installation/namespace-level. However, we need an installation/namespace-level with account-level metrics because in reporting, end users interact with instance specific scores and require flexibility to compare account scores against specific instances per account. This table also includes records of accounts that do not have any associated Service Ping data.

{% enddocs %}

{% docs rpt_license_utilization_daily %}

**Description:**
This model provides a comprehensive view of license and billable users for GitLab Duo Pro, GitLab Duo Enterprise, and Enterprise Agile Planning add-ons across different data sources (Zuora, Seat Link, Service Ping, and Postgres Assignment tables for GitLab.com). It combines data to offer insights for Self-Managed, Dedicated and GitLab.com deployments, including daily license and billable user counts for GitLab.com Duo Pro and Duo Enterprise add-ons.

**Data Grain:**
- reporting_date
- dim_installation_id
- dim_namespace_id

**Filters Applied to Model:**
- Excludes records where both dim_installation_id and dim_namespace_id are NULL
- Includes data for GitLab Duo Pro, GitLab Duo Enterprise and Enterprise Agile Planning add-ons only

**Business Logic in this Model:**
1. Combines license user data from Zuora, Seat Link, Service Ping, and Postgres Assignment tables (for GitLab.com)
2. Includes billable user data from Seat Link and Service Ping and Postgres Assignment tables (for GitLab.com)
3. Handles both Self-Managed, Dedicated (using dim_installation_id) and GitLab.com (using dim_namespace_id) deployments
4. For Self-Managed instances, preserves data points from both Seat Link and Service Ping when available on the same day

**Other Comments:**
- This model **should not** be used to calculate the total number of billable or license seats across Self-Managed installations, as it will lead to overcounting if a single license key is used for multiple installations, as described [here](https://about.gitlab.com/pricing/licensing-faq/#multiple-instances)
- Self-Managed customers can apply their license key to multiple installations, resulting in the same number of purchased seats for every installation with the same license key applied. However, each of those installations may have a different number of assigned seats. Rather than summing purchased or assigned seats across installations (which would produce an incorrect result), the correct method is to pick one record for each customer. Generally, the preferred method is to pick the production installation.
- Some installations or namespaces may have NULL values, which are excluded from the final output.
- The model is intended for analyzing license and billable user counts at the installation or namespace level for GitLab Duo Pro, GitLab Duo Enterprise and Enterprise Agile Planning add-ons. It can be used to track license utilization, identify renewal risks, and identify upsell opportunities for these products.
- Currently, Enterprise Agile Planning does not have any assigned seats metrics instrumented. As the data for those starts flowing, they will be added to this table.

{% enddocs %}
