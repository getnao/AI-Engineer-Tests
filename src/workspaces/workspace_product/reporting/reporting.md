{% docs wk_rpt_registration_website_page_view %}

This model tracks user interactions with GitLab's registration flow pages, capturing detailed page view information for each step of the registration process. It filters the main website page view fact table to include only specific URLs involved in the user registration and onboarding journey. The model includes comprehensive metadata about each page view, such as referrer information, session details, user context, and browser information. This incremental model supports funnel analysis of the registration flow, conversion optimization, and identification of drop-off points during the sign-up process.

{% enddocs %}


{% docs wk_experiment_event_entity_daily %}

This model aggregates daily event data for GitLab A/B experiments, tracking user interactions with different experiment variants. It combines structured event data specifically tagged with experiment context, providing metrics on user engagement by experiment name and variant. The model organizes events by namespace or experiment context key, enabling analysis at both the entity level and by experiment attributes. This workspace model supports experiment analysis, feature adoption tracking, and data-driven product development decisions.


{% enddocs %}


{% docs wk_mart_ping_product_entity_metric_monthly_all_deployments %}

## Overview
This data source combines product usage metrics across GitLab.com (SaaS) and self-managed GitLab deployments. It provides a unified view of product usage at the namespace level for GitLab.com and installation level for self-managed instances. The model tracks various usage metrics over time, including Monthly Active Users (MAU), Group Monthly Active Users (GMAU), Paid Group Monthly Active Users, and Stage Monthly Active Users (SMAU).

## Model Type
- **Materialization**: Table
- **Tags**: product, mnpi_exception

## Data Sources
This model is built from the following sources:
1. **GitLab.com data**:
   - `fct_ping_namespace_metric`
   - `dim_ping_metric`
   - `dim_namespace`
   - `mart_event_namespace_daily`
   - `dim_crm_account`
   - `dim_subscription`

2. **Self-managed data**:
   - `mart_ping_instance_metric_monthly`
   - `dim_crm_account`
   - `dim_subscription`

## Key Information
- **Time Range**: Contains data for the last 14 months
- **Entity Level**: 
  - GitLab.com: Ultimate Parent Namespace ID
  - Self-managed: Installation ID
- **Product Entity ID**: Presents the Ultimate Parent Namespace ID for GitLab.com deployments and Dim Installation ID for Self-Managed & Dedicated deployments.
- **Time Granularity**: Monthly
- **Ping Types**: Includes both GitLab.com and self-managed instance pings
- **Metric Types**: Includes metrics with timeframes of '28d' (28-day) and 'all' (all-time)



## Model Logic
1. **SaaS (GitLab.com) Processing**: 
   - Pulls namespace-level ping metrics from GitLab.com
   - Joins with dimension tables to enrich with metadata
   - Filters to only include metrics from the last 14 months
   - Takes the latest ping per namespace per month
   - For namespaces with multiple records per metric per month, takes the maximum value

2. **Self-managed Processing**:
   - Pulls instance-level ping metrics from self-managed installations
   - Joins with dimension tables to enrich with metadata
   - Filters to only include metrics from the last 14 months
   - Includes only the last ping of each month
   - Excludes internal instances and ensures positive metric values

3. **Unification**:
   - Combines GitLab.com and self-managed data into a single unified dataset
   - Standardizes field names and types across both sources
   - Creates a surrogate key for entity-metric-month combination

## Data Quality Filters
- Self-managed data filtered to:
  - Non-internal instances
  - Positive metric values (monthly_metric_value > 0)
  - Positive user values (umau_value > 0)
  - Only last ping of each month
- GitLab.com data filtered to:
  - Non-internal namespaces
  - Metrics from the last 14 months

## Usage Notes
- This model is intended for product analytics and customer intelligence
- Useful for understanding product adoption and usage across deployment types
- The data is refreshed with each DBT run, with the latest ping data per entity per month


{% enddocs %}