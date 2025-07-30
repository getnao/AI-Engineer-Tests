{% docs finops_pipeline_desc_infra %}

**rpt_gcp_billing_infra_mapping_day**

    Mission: Map GCP billing data to infrastructure labels.
    Objective: Provide daily GCP billing data with additional metadata for better reporting and analysis.
    Input: Raw billing data from GCP BigQuery
    Granularity: Daily
    Inputs: Raw GCP billing data

    day: date - Date of the record
    gcp_project_id: varchar - GCP project identifier
    gcp_service_description: varchar - GCP service description
    gcp_sku_description: varchar - GCP SKU description
    infra_label: varchar - Infrastructure label
    env_label: varchar - Environment label
    runner_label: varchar - Runner label
    usage_unit: varchar - Unit of usage
    pricing_unit: varchar - Unit of pricing
    usage_amount: float - Amount of usage
    usage_amount_in_pricing_units: float - Usage amount in pricing units
    cost_before_credits: float - Cost before credits applied
    net_cost: float - Net cost after credits applied
    usage_standard_unit: varchar - Standard unit of usage
    usage_amount_in_standard_unit: float - Usage amount in standard units

{% enddocs %}

{% docs finops_pipeline_desc_pl_day %}

**rpt_gcp_billing_pl_day**

    Mission: Calculate daily GCP billing data by Profit & Loss categories.
    Objective: Provide a daily overview of GCP costs by pl_category for reporting and cost analysis. This table will be the main source for https://app.periscopedata.com/app/gitlab:safe-dashboard/1116156/WIP:-GCP-Billing:-Cockpit---Infra-labels-version
    Granularity: Daily
    Inputs: rpt_gcp_billing_infra_mapping_day, combined_pl_mapping

    date_day: date - Date of the record
    gcp_project_id: varchar - GCP project identifier
    gcp_service_description: varchar - GCP service description
    gcp_sku_description: varchar - GCP SKU description
    infra_label: varchar - Infrastructure label
    env_label: varchar - Environment label
    runner_label: varchar - Runner label
    pl_category: varchar - Profit & Loss category
    usage_unit: varchar - Unit of usage
    pricing_unit: varchar - Unit of pricing
    usage_amount: float - Amount of usage
    usage_amount_in_pricing_units: float - Usage amount in pricing units
    cost_before_credits: float - Cost before credits applied
    net_cost: float - Net cost after credits applied
    usage_standard_unit: varchar - Standard unit of usage
    usage_amount_in_standard_unit: float - Usage amount in standard units
    from_mapping: varchar - Source of mapping

{% enddocs %}

{% docs finops_pipeline_desc_combined %}

**combined_pl_mappings**

    Mission: Combine all Profit & Loss mappings into a single model.
    Objective: Create a unified model to simplify the mapping process and improve maintainability.
    Granularity: Daily
    Inputs: Various PL mappings

    date_day: timestamp_ntz - Date of the record
    gcp_project_id: varchar - GCP project identifier
    gcp_service_description: varchar - GCP service description
    gcp_sku_description: varchar - GCP SKU description
    infra_label: varchar - Infrastructure label
    env_label: varchar - Environment label
    runner_label: varchar - Runner label
    pl_category: varchar - Profit & Loss category
    pl_percent: float - Percentage of Profit & Loss category
    from_mapping: varchar - Source of mapping

{% enddocs %}

{% docs finops_pipeline_desc_haproxy_backend_pl %}

**haproxy_backend_pl**

    Mission: Maps each HAproxy backend to a specific P&L split
    Objective: Enable better allocation and reporting of infrastructure costs by pl_category.
    Granularity: N/A (mapping)
    Inputs: gcp_billing_haproxy_pl_mapping (csv seed)
    Accuracy rating: Medium
    Completeness rating: High

    METRIC_BACKEND: VARCHAR
    TYPE: VARCHAR
    ALLOCATION: FLOAT

{% enddocs %}

{% docs finops_pipeline_desc_haproxy_backend_ratio %}

**haproxy_backend_ratio_daily**

    Mission: Splits Networking costs into its different backends (SSH, HTTPs, ...)
    Objective: Enable better allocation and reporting of infrastructure costs by pl_category.
    Granularity: N/A (mapping)
    Inputs: Thanos HAproxy data, also visible [on Grafana](https://dashboards.gitlab.net/d/general-egress_ingress/general-network-ingress-egress-overview?orgId=1&from=1667956424979&to=1668115400979)
    Accuracy rating: Medium
    Completeness rating: High

    date_day: timestamp_ntz - Date of the record
    backend_category: varchar - Backend category identifier
    usage_ratio: float - Usage ratio for the backend

{% enddocs %}

{% docs finops_pipeline_desc_infralabel_pl %}

**infralabel_pl**

    Mission: Map infrastructure labels to Profit & Loss categories.
    Objective: Enable better allocation and reporting of infrastructure costs by pl_category.
    Granularity: N/A (mapping)
    Inputs: gcp_billing_infra_pl_mapping (csv seed)
    Accuracy rating: Medium
    Completeness rating: High

    infra_label: varchar - Infrastructure label
    type: varchar - Type of allocation
    allocation: float - Allocation value

{% enddocs %}

{% docs finops_pipeline_desc_projects_pl %}

**projects_pl**

    Mission: Map specific GCP projects to Profit & Loss categories.
    Objective: Provide accurate allocation and reporting of project costs by pl_category
    Granularity: N/A (mapping)
    Inputs: gcp_billing_project_pl_mapping (csv seed)
    Accuracy rating: High
    Completeness rating: High

    project_id: varchar - Project identifier
    type: varchar - Type of allocation
    allocation: number - Allocation value

{% enddocs %}

{% docs finops_pipeline_desc_sandbox_projects %}

**sandbox_projects_pl**

    Mission: Map sandbox projects to specific Profit & Loss categories.
    Objective: Provide accurate allocation and reporting of sandbox project costs by pl_category.
    Granularity: N/A (mapping)
    Inputs: gcp_billing_sandbox_projects (csv seed)
    Accuracy rating: Very High
    Completeness rating: Very High

    gcp_project_id: varchar - GCP project identifier
    classification: varchar - Classification category

{% enddocs %}

{% docs finops_pipeline_desc_single_sku_pl %}

**single_sku_pl**

    Mission: Map specific SKUs or Service-SKU combinations to Profit & Loss categories.
    Objective: Enable accurate allocation and reporting of specific costs by pl_category.
    Granularity: N/A (mapping)
    Inputs: gcp_billing_single_sku_pl_mapping (csv seed)
    Accuracy rating: Very High
    Completeness rating: Very High

    service_description: varchar - Service description
    sku_description: varchar - SKU description
    type: varchar - Type of allocation
    allocation: number - Allocation value

{% enddocs %}

{% docs finops_pipeline_desc_skus_day %}

**rpt_gcp_billing_skus_day**

    Mission: Map specific SKUs or Service-SKU combinations to Profit & Loss categories.
    Objective: Enable accurate allocation and reporting of specific costs by pl_category.
    Granularity: N/A (mapping)
    Inputs: gcp_billing_single_sku_pl_mapping (csv seed)
    Accuracy rating: Very High
    Completeness rating: Very High

    service_description: varchar - Service description
    sku_description: varchar - SKU description
    type: varchar - Type of allocation
    allocation: number - Allocation value

{% enddocs %}

{% docs wk_clickhouse_billing_usage_cost %}

The  model processes raw ClickHouse billing usage cost data.

**API Reference:** [ClickHouse Usage Cost API](https://clickhouse.com/docs/cloud/manage/api/swagger#/paths/~1v1~1organizations~1%7BorganizationId%7D~1usageCost/get)

{% enddocs %}

{% docs wk_clickhouse_billing_environment_name %}

Name of the environment where the usage occurred. This field identifies the specific environment context for the billing data.
Example:
1. Development
2. Dedicated
3. Production SaaS

{% enddocs %}

{% docs wk_clickhouse_billing_cost_date %}

The date when the cost was incurred. This represents the billing period date for the usage charges.

{% enddocs %}

{% docs wk_clickhouse_billing_data_warehouse_id %}

Unique identifier for the data warehouse instance that generated the costs. Used to track usage across different warehouse instances.

{% enddocs %}

{% docs wk_clickhouse_billing_index %}

Sequential index number from the flattened cost array. This field maintains the original order of cost records within each JSON payload.

{% enddocs %}

{% docs wk_clickhouse_billing_service_id %}

Identifier for the specific service that incurred the costs. Links costs to particular ClickHouse services or features.

{% enddocs %}

{% docs wk_clickhouse_billing_entity_type %}

Type classification of the entity that generated the costs (e.g., warehouse, database, user). Helps categorize different cost-generating resources.

{% enddocs %}

{% docs wk_clickhouse_billing_entity_id %}

Unique identifier for the specific entity that incurred the costs. Used for detailed cost attribution and tracking.

{% enddocs %}

{% docs wk_clickhouse_billing_entity_name %}

Human-readable name of the entity that generated the costs. Provides friendly identification of cost-generating resources.

{% enddocs %}

{% docs wk_clickhouse_billing_organization_tier %}

The organizational tier or plan level associated with the usage. Indicates the service level or pricing tier for the organization.

{% enddocs %}

{% docs wk_clickhouse_billing_total_chc %}

Total ClickHouse Credits (CHC) consumed for this specific cost record. Represents the primary billing unit for this usage instance.

{% enddocs %}

{% docs wk_clickhouse_billing_discount %}

Discount amount applied to the usage costs. Shows any promotional credits or discounts that reduced the total billing amount.

{% enddocs %}

{% docs wk_clickhouse_billing_inter_region_tier1_chc %}

ClickHouse Credits consumed for inter-region tier 1 data transfer. Represents costs for data movement between regions at the highest performance tier.

{% enddocs %}

{% docs wk_clickhouse_billing_inter_region_tier2_chc %}

ClickHouse Credits consumed for inter-region tier 2 data transfer. Represents costs for data movement between regions at the second performance tier.

{% enddocs %}

{% docs wk_clickhouse_billing_inter_region_tier3_chc %}

ClickHouse Credits consumed for inter-region tier 3 data transfer. Represents costs for data movement between regions at the third performance tier.

{% enddocs %}

{% docs wk_clickhouse_billing_inter_region_tier4_chc %}

ClickHouse Credits consumed for inter-region tier 4 data transfer. Represents costs for data movement between regions at the lowest performance tier.

{% enddocs %}

{% docs wk_clickhouse_billing_public_data_transfer_chc %}

ClickHouse Credits consumed for public data transfer operations. Covers costs for data egress to public internet or external systems.

{% enddocs %}

{% docs wk_clickhouse_billing_compute_chc %}

ClickHouse Credits consumed for compute operations. 

{% enddocs %}

{% docs wk_clickhouse_billing_grand_total_chc %}

Grand total ClickHouse Credits for the entire result set.

{% enddocs %}

{% docs wk_clickhouse_billing_currency %}

Currency for the calculation (`USD` in this case).

{% enddocs %}

{% docs wk_clickhouse_billing_uploaded_at %}

Timestamp when the billing data was uploaded to the system.

{% enddocs %}