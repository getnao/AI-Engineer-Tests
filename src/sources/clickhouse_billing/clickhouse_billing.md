{% docs clickhouse_billing_usage_cost %}

The  model processes raw ClickHouse billing usage cost data.

**API Reference:** [ClickHouse Usage Cost API](https://clickhouse.com/docs/cloud/manage/api/swagger#/paths/~1v1~1organizations~1%7BorganizationId%7D~1usageCost/get)

{% enddocs %}

{% docs clickhouse_billing_environment_name %}

Name of the environment where the usage occurred. This field identifies the specific environment context for the billing data.
Example:
1. Development
2. Dedicated
3. Production SaaS

{% enddocs %}

{% docs clickhouse_billing_cost_date %}

The date when the cost was incurred. This represents the billing period date for the usage charges.

{% enddocs %}

{% docs clickhouse_billing_data_warehouse_id %}

Unique identifier for the data warehouse instance that generated the costs. Used to track usage across different warehouse instances.

{% enddocs %}

{% docs clickhouse_billing_index %}

Sequential index number from the flattened cost array. This field maintains the original order of cost records within each JSON payload.

{% enddocs %}

{% docs clickhouse_billing_service_id %}

Identifier for the specific service that incurred the costs. Links costs to particular ClickHouse services or features.

{% enddocs %}

{% docs clickhouse_billing_entity_type %}

Type classification of the entity that generated the costs (e.g., warehouse, database, user). Helps categorize different cost-generating resources.

{% enddocs %}

{% docs clickhouse_billing_entity_id %}

Unique identifier for the specific entity that incurred the costs. Used for detailed cost attribution and tracking.

{% enddocs %}

{% docs clickhouse_billing_entity_name %}

Human-readable name of the entity that generated the costs. Provides friendly identification of cost-generating resources.

{% enddocs %}

{% docs clickhouse_billing_organization_tier %}

The organizational tier or plan level associated with the usage. Indicates the service level or pricing tier for the organization.

{% enddocs %}

{% docs clickhouse_billing_total_chc %}

Total ClickHouse Credits (CHC) consumed for this specific cost record. Represents the primary billing unit for this usage instance.

{% enddocs %}

{% docs clickhouse_billing_discount %}

Discount amount applied to the usage costs. Shows any promotional credits or discounts that reduced the total billing amount.

{% enddocs %}

{% docs clickhouse_billing_inter_region_tier1_chc %}

ClickHouse Credits consumed for inter-region tier 1 data transfer. Represents costs for data movement between regions at the highest performance tier.

{% enddocs %}

{% docs clickhouse_billing_inter_region_tier2_chc %}

ClickHouse Credits consumed for inter-region tier 2 data transfer. Represents costs for data movement between regions at the second performance tier.

{% enddocs %}

{% docs clickhouse_billing_inter_region_tier3_chc %}

ClickHouse Credits consumed for inter-region tier 3 data transfer. Represents costs for data movement between regions at the third performance tier.

{% enddocs %}

{% docs clickhouse_billing_inter_region_tier4_chc %}

ClickHouse Credits consumed for inter-region tier 4 data transfer. Represents costs for data movement between regions at the lowest performance tier.

{% enddocs %}

{% docs clickhouse_billing_public_data_transfer_chc %}

ClickHouse Credits consumed for public data transfer operations. Covers costs for data egress to public internet or external systems.

{% enddocs %}

{% docs clickhouse_billing_compute_chc %}

ClickHouse Credits consumed for compute operations. 

{% enddocs %}

{% docs clickhouse_billing_grand_total_chc %}

Grand total ClickHouse Credits for the entire result set.

{% enddocs %}

{% docs clickhouse_billing_currency %}

Currency for the calculation (`USD` in this case).

{% enddocs %}

{% docs clickhouse_billing_uploaded_at %}

Timestamp when the billing data was uploaded to the system.

{% enddocs %}