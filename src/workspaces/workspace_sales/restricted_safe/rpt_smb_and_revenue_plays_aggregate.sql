{{ simple_cte([
    ('rpt_actuals','rpt_gtm_crm_actuals'),
    ('opportunity','mart_crm_opportunity'),
    ('targets','fct_sales_funnel_target_pivoted'),
    ('scaffold','rpt_gtm_scaffold'),
    ('user','dim_crm_user_hierarchy'),
    ('dim_date','dim_date')
]) }},

created_actuals AS (
    SELECT
        created_fiscal_quarter_name                                                                 AS fiscal_quarter_name,
        created_fiscal_quarter_date                                                                 AS fiscal_quarter_date,
        is_mid_market_plus,
        report_role_level_1,
        report_role_level_2,
        revenue_play,
        IFF(sales_qualified_source_name = 'Web Direct Generated', TRUE, FALSE)                     AS web_direct_flag,
        SUM(CASE WHEN is_net_arr_pipeline_created THEN new_logo_count END)                         AS pipeline_created_sao_count,
        SUM(CASE WHEN new_logo_count != 0 THEN booked_net_arr END)                                 AS net_arr_pipeline_created,
        SUM(CASE WHEN is_net_arr_pipeline_created
                AND day_of_fiscal_quarter <= current_day_of_fiscal_quarter THEN new_logo_count END) AS pipeline_created_sao_count_to_date,
        SUM(CASE WHEN new_logo_count != 0
                AND day_of_fiscal_quarter <= current_day_of_fiscal_quarter THEN booked_net_arr END) AS net_arr_pipeline_created_to_date,
        NULL                                                                                        AS booked_new_logo_count,
        NULL                                                                                        AS booked_net_arr,
        NULL                                                                                        AS booked_new_logo_count_to_date,
        NULL                                                                                        AS booked_net_arr_to_date
    FROM opportunity AS opportunity
    LEFT JOIN dim_date AS dim_date
        ON opportunity.close_date = dim_date.date_actual
    GROUP BY 1,2,3,4,5,6,7
),

booked AS (
    SELECT
        close_fiscal_quarter_name                                                                   AS fiscal_quarter_name,
        close_fiscal_quarter_date                                                                   AS fiscal_quarter_date,
        is_mid_market_plus,
        report_role_level_1,
        report_role_level_2,
        revenue_play,
        IFF(sales_qualified_source_name = 'Web Direct Generated', TRUE, FALSE)                     AS web_direct_flag,
        NULL                                                                                        AS pipeline_created_sao_count,
        NULL                                                                                        AS net_arr_pipeline_created,
        NULL                                                                                        AS pipeline_created_sao_count_to_date,
        NULL                                                                                        AS net_arr_pipeline_created_to_date,
        SUM(CASE WHEN fpa_master_bookings_flag THEN new_logo_count END)                            AS booked_new_logo_count,
        SUM(CASE WHEN new_logo_count != 0 THEN booked_net_arr END)                                 AS booked_net_arr,
        SUM(CASE WHEN fpa_master_bookings_flag
            AND day_of_fiscal_quarter <= current_day_of_fiscal_quarter THEN new_logo_count END)    AS booked_new_logo_count_to_date,
        SUM(CASE WHEN new_logo_count != 0
            AND day_of_fiscal_quarter <= current_day_of_fiscal_quarter THEN booked_net_arr END)    AS booked_net_arr_to_date
    FROM opportunity AS opportunity
    LEFT JOIN dim_date AS dim_date
        ON opportunity.close_date = dim_date.date_actual
    GROUP BY 1,2,3,4,5,6,7
)

SELECT * FROM created_actuals
UNION
SELECT * FROM booked