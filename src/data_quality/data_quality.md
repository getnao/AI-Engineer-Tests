{% docs data_detection_rule %}

A table to hold all the data quality detection rules. 

The data quality detection rules need to be manually added to the table.

{% enddocs %}

{% docs product_data_detection_run_detail %}

A table with all the run details of the data quality detection rules. 

This is an incremental model that contains the total number of processed records, passed records and failed records based on the data detection rules.

{% enddocs %}

{% docs product_data_detection_run_result %}

A table that holds the run results of data quality detection rules. 

This table includes a Flag to indicate if the Detection rule passed or failed based on the threshold value.

{% enddocs %}

{% docs data_detection_scorecard %}

A table that holds the name along with the purpose and release information of all the available Scorcards for different subject areas. 

{% enddocs %}

{% docs stale_dev_db_tables %}

This model identifies stale tables in developer databases within Snowflake that haven't been altered for at least 80 days. It joins information from Snowflake databases, tables, and users sources to identify tables owned by individual developers rather than system accounts. The model helps with data governance by tracking unused or abandoned developer tables that may be candidates for cleanup. Each record contains detailed information about the table, its creation and last altered dates, and links to the GitLab team member who owns it.


{% enddocs %}
