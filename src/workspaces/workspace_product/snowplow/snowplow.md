{% docs wk_rpt_daily_code_suggestion_metrics %}

This table tracks daily metrics for both code suggestion events and all behavior events.
For code suggestions, it focuses specifically on AI Gateway requests by filtering for
feature_category = 'code_suggestions' and app_id = 'gitlab_ai_gateway' from the 
mart_behavior_structured_event_ai_gateway_flattened table. The table also includes
metrics for all events from the mart_behavior_structured_event table.

IMPORTANT DATA TIMELINE NOTES:
1. All behavior event data exists since 2021-03-15
2. Code suggestion data exists only since 2024-08-03
3. The code suggestion event data underwent a significant change on 2024-11-12. Prior to this date,
  a single 'request_code_suggestions' action was used. After this date, the team split this
  into two smaller unit primitives: 'request_complete_code' and 'request_generate_code'.
  This change was implemented in MR: 
  https://gitlab.com/gitlab-org/modelops/applied-ml/code-suggestions/ai-assist/-/merge_requests/1557

The metrics are segmented in these ways:
1. All behavior events (total_all_events, peak_all_events_per_second)
2. All code suggestion events (total_code_suggestion_events, peak_code_suggestion_events_per_second)
3. Generate code events (request_generate_code_events, peak_generate_code_events_per_second)
4. Complete code events (request_complete_code_events, peak_complete_code_events_per_second)

Note that after 2024-11-12, 'request_complete_code_events' + 'request_generate_code_events' should be equal to 'total_code_suggestion_events', as these two event types replaced the original 'request_code_suggestions' action

{% enddocs %}