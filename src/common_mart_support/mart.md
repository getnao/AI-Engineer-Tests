{% docs rpt_top_issues_by_arr %}

This report is at the issue grain and ranks issues in terms of the ARR represented by the accounts that mention those issues in Zendesk tickets.

The model identifies GitLab engineering issues (from gitlab-org projects) that are referenced in customer support tickets, 
then ranks them by the total ARR of customers who mentioned each issue. Only non-confidential issues are included in the final ranking.

Key filters applied:
- Only tickets from last 'ticket_lookback_days', (default 60) days
- Only tickets with gitlab-org issue tags
- Only active subscriptions for the current fiscal year
- Excludes confidential engineering issues

{% enddocs %}
