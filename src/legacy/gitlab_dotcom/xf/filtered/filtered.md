{% docs temp_gitlab_dotcom_notes_filtered %}

This model contains a filtered subset of GitLab.com notes focusing specifically on issues and merge requests for performance optimization. It incrementally processes notes data, keeping only the most recent 25 months of notes on specific notable types. The model extracts minimal metadata about each note to support downstream analytics efficiently without unnecessary columns. This lightweight, filtered table improves query performance for downstream models that only need access to specific note types.

{% enddocs %}


{% docs temp_gitlab_dotcom_events_filtered %}

This model contains a filtered subset of GitLab.com events focused on specific action types and targets for performance optimization. It incrementally processes events data, keeping only the most recent 25 months of activity for relevant event types. The model specifically extracts code pushes, design management actions, and wiki page modifications to support specialized reporting needs. This lightweight, filtered table improves query performance for downstream models that only need access to specific event types.

{% enddocs %}
