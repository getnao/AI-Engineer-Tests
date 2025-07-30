WITH internal_notes AS (
    SELECT *
    FROM {{ ref('internal_notes') }}
),
dim_note AS (
    SELECT *
    FROM {{ ref('dim_note') }}
),
internal_merge_request_diffs AS (
    SELECT *
    FROM {{ ref('internal_merge_request_diffs') }}
),
gitlab_dotcom_users AS (
    SELECT *
    FROM {{ ref('gitlab_dotcom_users') }}
),
dim_user AS (
    SELECT *
    FROM {{ ref('dim_user') }}
),
engineering_merge_requests AS (
    SELECT *
    FROM {{ ref('engineering_merge_requests') }}
),
dim_merge_request AS (
    SELECT *
    FROM {{ ref('dim_merge_request') }}
),
SIZES_PART_OF_PRODUCT_MERGE_REQUESTS AS (
    SELECT *
    FROM {{ ref('sizes_part_of_product_merge_requests') }}
),
-- Fetches notes related to review requests and removals from merge requests
review_notes AS (
    SELECT 
        internal_notes.note_id,
        internal_notes.noteable_id AS merge_request_id,
        internal_notes.created_at,
        internal_notes.note AS original_note,
        note.action_type
    FROM internal_notes
    JOIN dim_note AS note ON internal_notes.note_id = note.dim_note_id
    WHERE internal_notes.noteable_type = 'MergeRequest'
    AND (internal_notes.note LIKE 'requested review from%' OR internal_notes.note LIKE 'removed review request for%')
),
-- Extracts "requested" and "removed" segments separately from the note; You need primary_segment and secondary_segment since a single note could contain both
extract_review_sections AS ( 
    SELECT *,
        CASE
            WHEN original_note LIKE '%requested review%' AND original_note LIKE '%removed review%' THEN
                REGEXP_SUBSTR(original_note, 'requested review from @[a-zA-Z0-9._-]+(, @[a-zA-Z0-9._-]+)*(,? and @[a-zA-Z0-9._-]+)?')
            WHEN original_note LIKE '%requested review%' THEN
                REGEXP_SUBSTR(original_note, 'requested review from @[a-zA-Z0-9._-]+(, @[a-zA-Z0-9._-]+)*(,? and @[a-zA-Z0-9._-]+)?')
            ELSE NULL
        END AS primary_segment,
        
        CASE
            WHEN original_note LIKE '%requested review%' AND original_note LIKE '%removed review%' THEN
                REGEXP_SUBSTR(original_note, 'removed review request for @[a-zA-Z0-9._-]+(, @[a-zA-Z0-9._-]+)*(,? and @[a-zA-Z0-9._-]+)?')
            WHEN original_note LIKE '%removed review%' THEN
                REGEXP_SUBSTR(original_note, 'removed review request for @[a-zA-Z0-9._-]+(, @[a-zA-Z0-9._-]+)*(,? and @[a-zA-Z0-9._-]+)?')
            ELSE NULL
        END AS secondary_segment
    FROM review_notes
),
-- Splits out the segments into separate rows for each "requested" and "removed" segment
separated_actions AS (
    SELECT *, primary_segment AS segment 
    FROM extract_review_sections 
    WHERE primary_segment IS NOT NULL
    UNION ALL
    SELECT *, secondary_segment AS segment 
    FROM extract_review_sections 
    WHERE secondary_segment IS NOT NULL
),
-- Labels each segment as either a 'request' or 'remove'
labeled_actions AS (
    SELECT *,
        CASE
            WHEN segment LIKE 'requested%' THEN 'request'
            WHEN segment LIKE 'removed%' THEN 'remove'
            ELSE NULL
        END AS action
    FROM separated_actions
),
-- Extracts all @user mentions from each segment
extract_usernames AS (
    SELECT *,
        REGEXP_SUBSTR_ALL(segment, '@([a-zA-Z0-9._-]+)') AS all_usernames
    FROM labeled_actions
),
-- Flattens the array of usernames into individual rows
flattened_usernames AS (
    SELECT 
        *, 
        REPLACE(value, '@', '') AS username
    FROM extract_usernames,
         LATERAL FLATTEN(input => all_usernames)
),
-- Removes users who appear in both "request" and "remove" actions for a given merge request; This was built to remove users who were mistakenly added as a reviewer.
deduped_users AS (
    
    SELECT 
        merge_request_id,
        username,
        COUNT(DISTINCT action) AS action_count
    FROM flattened_usernames
    GROUP BY merge_request_id, username
    HAVING action_count = 1
),
-- Appends user_id and retrieves the first time a review was requested for each user on a merge request
review_history AS (
    SELECT 
        flattened_usernames.merge_request_id, 
        flattened_usernames.username, 
        gitlab_dotcom_users.user_id, 
        CASE WHEN deduped_users.username IS NOT NULL THEN 0 ELSE 1 END AS reviewer_removed_flag,
        min(flattened_usernames.created_at) AS review_requested_at
    FROM flattened_usernames
    JOIN gitlab_dotcom_users ON flattened_usernames.username = gitlab_dotcom_users.user_name
    LEFT JOIN deduped_users ON flattened_usernames.merge_request_id = deduped_users.merge_request_id
    AND flattened_usernames.username = deduped_users.username
    WHERE action = 'request'
    GROUP BY ALL
),
-- Retrieves commit information for each merge request, including timezone of commit creation
commits AS (
    SELECT 
        merge_request_id,
        commits_count,
        max(created_at) AS commit_created_at,
        CASE 
            WHEN (hour(commit_created_at) = 5 AND minute(commit_created_at) >= 30) 
                 OR hour(commit_created_at) BETWEEN 6 AND 7 THEN 'APAC'
            WHEN hour(commit_created_at) BETWEEN 8 AND 15 THEN 'AMER'
            ELSE 'EMEA'
        END AS commit_timezone,
        row_number() OVER (PARTITION BY merge_request_id ORDER BY commit_created_at DESC) AS rn
    FROM internal_merge_request_diffs
    WHERE diff_type = 1
    AND commits_count > 0
    GROUP BY ALL
),
-- Retrieves basic information about merge requests, including author, project, severity, labels, etc.
mrs AS (
    SELECT DISTINCT 
        engineering_merge_requests.merge_request_id,
        engineering_merge_requests.author_id,
        engineering_merge_requests.project_id,
        engineering_merge_requests.target_project_id,
        engineering_merge_requests.created_at,
        engineering_merge_requests.merged_at,
        dim_merge_request.latest_closed_at AS closed_at,
        COALESCE(engineering_merge_requests.merged_at,dim_merge_request.latest_closed_at) AS merged_or_closed_at,
        engineering_merge_requests.merge_month,
        engineering_merge_requests.merge_request_title,
        engineering_merge_requests.labels,
        engineering_merge_requests.masked_label_title,
        engineering_merge_requests.is_community_contribution,
        engineering_merge_requests.priority_label,
        engineering_merge_requests.severity_label,
        engineering_merge_requests.group_label,
        engineering_merge_requests.section_label,
        engineering_merge_requests.stage_label,
        engineering_merge_requests.url,
        engineering_merge_requests.type_label,
        engineering_merge_requests.subtype_label,
        engineering_merge_requests.milestone_title,
        engineering_merge_requests.milestone_start_date,
        engineering_merge_requests.milestone_due_date,
        mrsz.product_merge_request_lines_added,
        mrsz.product_merge_request_lines_removed,
        dim_user.user_name AS author_user_name,
        array_contains('deliverable'::variant, engineering_merge_requests.labels) AS is_deliverable
    FROM engineering_merge_requests
    LEFT JOIN SIZES_PART_OF_PRODUCT_MERGE_REQUESTS mrsz
        ON mrsz.product_merge_request_iid = engineering_merge_requests.merge_request_iid
        AND mrsz.PRODUCT_MERGE_REQUEST_PROJECT_ID = engineering_merge_requests.target_project_id
    LEFT JOIN dim_user ON engineering_merge_requests.author_id = dim_user.user_id
        AND dim_user.user_type != 'Duo Code Review Bot'
    LEFT JOIN dim_merge_request ON dim_merge_request.merge_request_id = engineering_merge_requests.merge_request_id
        AND dim_merge_request.merge_request_state = 'closed'
    WHERE COALESCE(engineering_merge_requests.merge_month, dim_merge_request.latest_closed_at) >= DATEADD('month', -13, DATE_TRUNC('month', CURRENT_DATE))
),
-- Fetches notes related to approvals for merge requests
dim_note_approval AS (
    SELECT *
    FROM dim_note
    WHERE noteable_type = 'MergeRequest'
        AND action_type = 'approved'
),
-- Retrieves the first engagement for each merge request by filtering for the first note per reviewer
notes AS (
    SELECT 
        internal_notes.note_id,
        internal_notes.note,
        internal_notes.noteable_id AS merge_request_id,
        internal_notes.note_author_id,
        internal_notes.created_at AS note_created_at,
        min(note_created_at) OVER (PARTITION BY merge_request_id) AS first_engagement_per_mr,
        CASE 
            WHEN (hour(note_created_at) = 5 AND minute(note_created_at) >= 30) 
                 OR hour(note_created_at) BETWEEN 6 AND 7 THEN 'APAC'
            WHEN hour(note_created_at) BETWEEN 8 AND 15 THEN 'AMER'
            ELSE 'EMEA'
        END AS notes_timezone
    FROM internal_notes
    JOIN review_history ON internal_notes.noteable_id = review_history.merge_request_id
        AND review_history.user_id = internal_notes.note_author_id
        AND internal_notes.created_at > review_history.review_requested_at
    WHERE internal_notes.noteable_type = 'MergeRequest'
    QUALIFY row_number() OVER (
            PARTITION BY internal_notes.noteable_id, internal_notes.note_author_id
            ORDER BY internal_notes.created_at
        ) = 1
),
-- Combines all timezones (commits and notes) for each merge request
all_timezones AS (
    SELECT merge_request_id, notes_timezone AS timezone FROM notes
    UNION ALL
    SELECT merge_request_id, commit_timezone AS timezone FROM commits
),
-- Aggregates timezones per merge request to check if multiple timezones were involved
timezones_per_mr AS (
    SELECT 
        merge_request_id,
        listagg(DISTINCT timezone, ',') AS timezones,
        array_size(split(timezones, ',')) > 1 AS multi_timezones
    FROM all_timezones
    GROUP BY ALL
),
-- Joins all previous CTEs and combines relevant data per merge request
combined AS (
    SELECT 
        mrs.*,
        review_history.username,
        review_history.user_id,
        review_history.review_requested_at,
        review_history.reviewer_removed_flag,
        notes.note_id,
        notes.note,
        notes.note_author_id,
        notes.note_created_at,
        notes.first_engagement_per_mr,
        timezones_per_mr.timezones,
        timezones_per_mr.multi_timezones,
        min(dim_note_approval.created_at) AS approval_created_at,
        min(iff(commits.rn = 1, commit_created_at, NULL)) AS latest_commit_created_at,
        min(iff(commits.rn = 1, commits_count, NULL)) - min(
            iff(
                commits.commit_created_at >= first_engagement_per_mr,
                commits.commits_count,
                NULL
            )
        ) + 1 AS commits_after_first_engagement
    FROM mrs
    LEFT JOIN review_history ON mrs.merge_request_id = review_history.merge_request_id
    LEFT JOIN notes ON review_history.merge_request_id = notes.merge_request_id
        AND review_history.user_id = notes.note_author_id
    LEFT JOIN dim_note_approval ON review_history.merge_request_id = dim_note_approval.noteable_id
        AND review_history.user_id = dim_note_approval.author_id
    LEFT JOIN commits ON mrs.merge_request_id = commits.merge_request_id
    LEFT JOIN timezones_per_mr ON mrs.merge_request_id = timezones_per_mr.merge_request_id
    GROUP BY ALL
),
FINAL AS (
    SELECT 
        combined.*, 
        -- Review flag: 1 if there is any engagement (note author and note creation date are not NULL)
        CASE
            WHEN note_author_id || note_created_at IS NOT NULL THEN 1
            ELSE 0
        END AS review_flag,
        -- Review assign flag: 1 if the username is not NULL, indicating the reviewer was assigned
        CASE
            WHEN username IS NOT NULL THEN 1
            ELSE 0
        END AS review_assign_flag,
        -- Time from review request to the first comment in hours
        datediff('minute', review_requested_at, note_created_at) / 60 AS hours_from_request_to_comment,
        -- Time from approval to merge in hours
        datediff('minute', approval_created_at, merged_at) / 60 AS hours_from_approval_to_merge,
        -- Time from review request to merge in hours
        datediff('minute', review_requested_at, merged_at) / 60 AS hours_from_request_to_merge,
        -- MR lifecycle: Time from creation to first engagement in hours
        datediff('minute', created_at, first_engagement_per_mr) / 60 AS hours_from_creation_to_first_engagement,
        -- Time from first engagement to final commit in hours
        datediff('minute', first_engagement_per_mr, latest_commit_created_at) / 60 AS hours_from_first_engagement_to_final_commit,
        -- Time from final commit to merge in hours
        datediff('minute', latest_commit_created_at, merged_at) / 60 AS hours_from_final_commit_to_merge,
        -- Time from creation to merge in days
        datediff('day', created_at, merged_at) AS days_from_creation_to_merge
    FROM combined
)

SELECT *
FROM FINAL