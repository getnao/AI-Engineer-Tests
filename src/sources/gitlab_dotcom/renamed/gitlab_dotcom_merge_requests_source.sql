WITH all_merge_requests AS (

  SELECT

    id::NUMBER                                         AS merge_request_id,
    iid::NUMBER                                        AS merge_request_iid,
    title::VARCHAR                                     AS merge_request_title,

    IFF(LOWER(merge_error) = 'nan', NULL, merge_error) AS merge_error,
    assignee_id::NUMBER                                AS assignee_id,
    updated_by_id::NUMBER                              AS updated_by_id,
    merge_user_id::NUMBER                              AS merge_user_id,
    last_edited_by_id::NUMBER                          AS last_edited_by_id,
    milestone_id::NUMBER                               AS milestone_id,
    head_pipeline_id::NUMBER                           AS head_pipeline_id,
    latest_merge_request_diff_id::NUMBER               AS latest_merge_request_diff_id,
    approvals_before_merge::NUMBER                     AS approvals_before_merge,
    lock_version::NUMBER                               AS lock_version,
    time_estimate::NUMBER                              AS time_estimate,
    source_project_id::NUMBER                          AS project_id,
    target_project_id::NUMBER                          AS target_project_id,
    author_id::NUMBER                                  AS author_id,
    state_id::NUMBER                                   AS merge_request_state_id,
    -- Override state by mapping state_id. See issue #3556.
    {{ map_state_id('state_id') }}                                                AS merge_request_state,
    merge_status                                       AS merge_request_status,
    merge_when_pipeline_succeeds::BOOLEAN              AS does_merge_when_pipeline_succeeds,
    squash::BOOLEAN                                    AS does_squash,
    discussion_locked::BOOLEAN                         AS is_discussion_locked,
    allow_maintainer_to_push::BOOLEAN                  AS does_allow_maintainer_to_push,
    created_at::TIMESTAMP                              AS created_at,
    updated_at::TIMESTAMP                              AS updated_at,
    last_edited_at::TIMESTAMP                          AS merge_request_last_edited_at,
    description::VARCHAR                               AS merge_request_description,
    merge_commit_sha::VARCHAR                          AS merge_commit_sha,
    rebase_commit_sha::VARCHAR                         AS rebase_commit_sha,
    sprint_id::NUMBER                                  AS sprint_id,
    draft::BOOLEAN                                     AS draft,
    prepared_at::TIMESTAMP                             AS prepared_at,
    imported_from::NUMBER                              AS imported_from,
    retargeted::BOOLEAN                                AS retargeted,
    override_requested_changes::BOOLEAN                AS override_requested_changes,
    pgp_is_deleted::BOOLEAN                            AS is_deleted,
    pgp_is_deleted_updated_at::TIMESTAMP               AS is_deleted_updated_at

  --merge_params // hidden for privacy

  FROM {{ ref('gitlab_dotcom_merge_requests_dedupe_source') }}

),

internal_merge_requests AS (

  SELECT

    id::NUMBER                                                   AS internal_merge_request_id,
    title::VARCHAR                                               AS internal_merge_request_title,
    description::VARCHAR                                         AS internal_merge_request_description,
    target_branch::VARCHAR                                       AS internal_target_branch,
    IFF(LOWER(target_branch) IN ('master', 'main'), TRUE, FALSE) AS is_merge_to_master

  FROM {{ ref('gitlab_dotcom_merge_requests_internal_only_dedupe_source') }}
),

joined AS (

  SELECT

    all_merge_requests.merge_request_id,
    all_merge_requests.merge_request_iid,
    internal_merge_requests.internal_merge_request_title       AS merge_request_title,
    internal_merge_requests.internal_merge_request_description AS merge_request_description,
    internal_merge_requests.internal_target_branch             AS target_branch,
    internal_merge_requests.is_merge_to_master,
    all_merge_requests.merge_error,
    all_merge_requests.assignee_id,
    all_merge_requests.updated_by_id,
    all_merge_requests.merge_user_id,
    all_merge_requests.last_edited_by_id,
    all_merge_requests.milestone_id,
    all_merge_requests.head_pipeline_id,
    all_merge_requests.latest_merge_request_diff_id,
    all_merge_requests.approvals_before_merge,
    all_merge_requests.lock_version,
    all_merge_requests.time_estimate,
    all_merge_requests.project_id,
    all_merge_requests.target_project_id,
    all_merge_requests.author_id,
    all_merge_requests.merge_request_state_id,
    all_merge_requests.merge_request_state,
    all_merge_requests.merge_request_status,
    all_merge_requests.does_merge_when_pipeline_succeeds,
    all_merge_requests.does_squash,
    all_merge_requests.is_discussion_locked,
    all_merge_requests.does_allow_maintainer_to_push,
    all_merge_requests.created_at,
    all_merge_requests.updated_at,
    all_merge_requests.merge_request_last_edited_at,
    all_merge_requests.merge_commit_sha,
    all_merge_requests.rebase_commit_sha,
    all_merge_requests.sprint_id,
    all_merge_requests.draft,
    all_merge_requests.prepared_at,
    all_merge_requests.imported_from,
    all_merge_requests.retargeted,
    all_merge_requests.override_requested_changes,
    all_merge_requests.is_deleted,
    all_merge_requests.is_deleted_updated_at

  FROM all_merge_requests
  LEFT JOIN internal_merge_requests
    ON all_merge_requests.merge_request_id = internal_merge_requests.internal_merge_request_id

)

SELECT *
FROM joined
