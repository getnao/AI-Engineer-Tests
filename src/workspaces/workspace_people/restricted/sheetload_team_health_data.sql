
WITH source AS (
  SELECT *
  FROM {{ ref('sheetload_team_health_data_source') }}
),

team_member_directory_source AS (
    SELECT * 
    FROM {{ ref('mart_team_member_directory') }}
),

team_health_survey_data AS (
  SELECT
    timestamp::DATE                   AS survey_date_at,
    email_address::VARCHAR            AS email_address,
    question::VARCHAR                 AS survey_question,
    CASE
      WHEN question = 'Do you understand what results your team needs to deliver?' THEN 'survey_question_1'
      WHEN question = 'Does each member on your team understand what results they need to deliver?' THEN 'survey_question_2'
      WHEN question = 'Do the members of your team have the right skills/abilities to deliver results?' THEN 'survey_question_3'
      WHEN question = 'Does your team have the right resources to deliver results?' THEN 'survey_question_4'
      WHEN question = 'Is your team able to pivot and maintain results during time off (PTO, Extended Leave, Position Borrows, unplanned time away, etc.)?' THEN 'survey_question_5'
      WHEN question = 'Does your team have a shared system or tool used to monitor and communicate status updates?' THEN 'survey_question_6'
      WHEN question = 'Does your team demonstrate open and collaborative communication?' THEN 'survey_question_7'
      WHEN question = 'Does your team demonstrate effective cross-functional collaboration?' THEN 'survey_question_8'
    END                               AS survey_question_number,
    rating_description::VARCHAR       AS rating_description,
    rating_value::FLOAT              AS rating_value,
    TO_TIMESTAMP(_updated_at::NUMBER) AS uploaded_at
  FROM source
),

directory AS (
  SELECT
    employee_id,
    work_email,
    full_name,
    position,
    team_manager_name,
    department,
    division,
    cost_center,
    suporg,
    valid_from,
    valid_to
  FROM team_member_directory_source
)

SELECT 
    directory.employee_id,
    directory.full_name,
    directory.position,
    directory.team_manager_name AS manager_name,
    directory.department,
    directory.division,
    directory.cost_center,
    directory.suporg,
    team_health_survey_data.*
FROM team_health_survey_data

LEFT JOIN directory
    ON team_health_survey_data.email_address = directory.work_email
    AND team_health_survey_data.survey_date_at >= directory.valid_from
    AND team_health_survey_data.survey_date_at <= directory.valid_to
WHERE team_health_survey_data.survey_date_at <= CURRENT_DATE()

ORDER BY team_health_survey_data.survey_date_at, team_health_survey_data.email_address, team_health_survey_data.survey_question_number