WITH base AS (
  SELECT
    CONCAT(
      SAFE_CAST(event_timestamp AS STRING),
      user_pseudo_id, 
      COALESCE(SAFE_CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING), '')
    ) AS funnel_key,

    CONCAT(user_pseudo_id, COALESCE(user_id, '')) AS user_key,

    CONCAT(
      user_pseudo_id, 
      COALESCE(SAFE_CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING), '')
    ) AS session_key,

    CONCAT(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title'),
      user_pseudo_id, 
      COALESCE(SAFE_CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING), ''),
      SAFE_CAST(event_timestamp AS STRING)
    ) AS page_key,

    event_timestamp,
    event_name,

    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title,

    SPLIT(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title'),
      '>'
    ) AS title_parts

  FROM
    `miteam-186206.analytics_290055093.events_20251025`
)

SELECT
  funnel_key,
  user_key,
  session_key,
  page_key,
  event_timestamp,

  TRIM(title_parts[SAFE_OFFSET(1)]) AS insurance_name,

  COALESCE(
    REGEXP_EXTRACT(
      title_parts[SAFE_OFFSET(3)],
      r'^([0-9]+)\.'
    ),
    "0"
  ) AS funnel_step,

  COALESCE(
    REGEXP_EXTRACT(
      title_parts[SAFE_OFFSET(3)],
      r'^[0-9]+\.([0-9]+)'
    ),
    "0"
  ) AS funnel_step_detail,

  TRIM(
    REGEXP_EXTRACT(
      title_parts[SAFE_OFFSET(3)],
      r'^[0-9]+\.[0-9]+(.*)$'
    )
  ) AS funnel_step_name,

  event_name,
  page_title,
  CURRENT_DATE() AS current_date

FROM
  base
WHERE 
  page_title LIKE '%청약진행%'

