WITH base AS (
  SELECT
    -- 페이지 고유키
    CONCAT(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title'),
      user_pseudo_id,
      COALESCE(
        SAFE_CAST(
          (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING
        ),
        ''
      ),
      SAFE_CAST(event_timestamp AS STRING)
    ) AS page_key,

    -- 사용자 및 세션 키
    CONCAT(user_pseudo_id, COALESCE(user_id, '')) AS user_key,
    CONCAT(
      user_pseudo_id,
      COALESCE(
        SAFE_CAST(
          (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING
        ),
        ''
      )
    ) AS session_key,

    -- 타임스탬프
    event_timestamp,

    -- 페이지 타이틀 및 URL 관련
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT REGEXP_EXTRACT(value.string_value, r'^[^?#]+')
     FROM UNNEST(event_params)
     WHERE key = 'page_location'
    ) AS page_path,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS page_referrer,

    -- 유입 정보
    session_traffic_source_last_click. manual_campaign.source         AS traffic_source,
    session_traffic_source_last_click. manual_campaign.medium         AS traffic_medium,
    session_traffic_source_last_click. manual_campaign.campaign_name  AS traffic_campaign,
    session_traffic_source_last_click. manual_campaign.content        AS traffic_content,
    session_traffic_source_last_click. manual_campaign.term           AS traffic_term,

    -- 사용자 속성
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'up_platform_Channel')
      AS platform_channel

  FROM
    `{{ 테이블 }}`
  WHERE
    event_name = 'page_view'
),

-- 페이지 타이틀 및 URL 분해
base_with_split AS (
  SELECT
    b.*,
    SPLIT(page_title, '>') AS title_parts,
    COALESCE(
      REGEXP_EXTRACT(page_path, r'^https?://[^/]+(/[^?#]*)'),
      REGEXP_EXTRACT(page_path, r'^(/[^?#]*)'),
      '/'
    ) AS clean_path
  FROM base b
),

-- URL path segment 배열 생성
base_with_both AS (
  SELECT
    *,
    REGEXP_EXTRACT_ALL(clean_path, r'[^/]+') AS path_parts
  FROM base_with_split
)

SELECT
  page_key,
  user_key,
  session_key,
  event_timestamp,
  page_title,
  page_location,
  page_path,
  page_referrer,

  --------------------------------------------------------------------
  -- 🔹 세션 내 페이지뷰 순번
  --------------------------------------------------------------------
  ROW_NUMBER() OVER (
    PARTITION BY session_key
    ORDER BY event_timestamp
  ) AS page_order,

  --------------------------------------------------------------------
  -- 🔹 페이지 타이틀 뎁스 (">" 기준)
  --------------------------------------------------------------------
  TRIM(title_parts[SAFE_OFFSET(0)]) AS page_title_1depth,

  CASE WHEN ARRAY_LENGTH(title_parts) >= 2 THEN
    ARRAY_TO_STRING([
      TRIM(title_parts[OFFSET(0)]),
      TRIM(title_parts[OFFSET(1)])
    ], '>')
  END AS page_title_2depth,

  CASE WHEN ARRAY_LENGTH(title_parts) >= 3 THEN
    ARRAY_TO_STRING([
      TRIM(title_parts[OFFSET(0)]),
      TRIM(title_parts[OFFSET(1)]),
      TRIM(title_parts[OFFSET(2)])
    ], '>')
  END AS page_title_3depth,

  CASE WHEN ARRAY_LENGTH(title_parts) >= 4 THEN
    ARRAY_TO_STRING(
      ARRAY(SELECT TRIM(part) FROM UNNEST(title_parts) AS part),
      '>'
    )
  END AS page_title_4depth,

  --------------------------------------------------------------------
  -- 🔹 URL path 뎁스 ("/" 기준)
  --------------------------------------------------------------------
  CASE WHEN ARRAY_LENGTH(path_parts) >= 1 THEN '/' || path_parts[OFFSET(0)] END AS page_loc_1depth,

  CASE WHEN ARRAY_LENGTH(path_parts) >= 2 THEN
    '/' || path_parts[OFFSET(0)] || '/' || path_parts[OFFSET(1)]
  END AS page_loc_2depth,

  CASE WHEN ARRAY_LENGTH(path_parts) >= 3 THEN
    '/' || path_parts[OFFSET(0)] || '/' || path_parts[OFFSET(1)] || '/' || path_parts[OFFSET(2)]
  END AS page_loc_3depth,

  CASE WHEN ARRAY_LENGTH(path_parts) >= 4 THEN clean_path END AS page_loc_4depth,

  --------------------------------------------------------------------
  -- 🔹 페이지 체류 시간 계산
  --------------------------------------------------------------------
  CASE
    WHEN COUNT(*) OVER (PARTITION BY session_key) = 1 THEN 0
    WHEN LEAD(event_timestamp) OVER (
      PARTITION BY session_key ORDER BY event_timestamp
    ) IS NULL THEN -1
    ELSE SAFE_DIVIDE(
      LEAD(event_timestamp) OVER (
        PARTITION BY session_key ORDER BY event_timestamp
      ) - event_timestamp,
      1000000.0
    )
  END AS page_stay_seconds,

  --------------------------------------------------------------------
  -- 🔹 방문/이탈 판단
  --------------------------------------------------------------------
  CASE
    WHEN ROW_NUMBER() OVER (PARTITION BY session_key ORDER BY event_timestamp) = 1
    THEN 'Y' ELSE 'N'
  END AS is_landing_page,

  CASE
    WHEN COUNT(*) OVER (PARTITION BY session_key) = 1 THEN 'N'
    WHEN LEAD(event_timestamp) OVER (
      PARTITION BY session_key ORDER BY event_timestamp
    ) IS NULL THEN 'Y'
    ELSE 'N'
  END AS is_last_page,

  --------------------------------------------------------------------
  -- 🔹 유입 정보 + 마트 날짜
  --------------------------------------------------------------------
  session_source,
  session_medium,
  session_campaign,
  session_term,
  session_content,
  platform_channel,
  CURRENT_DATE() AS current_date

FROM base_with_both;
