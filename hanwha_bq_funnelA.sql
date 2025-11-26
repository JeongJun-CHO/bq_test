WITH base AS (
  SELECT
    -- 퍼널 레코드 고유 키
    CONCAT(
      SAFE_CAST(event_timestamp AS STRING),
      user_pseudo_id,
      COALESCE(
        SAFE_CAST(
          (SELECT value.int_value
           FROM UNNEST(event_params)
           WHERE key = 'ga_session_id') AS STRING
        ),
        ''
      )
    ) AS funnel_key,

    -- 사용자 / 세션 키
    CONCAT(user_pseudo_id, COALESCE(user_id, '')) AS user_key,
    CONCAT(
      user_pseudo_id,
      COALESCE(
        SAFE_CAST(
          (SELECT value.int_value
           FROM UNNEST(event_params)
           WHERE key = 'ga_session_id') AS STRING
        ),
        ''
      )
    ) AS session_key,

    -- 페이지 키
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

    -- 기본 이벤트 정보
    event_timestamp,
    event_name,

    -- 페이지 타이틀 및 분해
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title')
      AS page_title,
    SPLIT(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title'),
      '>'
    ) AS title_parts

  FROM
    `{{ 테이블 }}`
)

SELECT
  funnel_key,
  user_key,
  session_key,
  page_key,
  event_timestamp,

  -- 보험명 (2뎁스)
  TRIM(title_parts[SAFE_OFFSET(1)]) AS insurance_name,

  -- 퍼널 스텝 (예: 1.3중 '1')
  COALESCE(
    REGEXP_EXTRACT(
      title_parts[SAFE_OFFSET(3)],
      r'^([0-9]+)\.'
    ),
    '0'
  ) AS funnel_step,

  -- 퍼널 스텝 디테일 (예: 1.3중 '3')
  COALESCE(
    REGEXP_EXTRACT(
      title_parts[SAFE_OFFSET(3)],
      r'^[0-9]+\.([0-9]+)'
    ),
    '0'
  ) AS funnel_step_detail,

  -- 퍼널 스텝 이름 (예: '1.3테스트' → '테스트')
  TRIM(
    REGEXP_EXTRACT(
      title_parts[SAFE_OFFSET(3)],
      r'^[0-9]+\.[0-9]+(.*)$'
    )
  ) AS funnel_step_name,

  event_name,
  page_title,

  -- 마트 생성 날짜
  CURRENT_DATE() AS current_date

FROM
  base
WHERE
  page_title LIKE '%청약진행%';
