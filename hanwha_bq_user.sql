WITH temp AS (
  SELECT
    -- 키/식별자
    CONCAT(user_pseudo_id, COALESCE(user_id, '')) AS user_key,
    user_pseudo_id AS client_id,
    user_id AS user_id,

    -- 사용자 속성
    (
      SELECT value.string_value
      FROM UNNEST(user_properties)
      WHERE key = 'up_mid'
    ) AS mid,

    (
      SELECT value.string_value
      FROM UNNEST(user_properties)
      WHERE key = 'up_platform_Channel'
    ) AS platform_channel,

    (
      SELECT value.string_value
      FROM UNNEST(user_properties)
      WHERE key = 'up_age'
    ) AS age,

    (
      SELECT value.string_value
      FROM UNNEST(user_properties)
      WHERE key = 'up_gender'
    ) AS gender,

    -- 기본 디바이스/유입 정보
    device.operating_system AS operating_system,
    traffic_source.source AS traffic_source,
    traffic_source.medium AS traffic_medium,
    traffic_source.name AS traffic_campaign,

    -- 이벤트 정보
    event_name AS event_name,
    ep.value.int_value AS session_id,
    is_active_user AS is_active_user,
    (
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'page_title'
    ) AS page_title

  FROM
    `{{ 테이블 }}`,
    UNNEST(event_params) AS ep
  WHERE
    ep.key = 'ga_session_id'
),

session_time AS (
  SELECT
    CONCAT(user_pseudo_id, COALESCE(user_id, '')) AS user_key,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'ga_session_id'
    ) AS session_id,
    (MAX(event_timestamp) - MIN(event_timestamp)) / 1000000 AS session_length_in_seconds
  FROM
    `{{ 테이블 }}`
  GROUP BY
    1, 2
),

total_session_time AS (
  SELECT
    user_key,
    SUM(session_length_in_seconds) AS total_session_length_in_seconds
  FROM
    session_time
  GROUP BY
    1
),

first_event_time AS (
  SELECT
    CONCAT(user_pseudo_id, COALESCE(user_id, '')) AS user_key,
    MIN(event_timestamp) AS first_event_timestamp_micro
  FROM
    `{{ 테이블 }}`
  GROUP BY
    1
)

SELECT
  -- 사용자 고유값 PK
  user_key,
  -- client_id
  client_id,
  -- user_id
  user_id,
  -- ITMR_id
  mid,

  -- 플랫폼
  ANY_VALUE(platform_channel)        AS platform_channel,
  -- 성별
  ANY_VALUE(gender)                  AS gender,
  -- 나이
  ANY_VALUE(age)                     AS age,
  -- 운영체제
  ANY_VALUE(operating_system)        AS operating_system,
  -- 첫 획득 유입소스
  ANY_VALUE(traffic_source)          AS traffic_source,
  -- 첫 획득 유입매체
  ANY_VALUE(traffic_medium)          AS traffic_medium,
  -- 첫 획득 유입 캠페인
  ANY_VALUE(traffic_campaign)        AS traffic_campaign,

  -- 사용자 첫 이벤트 발생 시각
  TIMESTAMP_MICROS(
    ANY_VALUE(first_event_timestamp_micro)
  )                                  AS user_first_touch_timestamp,
  -- 이벤트 수
  COUNT(*)                           AS user_event_cnt,
  -- 조회 수
  COUNTIF(event_name = 'page_view')  AS user_pageview_cnt,
  -- 세션 수
  COUNT(DISTINCT session_id)         AS user_session_cnt,
  -- 총 세션시간
  COALESCE(
    ANY_VALUE(total_session_length_in_seconds),
    0
  )                                  AS user_total_session_time,

  -- 보험 계산기 횟수
  COUNTIF(event_name LIKE '{{ 계산하기 이벤트명 }}')
                                      AS user_calc_cnt,

  -- 가입수
  COUNTIF(
    event_name = 'page_view'
    AND page_title LIKE '{{ 가입 완료 페이지 }}'
  )                                   AS user_purchase_cnt,

  -- 상세페이지에서 발생한 버튼 클릭 수
  COUNTIF(
    event_name LIKE '{{ 상세페이지 버튼 클릭 명 }}'
    AND page_title LIKE '{{ 보험 상세 페이지 }}'
  )                                   AS user_detail_btn_click_cnt,

  -- 보험 상세페이지 조회수
  COUNTIF(
    event_name = 'page_view'
    AND page_title LIKE '{{ 보험 상세 페이지 }}'
  )                                   AS user_detail_view_cnt,

  -- 상담 신청 수
  COUNTIF(event_name LIKE '{{ 상담 신청 이벤트 명 }}')
                                      AS user_counsel_cnt,

  -- 활성 사용자 여부 (세션 중 한 번이라도 active면 TRUE)
  MAX(is_active_user)                AS is_active_user,

  -- 로그인 여부
  CASE
    WHEN user_id IS NOT NULL AND user_id != '' THEN 'Y'
    ELSE 'N'
  END                                AS is_login_yn,

  -- 마트 생성 날짜
  CURRENT_DATE()                     AS current_date

FROM
  temp
LEFT JOIN
  total_session_time USING (user_key)
LEFT JOIN
  first_event_time USING (user_key)
GROUP BY
  1, 2, 3, 4;
