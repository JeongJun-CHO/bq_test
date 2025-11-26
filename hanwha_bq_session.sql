WITH temp AS (
  SELECT
    -- 세션 키
    CONCAT(
      user_pseudo_id,
      COALESCE(
        SAFE_CAST(
          (
            SELECT value.int_value
            FROM UNNEST(event_params)
            WHERE key = 'ga_session_id'
          ) AS STRING
        ),
        ''
      )
    ) AS session_key,

    -- 사용자 키
    CONCAT(user_pseudo_id, COALESCE(user_id, '')) AS user_key,
    user_pseudo_id AS client_id,
    user_id        AS user_id,

    -- 기본 정보
    platform                  AS platform_channel,
    event_date                AS event_date,
    device.operating_system   AS operating_system,
    session_traffic_source_last_click. manual_campaign.source         AS traffic_source,
    session_traffic_source_last_click. manual_campaign.medium         AS traffic_medium,
    session_traffic_source_last_click. manual_campaign.campaign_name  AS traffic_campaign,
    session_traffic_source_last_click. manual_campaign.content        AS traffic_content,
    session_traffic_source_last_click. manual_campaign.term           AS traffic_term,

    -- 이벤트 / 세션 정보
    event_name AS event_name,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'ga_session_id'
    ) AS session_id,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'ga_session_number'
    ) AS session_number,

    -- 페이지 정보
    (
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'page_title'
    ) AS page_title,
    (
      SELECT REGEXP_EXTRACT(value.string_value, r'^[^?#]+')
      FROM UNNEST(event_params)
      WHERE key = 'page_location'
    ) AS page_location,

    -- 참여 세션 플래그
    (
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'session_engaged'
    ) AS session_engaged,

    event_timestamp AS event_timestamp

  FROM
    `{{ 테이블 }}`
),

session_time AS (
  SELECT
    CONCAT(
      user_pseudo_id,
      COALESCE(
        SAFE_CAST(
          (
            SELECT value.int_value
            FROM UNNEST(event_params)
            WHERE key = 'ga_session_id'
          ) AS STRING
        ),
        ''
      )
    ) AS session_key,
    (MAX(event_timestamp) - MIN(event_timestamp)) / 1000000 AS session_length_in_seconds
  FROM
    `{{ 테이블 }}`
  GROUP BY
    1
)

SELECT
  -- 세션 고유값 PK
  session_key,
  -- 사용자 고유값 FK
  user_key,
  -- client_id
  client_id,
  -- user_id
  user_id,

  -- 세션 시작일 / 시간
  ANY_VALUE(DATE(TIMESTAMP_SECONDS(session_id))) AS session_start_date,
  ANY_VALUE(TIME(TIMESTAMP_SECONDS(session_id))) AS session_start_time,

  -- 플랫폼 / OS
  ANY_VALUE(platform_channel)    AS platform_channel,
  ANY_VALUE(operating_system)    AS operating_system,

  -- 세션 유입 정보
  ANY_VALUE(traffic_source)      AS traffic_source,
  ANY_VALUE(traffic_medium)      AS traffic_medium,
  ANY_VALUE(traffic_campaign)    AS traffic_campaign,
  ANY_VALUE(traffic_term)        AS traffic_term,
  ANY_VALUE(traffic_content)     AS traffic_content,

  -- 첫 방문 페이지 (landing)
  ARRAY_AGG(
    IF(event_name = 'page_view', page_location, NULL) IGNORE NULLS
    ORDER BY event_timestamp
  )[SAFE_OFFSET(0)] AS landing_page_path,

  -- 두 번째 페이지
  ARRAY_AGG(
    IF(event_name = 'page_view', page_location, NULL) IGNORE NULLS
    ORDER BY event_timestamp
  )[SAFE_OFFSET(1)] AS second_page_path,

  -- 마지막 페이지
  ARRAY_AGG(
    IF(event_name = 'page_view', page_location, NULL) IGNORE NULLS
    ORDER BY event_timestamp
  )[SAFE_OFFSET(
      ARRAY_LENGTH(
        ARRAY_AGG(
          IF(event_name = 'page_view', page_location, NULL) IGNORE NULLS
          ORDER BY event_timestamp
        )
      ) - 1
    )] AS last_page_path

  -- 세션 번호
  ANY_VALUE(session_number)      AS session_number,

  -- 이벤트 수
  COUNT(*)                       AS session_event_cnt,
  -- 조회 수
  COUNTIF(event_name = 'page_view') AS session_pageview_cnt,
  -- 세션시간
  ANY_VALUE(session_length_in_seconds) AS session_session_time,

  -- 보험 계산기 횟수
  COUNTIF(event_name LIKE '{{ 계산하기 이벤트명 }}') AS session_calc_cnt,

  -- 가입수
  COUNTIF(
    event_name = 'page_view'
    AND page_title LIKE '{{ 가입 완료 페이지 }}'
  ) AS session_purchase_cnt,

  -- 상세페이지에서 발생한 버튼 클릭 수
  COUNTIF(
    event_name LIKE '{{ 상세페이지 버튼 클릭 명 }}'
    AND page_title LIKE '{{ 보험 상세 페이지 }}'
  ) AS session_detail_btn_click_cnt,

  -- 보험 상세페이지 조회수
  COUNTIF(
    event_name = 'page_view'
    AND page_title LIKE '{{ 보험 상세 페이지 }}'
  ) AS session_detail_view_cnt,

  -- 상담 신청 수
  COUNTIF(event_name LIKE '{{ 상담 신청 이벤트 명 }}') AS session_counsel_cnt,

  -- 첫 방문 세션 여부
  ANY_VALUE(
    CASE
      WHEN session_number IS NOT NULL AND session_number = 1 THEN 'Y'
      ELSE 'N'
    END
  ) AS is_first_visit_session,

  -- 참여 세션 여부
  CASE
    WHEN MAX(session_engaged) IS NOT NULL
         AND MAX(session_engaged) = '1' THEN 'Y'
    ELSE 'N'
  END AS is_engaged_session,

  -- 로그인 여부
  CASE
    WHEN user_id IS NOT NULL AND user_id != '' THEN 'Y'
    ELSE 'N'
  END AS is_login_yn,

  -- 마트 날짜
  CURRENT_DATE() AS current_date

FROM
  temp
LEFT JOIN
  session_time USING (session_key)
GROUP BY
  1, 2, 3, 4;
