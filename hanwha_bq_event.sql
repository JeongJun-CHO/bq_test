WITH base AS (
  SELECT
    -- 이벤트 고유 키
    CONCAT(
      event_name,
      user_pseudo_id,
      COALESCE(
        SAFE_CAST(
          (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING
        ),
        ''
      ),
      SAFE_CAST(event_timestamp AS STRING)
    ) AS event_key,

    -- 사용자 / 세션 키
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

    -- 이벤트 기본 정보
    event_name,
    event_timestamp,
    SPLIT(event_name, '/')[OFFSET(0)] AS event_name_1depth,

    -- 페이지 정보
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title')      AS page_title,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')   AS page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ep_event_Label')  AS event_label,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ep_button_Text')  AS event_button_text,

    -- 공통 사용자 속성
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'up_platform_Channel')        AS platform_channel,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ep_tab_Status')              AS tab_status,

    -- 상품 찾기 관련 속성
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ep_findprod_Age')            AS findprod_age,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ep_findprod_Gender')         AS findprod_gender,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ep_findprod_PersInfoYN')     AS findprod_persInfo_yn,

    -- 보험 설계 관련 속성
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ep_insu_CvrgCombine')        AS insu_cvrg_combine,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ep_insu_GrtAmount')          AS insu_grt_amount,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ep_insu_Period')             AS insu_period,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ep_insu_PayPeriod')          AS insu_pay_period,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ep_insu_SmokeYN')            AS insu_smoke_yn,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ep_insu_HealthCustDscnt')    AS insu_health_cust_dscnt,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ep_insu_MaturityAmount')     AS insu_maturity_amount,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ep_insu_DiscountApply')      AS insu_discnt_apply,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ep_insu_prodType')           AS insu_prod_type

  FROM
    `{{ 테이블 }}`
  WHERE
    event_name NOT IN (
      'session_start',
      'first_visit',
      'user_engagement',
      'scroll',
      'click',
      'file_download'
    );
)

SELECT
  event_key,
  user_key,
  session_key,
  event_name,
  event_timestamp,
  event_name_1depth,

  -- 세션 내 이벤트 순서
  ROW_NUMBER() OVER (
    PARTITION BY session_key
    ORDER BY event_timestamp
  ) AS event_order,

  page_title,
  page_location,
  event_label,
  event_button_text,
  platform_channel,
  tab_status,
  findprod_age,
  findprod_gender,
  findprod_persInfo_yn,
  insu_cvrg_combine,
  insu_grt_amount,
  insu_period,
  insu_pay_period,
  insu_smoke_yn,
  insu_health_cust_dscnt,
  insu_maturity_amount,
  insu_discnt_apply,
  insu_prod_type,

  CURRENT_DATE() AS current_date

FROM
  base;
