{{ 
    config(
        materialized='table', 
        alias='STG_RYP_MEMBERS',
	tags=["stg-ryp"]
    ) 
}}

WITH MEMBERS AS (
    SELECT
        MEMBER_ID,
        EMAILCONFIRMED,
        USERNAME,
        CREDIT_DATE_TIME,
        REGIONAWS,
        MEMBER_LAST_NAME,
        CREDIT,
        PHONE,
        MEMBER_FIRST_NAME,
        DATE_TIME_CREATED,
        MEMBER_EMAIL,
        COUNTRYAWS,
        PASSWORDHASH,
        IPADDRESS,
        DISPLAY_NAME,
        REFERRAL_CODE,
        REFERRED_BY,
        CHAT_USER_ID,
        'RYP' AS SOURCE
    FROM
        {{ source('RYP_RUNYOURPOOL_DBO', 'MEMBERS') }}
)

SELECT * FROM MEMBERS
