{{ 
    config(
        materialized='table', 
        alias='STG_RYP_LOGIN_LOG',
	tags=["stg-ryp"]
    ) 
}}

WITH LOGIN_LOG AS (
    SELECT
        MEMBER_ID,
        DATEADD('MS', TIMESTAMP, '1970-01-01') AS TIMESTAMP,
        IP_ADDRESS,
        TYPE,
        USER_AGENT,
        'RYP' AS SOURCE

    FROM
        {{ source('RYP_DYNAMODB', 'LOGIN_LOG') }}
)

SELECT * FROM LOGIN_LOG
