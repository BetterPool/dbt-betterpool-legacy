{{ 
    config(
        materialized='table', 
        alias='STG_RYP_USER_CONNECTIONS',
	tags=["stg-ryp"]
    ) 
}}

WITH USER_CONNECTIONS AS (
    SELECT
        USER_CONNECTION_ID,
        APPROVED_AT,
        TO_USER_ID,
        CREATED_AT,
        FROM_USER_ID,
        CHAT_CHANNEL_ID,
        'RYP' AS SOURCE
    FROM
        {{ source('RYP_RUNYOURPOOL_DBO', 'USER_CONNECTIONS') }}
)

SELECT * FROM USER_CONNECTIONS
