{{ 
    config(
        materialized='table', 
        alias='STG_RYP_LEAGUE_RESET_STATUS',
	tags=["stg-ryp"]
    ) 
}}

WITH LEAGUE_RESET_STATUS AS (
    SELECT
        LEAGUE_ID,
        POOL_YEAR,
        RESET_DATE,
        RESET_STATUS_ID,
        'RYP' AS SOURCE

    FROM
        {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUE_RESET_STATUS') }}
)

SELECT * FROM LEAGUE_RESET_STATUS
