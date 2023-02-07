{{ 
    config(
        materialized='table', 
        alias='STG_RYP_PICK_LOG_PGA',
	tags=["stg-ryp"]
    ) 
}}

WITH PICK_LOG_PGA AS (
    SELECT
	    {{ dbt_utils.surrogate_key(['P.TEAM_ID', 'P.TIMESTAMP']) }} as PICK_ID,
        M.MEMBER_ID,
        M.MEMBER_EMAIL,
        T.TEAM_ID,
        T.LEAGUE_ID,
        P.IP_ADDRESS,
        P.LEAGUE_TYPE_ID,
        P.ENTRY_ID,
        1 AS SHEET_ID,
        P.PICK_PERIOD,
        'RYP' AS PRODUCT,
        LC.DATASOURCE_NAME AS LEAGUE_CATEGORY,
        LT.DESCRIPTION AS LEAGUE_TYPE,
        DATEADD('MS', P.TIMESTAMP, '1970-01-01') AS PICK_TIMESTAMP,
        prod.public.parse_useragent(TO_ARRAY(P.USER_AGENT)) AS USER_AGENT,
        CURRENT_TIMESTAMP AS ETL_AT
    FROM {{ source('RYP_PICK_LOGS', 'PICK_LOG_PGA') }} AS P
    INNER JOIN
        {{ source('RYP_RUNYOURPOOL_DBO', 'TEAMS') }} AS T ON
            P.TEAM_ID = T.TEAM_ID
    INNER JOIN
        {{ source('RYP_RUNYOURPOOL_DBO', 'MEMBERS') }} AS M ON
            T.MEMBER_ID = M.MEMBER_ID
    INNER JOIN
        {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUE_TYPES') }} AS LT ON
            LT.LEAGUE_TYPE_ID = P.LEAGUE_TYPE_ID
    INNER JOIN
        {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUE_CATEGORIES') }} AS LC ON
            LT.LEAGUE_CATEGORY_ID = LC.LEAGUE_CATEGORY_ID
)

SELECT * FROM PICK_LOG_PGA
