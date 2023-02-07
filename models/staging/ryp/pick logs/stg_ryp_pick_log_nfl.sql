{{ 
    config(
        materialized='table', 
        alias='STG_RYP_PICK_LOG_NFL',
	tags=["stg-ryp"]
    ) 
}}

WITH PICK_LOG_NFL AS (
    SELECT
	{{ dbt_utils.surrogate_key(['P.TEAM_ID', 'P.TIMESTAMP']) }} as PICK_ID,
        M.MEMBER_ID,
        M.MEMBER_EMAIL,
        T.TEAM_ID,
        T.LEAGUE_ID,
        P.IP_ADDRESS,
        P.LEAGUE_TYPE_ID,
        P.ENTRY_ID,
        P.SHEET AS SHEET_ID,
        P.PICK_PERIOD,
        'RYP' AS PRODUCT,
        LC.DATASOURCE_NAME AS LEAGUE_CATEGORY,
        LT.DESCRIPTION AS LEAGUE_TYPE,
        DATEADD('MS', P.TIMESTAMP, '1970-01-01') AS PICK_TIMESTAMP,
        prod.public.parse_useragent(TO_ARRAY(P.USER_AGENT)) AS USER_AGENT,
        CURRENT_TIMESTAMP AS ETL_AT
    FROM {{ source('RYP_PICK_LOGS', 'PICK_LOG_NFL') }} AS P
        JOIN
        {{ source('RYP_RUNYOURPOOL_DBO', 'TEAMS') }} AS T ON
            P.TEAM_ID = T.TEAM_ID
        JOIN
        {{ source('RYP_RUNYOURPOOL_DBO', 'MEMBERS') }} AS M ON
            T.MEMBER_ID = M.MEMBER_ID
        JOIN
        {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUE_TYPES') }} AS LT ON
            LT.LEAGUE_TYPE_ID = P.LEAGUE_TYPE_ID
        JOIN
        {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUE_CATEGORIES') }} AS LC ON
            LT.LEAGUE_CATEGORY_ID = LC.LEAGUE_CATEGORY_ID
),

PICKS_NFL_SQUARES AS (
    SELECT
        null as PICK_ID,
        M.MEMBER_ID,
        M.MEMBER_EMAIL,
        T.TEAM_ID,
        T.LEAGUE_ID,
        null AS IP_ADDRESS,
        17 AS LEAGUE_TYPE_ID,
        E.ENTRY_ID,
        1 AS SHEET_ID,
        null AS PICK_PERIOD,
        'RYP' AS PRODUCT,
        'NFL' AS LEAGUE_CATEGORY,
        'NFL SQUARES' AS LEAGUE_TYPE,
        DATE_TRUNC('MONTH',G.GAME_DATE_TIME) AS PICK_TIMESTAMP,
        null AS USER_AGENT,
        CURRENT_TIMESTAMP AS ETL_AT

    FROM {{ source('RYP_NFL_DBO', 'PICKS_NFL_SQUARES') }} P
    INNER JOIN
        {{ source('RYP_NFL_DBO', 'NFL_SQUARES_SETTINGS') }} AS S ON
            P.LEAGUE_ID = S.LEAGUE_ID AND P.GRID_ID = S.GRID_ID
    INNER JOIN
        {{ source('RYP_NFL_DBO', 'GAMES_NFL') }} AS G ON
            G.GAME_ID = S.NFL_GAME_ID
    INNER JOIN
        {{ source('RYP_NFL_DBO', 'NFL_SQUARES_ENTRIES') }} AS E ON
            E.TEAM_ID = P.TEAM_ID AND P.SHEET_ID = 1
    INNER JOIN
        {{ source('RYP_RUNYOURPOOL_DBO', 'TEAMS') }} AS T ON
           P.TEAM_ID = T.TEAM_ID
    INNER JOIN
        {{ source('RYP_RUNYOURPOOL_DBO', 'MEMBERS') }} AS M ON
            T.MEMBER_ID = M.MEMBER_ID
    WHERE
        P.TEAM_ID IS NOT NULL AND S.SOURCE_GRID_ID IS NULL

),

PICKS_NFL_SQUARES_2 AS (
    SELECT
        null as PICK_ID,
        M.MEMBER_ID,
        M.MEMBER_EMAIL,
        T.TEAM_ID,
        T.LEAGUE_ID,
        null AS IP_ADDRESS,
        17 AS LEAGUE_TYPE_ID,
        E.ENTRY_ID,
        1 AS SHEET_ID,
        null AS PICK_PERIOD,
        'RYP' AS PRODUCT,
        'NFL' AS LEAGUE_CATEGORY,
        'NFL SQUARES' AS LEAGUE_TYPE,
        DATE_TRUNC('MONTH',G.GAME_DATE_TIME) AS PICK_TIMESTAMP,
        null AS USER_AGENT,
        CURRENT_TIMESTAMP AS ETL_AT

    FROM {{ source('RYP_NFL_2021_2022', 'PICKS_NFL_SQUARES') }} P
    INNER JOIN
        {{ source('RYP_NFL_2021_2022', 'NFL_SQUARES_SETTINGS') }} AS S ON
            P.LEAGUE_ID = S.LEAGUE_ID AND P.GRID_ID = S.GRID_ID
    INNER JOIN
        {{ source('RYP_NFL_2021_2022', 'GAMES_NFL') }} AS G ON
            G.GAME_ID = S.NFL_GAME_ID
    INNER JOIN
        {{ source('RYP_NFL_2021_2022', 'NFL_SQUARES_ENTRIES') }} AS E ON
            E.TEAM_ID = P.TEAM_ID AND P.SHEET_ID = 1
    INNER JOIN
        {{ source('RYP_RUNYOURPOOL_2022_05_01_DBO', 'TEAMS') }} AS T ON
           P.TEAM_ID = T.TEAM_ID
    INNER JOIN
        {{ source('RYP_RUNYOURPOOL_2022_05_01_DBO', 'MEMBERS') }} AS M ON
            T.MEMBER_ID = M.MEMBER_ID
    WHERE
        P.TEAM_ID IS NOT NULL AND S.SOURCE_GRID_ID IS NULL
),

PICKS_NFL_SQUARES_3 AS (
    SELECT
        null as PICK_ID,
        M.MEMBER_ID,
        M.MEMBER_EMAIL,
        T.TEAM_ID,
        T.LEAGUE_ID,
        null AS IP_ADDRESS,
        17 AS LEAGUE_TYPE_ID,
        E.ENTRY_ID,
        1 AS SHEET_ID,
        null AS PICK_PERIOD,
        'RYP' AS PRODUCT,
        'NFL' AS LEAGUE_CATEGORY,
        'NFL SQUARES' AS LEAGUE_TYPE,
        DATE_TRUNC('MONTH',G.GAME_DATE_TIME) AS PICK_TIMESTAMP,
        null AS USER_AGENT,
        CURRENT_TIMESTAMP AS ETL_AT

    FROM {{ source('RYP_NFL_2020_2021', 'PICKS_NFL_SQUARES') }} P
    INNER JOIN
        {{ source('RYP_NFL_2020_2021', 'NFL_SQUARES_SETTINGS') }} AS S ON
            P.LEAGUE_ID = S.LEAGUE_ID AND P.GRID_ID = S.GRID_ID
    INNER JOIN
        {{ source('RYP_NFL_2020_2021', 'GAMES_NFL') }} AS G ON
            G.GAME_ID = S.NFL_GAME_ID
    INNER JOIN
        {{ source('RYP_NFL_2020_2021', 'NFL_SQUARES_ENTRIES') }} AS E ON
            E.TEAM_ID = P.TEAM_ID AND P.SHEET_ID = 1
    INNER JOIN
        {{ source('RYP_RUNYOURPOOL_DBO', 'TEAMS') }} AS T ON
           P.TEAM_ID = T.TEAM_ID
    INNER JOIN
        {{ source('RYP_RUNYOURPOOL_DBO', 'MEMBERS') }} AS M ON
            T.MEMBER_ID = M.MEMBER_ID
    WHERE
        P.TEAM_ID IS NOT NULL AND S.SOURCE_GRID_ID IS NULL

)

SELECT * FROM PICK_LOG_NFL
UNION
SELECT * FROM PICKS_NFL_SQUARES
UNION
SELECT * FROM PICKS_NFL_SQUARES_2
UNION
SELECT * FROM PICKS_NFL_SQUARES_3
