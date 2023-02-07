{{ 
    config(
        materialized='table', 
        alias='FACT_ACTIVE_ENTRIES',
	tags=["fact"]
    ) 
}}

WITH NFL_ENTRIES_DATE AS (
    SELECT
        E.ENTRY_ID,
        E.MEMBER_EMAIL,
        E.SHEET_ID,
        E.TEAM_ID,
        E.LEAGUE_ID,
        E.LEAGUE_TYPE,
        E.LEAGUE_CATEGORY,
        D.DATE_DAY AS PICK_DATE,
        D.FISCAL_PERIOD,
        D.FISCAL_QUARTER,
        'RYP' AS SOURCE
    FROM {{ ref('stg_ryp_pick_log_nfl') }} AS E
    INNER JOIN {{ ref('dates') }} AS D ON E.PICK_TIMESTAMP::DATE = D.DATE_DAY

),

GENERIC_ENTRIES_DATE AS (
    SELECT
        E.ENTRY_ID,
        E.MEMBER_EMAIL,
        E.SHEET_ID,
        E.TEAM_ID,
        E.LEAGUE_ID,
        E.LEAGUE_TYPE,
        E.LEAGUE_CATEGORY,
        D.DATE_DAY AS PICK_DATE,
        D.FISCAL_PERIOD,
        D.FISCAL_QUARTER,
        'RYP' AS SOURCE
    FROM {{ ref('stg_ryp_pick_log_generic') }} AS E
    INNER JOIN {{ ref('dates') }} AS D ON E.PICK_TIMESTAMP::DATE = D.DATE_DAY

),

MLB_ENTRIES_DATE AS (
    SELECT
        E.ENTRY_ID,
        E.MEMBER_EMAIL,
        E.SHEET_ID,
        E.TEAM_ID,
        E.LEAGUE_ID,
        E.LEAGUE_TYPE,
        E.LEAGUE_CATEGORY,
        D.DATE_DAY AS PICK_DATE,
        D.FISCAL_PERIOD,
        D.FISCAL_QUARTER,
        'RYP' AS SOURCE
    FROM {{ ref('stg_ryp_pick_log_mlb') }} AS E
    INNER JOIN {{ ref('dates') }} AS D ON E.PICK_TIMESTAMP::DATE = D.DATE_DAY

),

CBB_ENTRIES_DATE AS (
    SELECT
        E.ENTRY_ID,
        E.MEMBER_EMAIL,
        E.SHEET_ID,
        E.TEAM_ID,
        E.LEAGUE_ID,
        E.LEAGUE_TYPE,
        E.LEAGUE_CATEGORY,
        D.DATE_DAY AS PICK_DATE,
        D.FISCAL_PERIOD,
        D.FISCAL_QUARTER,
        'RYP' AS SOURCE
    FROM {{ ref('stg_ryp_pick_log_cbb') }} AS E
    INNER JOIN {{ ref('dates') }} AS D ON E.PICK_TIMESTAMP::DATE = D.DATE_DAY

),

CFB_ENTRIES_DATE AS (
    SELECT
        E.ENTRY_ID,
        E.MEMBER_EMAIL,
        E.SHEET_ID,
        E.TEAM_ID,
        E.LEAGUE_ID,
        E.LEAGUE_TYPE,
        E.LEAGUE_CATEGORY,
        D.DATE_DAY AS PICK_DATE,
        D.FISCAL_PERIOD,
        D.FISCAL_QUARTER,
        'RYP' AS SOURCE
    FROM {{ ref('stg_ryp_pick_log_cfb') }} AS E
    INNER JOIN {{ ref('dates') }} AS D ON E.PICK_TIMESTAMP::DATE = D.DATE_DAY

),

NBA_ENTRIES_DATE AS (
    SELECT
        E.ENTRY_ID,
        E.MEMBER_EMAIL,
        E.SHEET_ID,
        E.TEAM_ID,
        E.LEAGUE_ID,
        E.LEAGUE_TYPE,
        E.LEAGUE_CATEGORY,
        D.DATE_DAY AS PICK_DATE,
        D.FISCAL_PERIOD,
        D.FISCAL_QUARTER,
        'RYP' AS SOURCE
    FROM {{ ref('stg_ryp_pick_log_nba') }} AS E
    INNER JOIN {{ ref('dates') }} AS D ON E.PICK_TIMESTAMP::DATE = D.DATE_DAY

),

NHL_ENTRIES_DATE AS (
    SELECT
        E.ENTRY_ID,
        E.MEMBER_EMAIL,
        E.SHEET_ID,
        E.TEAM_ID,
        E.LEAGUE_ID,
        E.LEAGUE_TYPE,
        E.LEAGUE_CATEGORY,
        D.DATE_DAY AS PICK_DATE,
        D.FISCAL_PERIOD,
        D.FISCAL_QUARTER,
        'RYP' AS SOURCE
    FROM {{ ref('stg_ryp_pick_log_nhl') }} AS E
    INNER JOIN {{ ref('dates') }} AS D ON E.PICK_TIMESTAMP::DATE = D.DATE_DAY
),

PGA_ENTRIES_DATE AS (
    SELECT
        E.ENTRY_ID,
        E.MEMBER_EMAIL,
        E.ENTRY_ID AS SHEET_ID,
        E.TEAM_ID,
        E.LEAGUE_ID,
        E.LEAGUE_TYPE,
        E.LEAGUE_CATEGORY,
        D.DATE_DAY AS PICK_DATE,
        D.FISCAL_PERIOD,
        D.FISCAL_QUARTER,
        'RYP' AS SOURCE
    FROM {{ ref('stg_ryp_pick_log_pga') }} AS E
    INNER JOIN {{ ref('dates') }} AS D ON E.PICK_TIMESTAMP::DATE = D.DATE_DAY

),

SOCCER_ENTRIES_DATE AS (
    SELECT
        E.ENTRY_ID,
        E.MEMBER_EMAIL,
        E.SHEET_ID,
        E.TEAM_ID,
        E.LEAGUE_ID,
        E.LEAGUE_TYPE,
        E.LEAGUE_CATEGORY,
        D.DATE_DAY AS PICK_DATE,
        D.FISCAL_PERIOD,
        D.FISCAL_QUARTER,
        'RYP' AS SOURCE
    FROM {{ ref('stg_ryp_pick_log_soccer') }} AS E
    INNER JOIN {{ ref('dates') }} AS D ON E.PICK_TIMESTAMP::DATE = D.DATE_DAY

),

HISTORICAL_ENTRIES_DATE AS (
    SELECT
        E.ENTRY_ID,
        E.MEMBER_EMAIL,
        E.SHEET_ID,
        E.TEAM_ID,
        E.LEAGUE_ID,
        E.LEAGUE_TYPE,
        E.LEAGUE_CATEGORY,
        D.DATE_DAY AS PICK_DATE,
        D.FISCAL_PERIOD,
        D.FISCAL_QUARTER,
        'RYP' AS SOURCE
    FROM {{ ref('stg_ryp_pick_log_historical') }} AS E
    INNER JOIN {{ ref('dates') }} AS D ON E.PICK_TIMESTAMP::DATE = D.DATE_DAY

),

BACK_LOG AS (
	SELECT * FROM {{ ref('stg_ryp_back_log') }}
	WHERE LEAGUE_TYPE NOT LIKE 'Golf Survivor'
),
RYP AS (
SELECT * FROM CBB_ENTRIES_DATE
UNION
SELECT * FROM NFL_ENTRIES_DATE
UNION
SELECT * FROM CFB_ENTRIES_DATE
UNION
SELECT * FROM MLB_ENTRIES_DATE
UNION
SELECT * FROM NBA_ENTRIES_DATE
UNION
SELECT * FROM NHL_ENTRIES_DATE
UNION
SELECT * FROM PGA_ENTRIES_DATE
UNION
SELECT * FROM SOCCER_ENTRIES_DATE
UNION
SELECT * FROM HISTORICAL_ENTRIES_DATE
UNION
SELECT * FROM BACK_LOG
UNION
SELECT * FROM GENERIC_ENTRIES_DATE

),

FINAL_RYP AS (
SELECT 
        CONCAT(IFNULL(F.TEAM_ID,0), IFNULL(F.LEAGUE_ID,0), F.SOURCE) AS ACTIVE_ENTRY_ID,
	F.ENTRY_ID,
	M.MEMBER_EMAIL,
	F.SHEET_ID,
	F.TEAM_ID,
	F.LEAGUE_ID,
	F.LEAGUE_TYPE,
	F.LEAGUE_CATEGORY,
	F.PICK_DATE,
	F.FISCAL_PERIOD,
	F.FISCAL_QUARTER,
	F.SOURCE 
FROM RYP F
JOIN {{ ref('stg_ryp_teams') }} T ON F.TEAM_ID = T.TEAM_ID
JOIN {{ ref('stg_ryp_members') }} M ON T.MEMBER_ID = M.MEMBER_ID

),

FINAL_OFP AS (
    SELECT
        CONCAT(IFNULL(MEMBER_ID,0), IFNULL(POOL_ID,0), SOURCE) AS ACTIVE_ENTRY_ID,
        (MEMBER_ID + POOL_ID) AS ENTRY_ID,
        MEMBER_EMAIL AS MEMBER_EMAIL,
        PL.POOL_ID AS SHEET_ID,
        (MEMBER_ID) AS TEAM_ID,
        PL.POOL_ID AS LEAGUE_ID,
        PL.POOL_TYPE AS LEAGUE_TYPE,
        PL. LEAGUE_CATEGORY AS LEAGUE_CATEGORY,
        PL.PICK_DATE::DATE AS PICK_DATE,
        D.FISCAL_PERIOD,
        D.FISCAL_QUARTER,
        'OFP' AS SOURCE

    FROM {{ ref('stg_ofp_pick_log') }} PL
    INNER JOIN {{ ref('dates') }} D ON PL.PICK_DATE::DATE = D.DATE_DAY
)
	SELECT * FROM FINAL_OFP
	UNION
	SELECT * FROM FINAL_RYP

