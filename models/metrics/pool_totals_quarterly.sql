{{ 
    config(
        materialized='table', 
        alias='pools_zero_members_quarterly'
    ) 
}}

WITH TOTAL_POOLS AS (
    SELECT
        D.FISCAL_PERIOD,
        D.FISCAL_QUARTER,
        count(L.LEAGUE_ID) AS TOTAL_POOLS_CREATED

    FROM {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUES') }} AS L
    INNER JOIN
        {{ ref('dates') }} AS D ON L.LEAGUE_START_DATE::date = D.DATE_DAY::date

    --where fiscal_period like '2022-2023'
    GROUP BY 1, 2
),

ZERO_MEMBER_POOLS AS (
    SELECT
        FISCAL_PERIOD AS FISCAL_PERIOD,
        FISCAL_QUARTER AS FISCAL_QUARTER,
        count(*) AS ZERO_MEMBER_POOLS
    FROM(
            SELECT
                L.LEAGUE_ID,
                D.FISCAL_QUARTER,
                D.FISCAL_PERIOD
            FROM {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUES') }} AS L
            INNER JOIN
                {{ source('RYP_RUNYOURPOOL_DBO', 'TEAMS') }} AS T ON
                    L.LEAGUE_ID = T.LEAGUE_ID
            INNER JOIN
                {{ ref('dates') }} AS D ON
                    L.LEAGUE_START_DATE::date = D.DATE_DAY::date

            --WHERE fiscal_period like '2022-2023'
            GROUP BY 1, 2, 3
            HAVING count(L.LEAGUE_ID) = 1)
    GROUP BY 1, 2
)

SELECT
    TOTAL_POOLS.FISCAL_PERIOD,
    TOTAL_POOLS.FISCAL_QUARTER,
    TOTAL_POOLS.TOTAL_POOLS_CREATED,
    ZERO_MEMBER_POOLS.ZERO_MEMBER_POOLS,
    ZERO_MEMBER_POOLS.ZERO_MEMBER_POOLS / TOTAL_POOLS.TOTAL_POOLS_CREATED * 100 AS PERCENT_OF_ZERO_MEMBER_POOLS
FROM TOTAL_POOLS
INNER JOIN
    ZERO_MEMBER_POOLS ON
        TOTAL_POOLS.FISCAL_QUARTER = ZERO_MEMBER_POOLS.FISCAL_QUARTER AND TOTAL_POOLS.FISCAL_PERIOD = ZERO_MEMBER_POOLS.FISCAL_PERIOD
ORDER BY 1 DESC, 2
