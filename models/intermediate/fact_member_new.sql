{{ 
    config(
        materialized='table', 
        alias='FACT_MEMBER_NEW',
	tags=["fact"]
    ) 
}}

WITH FINAL AS (
    SELECT DISTINCT
        MEMBER_EMAIL,
        MIN(DATE_TIME_CREATED) AS DATE_TIME_CREATED,
	SOURCE

    FROM {{ ref('stg_ryp_members') }}
	GROUP BY 1,3
UNION
    SELECT DISTINCT
        MEMBER_EMAIL,
        MIN(DATE_TIME_CREATED) AS DATE_TIME_CREATED,
	SOURCE

    FROM {{ ref('stg_ofp_members') }}
	GROUP BY 1,3

),

MIN AS (
    SELECT DISTINCT
        MEMBER_EMAIL,
        MIN(PICK_DATE) AS MIN_DATE,
	SOURCE

    FROM {{ ref('fact_active_entries') }}
	GROUP BY 1,3
)

SELECT F.*, M.MIN_DATE AS NEW_ACTIVE_USER_DATE FROM FINAL F 
LEFT JOIN MIN M ON F.MEMBER_EMAIL = M.MEMBER_EMAIL AND F.SOURCE = M.SOURCE