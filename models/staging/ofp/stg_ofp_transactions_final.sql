{{ 
    config(
        materialized='table', 
        alias='STG_OFP_TRANSACTIONS_FINAL',
	tags=["stg-ofp"]

    ) 
}}

WITH ALIAS AS (
    SELECT DISTINCT
        *
    FROM {{ ref('stg_ofp_members') }} M
    
    WHERE ALIAS_OF_ID IS NOT NULL 
),

FINAL_MEMBER AS (
    SELECT DISTINCT
        IFNULL(A.MEMBER_ID,M.MEMBER_ID) AS MEMBER_ID,
        M.MEMBER_EMAIL
    FROM {{ ref('stg_ofp_members') }} M
        JOIN ALIAS A ON M.MEMBER_ID = A.ALIAS_OF_ID
    UNION
    SELECT DISTINCT
        M.MEMBER_ID AS MEMBER_ID,
        M.MEMBER_EMAIL
    FROM {{ ref('stg_ofp_members') }} M
    WHERE ALIAS_OF_ID IS NULL 
),

FINAL_ACTIVE_POOLS_OFP AS (
    SELECT 
        TRANSACTION_ID AS ORDER_ID,
        T.POOL_ID AS LEAGUE_ID,
        TRANSACTION_DATE AS TRANSACTION_DATE,
        AMOUNT AS TOTAL_AMOUNT,
        LEAGUE_TYPE AS LEAGUE_TYPE,
        LEAGUE_CATEGORY AS LEAGUE_CATEGORY,
        M.MEMBER_ID AS MEMBER_ID,
        M.MEMBER_EMAIL AS MEMBER_EMAIL,
        'OFP' AS SOURCE

    FROM {{ ref('stg_ofp_transactions') }} T
        JOIN FINAL_MEMBER M ON M.MEMBER_ID = T.MEMBER_ID
        JOIN {{ ref('stg_ofp_pools') }} P ON T.POOL_ID = P.POOL_ID
),

FINAL_DELETED_POOLS_OFP AS (
    SELECT DISTINCT
        TRANSACTION_ID AS ORDER_ID,
        T.POOL_ID AS LEAGUE_ID,
        TRANSACTION_DATE AS TRANSACTION_DATE,
        AMOUNT AS TOTAL_AMOUNT,
        LEAGUE_TYPE AS LEAGUE_TYPE,
        LEAGUE_CATEGORY AS LEAGUE_CATEGORY,
        M.MEMBER_ID AS MEMBER_ID,
        M.MEMBER_EMAIL AS MEMBER_EMAIL,
        'OFP' AS SOURCE

    FROM {{ ref('stg_ofp_transactions') }} T
        JOIN FINAL_MEMBER M ON M.MEMBER_ID = T.MEMBER_ID
        JOIN {{ ref('stg_ofp_deleted_pools') }} P ON T.POOL_ID = P.POOL_ID
)
SELECT * FROM FINAL_DELETED_POOLS_OFP
UNION
SELECT * FROM FINAL_ACTIVE_POOLS_OFP