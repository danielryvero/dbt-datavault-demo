{{ config(
      alias='SAT_PART_STATIC'
    , materialized='incremental')
}}


WITH SAT_DATA AS (
SELECT  
      MD5(P_PARTKEY) HUB_ID_PART
      , '{{ var('cod_tenant') }}' AS TENANT      
      , P_NAME AS NAME
      , P_MFGR AS MANUFACTURER
      , P_BRAND AS BRAND
      , P_TYPE AS TYPE
      , P_COMMENT AS COMMENT
      , CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS DT_LOAD            
      , MD5( COALESCE(P_NAME,'#')
            ||COALESCE(P_MFGR,'#') 
            ||COALESCE(P_BRAND,'#') 
            ||COALESCE(P_TYPE,'#') 
            ||COALESCE(P_COMMENT,'#') 
            ) HASH_DIFF
      , '{{ source('SF_SAMPLE', 'PART') }}' AS RECORD_SOURCE
FROM {{ source('SF_SAMPLE', 'PART') }} 
)

SELECT 
      SAT_DATA.HUB_ID_PART
      , SAT_DATA.TENANT
      , SAT_DATA.NAME
      , SAT_DATA.MANUFACTURER
      , SAT_DATA.BRAND
      , SAT_DATA.TYPE
      , SAT_DATA.COMMENT
      , SAT_DATA.DT_LOAD
      , SAT_DATA.HASH_DIFF
      , SAT_DATA.RECORD_SOURCE
FROM SAT_DATA SAT_DATA
LEFT JOIN {{ this }} SAT
	ON SAT.HUB_ID_PART =  SAT_DATA.HUB_ID_PART
   AND SAT.HASH_DIFF = SAT_DATA.HASH_DIFF
WHERE SAT.HUB_ID_PART IS NULL






