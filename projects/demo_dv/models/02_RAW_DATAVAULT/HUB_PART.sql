{{ config(
      alias='HUB_PART'
    , materialized='incremental')
}}


WITH PART AS (
      SELECT 
            MD5(P_PARTKEY) HUB_ID_PART
            , P_PARTKEY AS PART_KEY
            , '{{ var('cod_tenant') }}' AS TENANT
            , CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS DT_LOAD   
            , '{{ source('SF_SAMPLE', 'PART') }}' AS RECORD_SOURCE
      FROM {{ source('SF_SAMPLE', 'PART') }} 
) 
SELECT 
        PART.HUB_ID_PART
      , PART.PART_KEY
      , PART.TENANT
      , PART.DT_LOAD
      , PART.RECORD_SOURCE
FROM PART PART
LEFT JOIN {{ this }} HUB
	ON HUB.HUB_ID_PART =  PART.HUB_ID_PART
WHERE HUB.HUB_ID_PART IS NULL