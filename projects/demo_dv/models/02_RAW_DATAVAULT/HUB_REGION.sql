{{ config(
      alias='HUB_REGION'
    , materialized='incremental')
}}


WITH REGION AS (
      SELECT 
            MD5(R_REGIONKEY) HUB_ID_REGION
            , R_REGIONKEY AS REGION_KEY
            , '{{ var('cod_tenant') }}' AS TENANT
            , CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS DT_LOAD   
            , '{{ source('SF_SAMPLE', 'REGION') }}' AS RECORD_SOURCE
      FROM {{ source('SF_SAMPLE', 'REGION') }} 
) 
SELECT 
        REGION.HUB_ID_REGION
      , REGION.REGION_KEY
      , REGION.TENANT
      , REGION.DT_LOAD
      , REGION.RECORD_SOURCE
FROM REGION REGION
LEFT JOIN {{ this }} HUB
	ON HUB.HUB_ID_REGION =  REGION.HUB_ID_REGION
WHERE HUB.HUB_ID_REGION IS NULL