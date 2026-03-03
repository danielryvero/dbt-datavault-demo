--There should not be any lineitem record in which, the RECEIPT_DATE is in the future.
--Any record having RECEIPT_DATE greater than current date should be incorrect

SELECT 
    LINK_ID_LINEITEM_OPS
FROM 
    {{ref('SATL_LINEITEM_OPS_DATES')}} SATL
WHERE 
    SATL.RECEIPT_DATE > CURRENT_DATE 