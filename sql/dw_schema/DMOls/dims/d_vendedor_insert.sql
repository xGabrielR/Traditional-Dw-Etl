INSERT INTO DMOls.D_VENDEDOR (
    CODIGO_VENDEDOR,
    PREFIXO_ZIPCODE,
    CIDADE,
    ESTADO
)
SELECT 
    S.SELLER_ID AS CODIGO_VENDEDOR,
    ISNULL(S.SELLER_ZIP_CODE_PREFIX, 0.0) AS PREFIXO_ZIPCODE,
    ISNULL(UPPER(TRIM(S.SELLER_CITY)), 'NAO LOCALIZADO') AS CIDADE,
    ISNULL(UPPER(TRIM(S.SELLER_STATE)), 'NAO LOCALIZADO') AS ESTADO
FROM StgOls.STG_OLS_SELLERS S
LEFT JOIN DMOls.D_VENDEDOR D ON D.CODIGO_VENDEDOR = S.SELLER_ID 
WHERE D.CODIGO_VENDEDOR IS NULL