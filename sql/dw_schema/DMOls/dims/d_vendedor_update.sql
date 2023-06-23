UPDATE DMOls.D_VENDEDOR SET 
    PREFIXO_ZIPCODE = ISNULL(S.SELLER_ZIP_CODE_PREFIX, 0.0),
    CIDADE = ISNULL(UPPER(TRIM(S.SELLER_CITY)), 'NAO LOCALIZADO'),
    ESTADO = ISNULL(UPPER(TRIM(S.SELLER_STATE)), 'NAO LOCALIZADO'),
    DATA_PROCESSAMENTO = GETDATE()
FROM StgOls.STG_OLS_SELLERS S
INNER JOIN DMOls.D_VENDEDOR D ON D.CODIGO_VENDEDOR = S.SELLER_ID 
WHERE D.CODIGO_VENDEDOR = S.SELLER_ID 