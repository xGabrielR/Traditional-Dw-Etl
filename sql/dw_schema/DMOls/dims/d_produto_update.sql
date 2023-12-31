UPDATE DMOls.D_PRODUTO SET 
    CATEGORIA = ISNULL(UPPER(TRIM(S.PRODUCT_CATEGORY_NAME)), 'NAO LOCALIZADO'),
    COMPRIMEIRO_NOME = ISNULL(S.PRODUCT_NAME_LENGHT,0),
    QUANTIDADE_FOTOS = ISNULL(S.PRODUCT_PHOTOS_QTY,0),
    DATA_PROCESSAMENTO = GETDATE()
FROM StgOls.STG_OLS_PRODUCTS S
INNER JOIN DMOls.D_PRODUTO D ON D.CODIGO_PRODUTO = S.PRODUCT_ID 
WHERE D.CODIGO_PRODUTO = S.PRODUCT_ID