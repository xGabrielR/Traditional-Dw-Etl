INSERT INTO DMOls.D_PRODUTO (
    CODIGO_PRODUTO,
    CATEGORIA,
    COMPRIMEIRO_NOME,
    QUANTIDADE_FOTOS
)
SELECT
    S.PRODUCT_ID AS CODIGO_PRODUTO,
    ISNULL(UPPER(TRIM(S.PRODUCT_CATEGORY_NAME)), 'NAO LOCALIZADO') AS CATEGORIA,
    ISNULL(S.PRODUCT_NAME_LENGHT,0) AS COMPRIMEIRO_NOME,
    ISNULL(S.PRODUCT_PHOTOS_QTY,0) AS QUANTIDADE_FOTOS
FROM StgOls.STG_OLS_PRODUCTS S
LEFT JOIN DMOls.D_PRODUTO D ON D.CODIGO_PRODUTO = S.PRODUCT_ID 
WHERE D.CODIGO_PRODUTO IS NULL