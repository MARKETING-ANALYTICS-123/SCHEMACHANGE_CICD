CREATE OR REPLACE PROCEDURE "USP_POPULATE_PRODUCT_SUMMARY"()
RETURNS VARCHAR(100)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
    BEGIN

    -- Insert new data into the target table
    INSERT INTO RPT."PRODUCT SUMMARY" (
        "Id",
        "Parent Product Id",
        "Product Style",
        "Image Url"
    )
    
    WITH GET_ALL_PRODUCT AS
    (
        SELECT 
            PARENTPRODUCTID AS "Parent Product Id",
            DESCRIPTION AS "Product Style",
            IMAGEURL AS "Image Url"
        FROM CDP_JC.PUBLIC.PRODUCTSUMMARY
        UNION
        SELECT 
            PARENTPRODUCTID AS "Parent Product Id",
            DESCRIPTION AS "Product Style",
            IMAGEURL AS "Image Url"
        FROM CDP.PUBLIC.PRODUCTSUMMARY
    ),

    IGNORE_PRESENT_ROWS AS (
        SELECT DISTINCT
            "Parent Product Id",
            "Product Style"
        FROM
            GET_ALL_PRODUCT
        EXCEPT
        SELECT DISTINCT
            "Parent Product Id",
            "Product Style"
        FROM
            RPT."PRODUCT SUMMARY"
    ),

    ROWS_TO_BE_INSERTED AS (
        SELECT 
        T1."Parent Product Id",
        T1."Product Style",
        T1."Image Url"
        FROM
        GET_ALL_PRODUCT T1
        INNER JOIN IGNORE_PRESENT_ROWS T2 ON T1."Parent Product Id" = T2."Parent Product Id"
        AND T1."Product Style" = T2."Product Style"
    ),

    GET_MAX_ID AS (
        SELECT COALESCE(MAX("Id"), 0) AS MAX_ID
        FROM
        RPT."PRODUCT SUMMARY"
    )
    
    SELECT 
        DENSE_RANK() OVER (ORDER BY "Parent Product Id", "Product Style", "Image Url") + (SELECT MAX_ID FROM GET_MAX_ID) AS "Id",
        "Parent Product Id",
        "Product Style",
        "Image Url"
    FROM (
        SELECT 
            "Parent Product Id",
            "Product Style",
            "Image Url",
            RANK() OVER (PARTITION BY "Parent Product Id", "Product Style" ORDER BY "Image Url") AS "Rank"
        FROM (
            SELECT
                "Parent Product Id",
                "Product Style",
                "Image Url"
            FROM
                ROWS_TO_BE_INSERTED
        ) AS ALLPRODUCT
    ) AS GETFIRSTIMAGEURLONLY
    WHERE "Rank" = 1;

    -- Return success message
    RETURN ''Table RPT."PRODUCT SUMMARY" created or replaced successfully.'';
END;
';