CREATE OR REPLACE PROCEDURE "USP_POPULATE_PRODUCT_CATEGORY_SUMMARY"()
RETURNS VARCHAR(100)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN

    -- Insert new data into the target table

    INSERT INTO RPT."PRODUCT CATEGORY SUMMARY" (
        "Id", 
        "Product Category Level 1", 
        "Product Category Level 2", 
        "Product Category Level 3", 
        "Product Category Level 4", 
        "Product Category Level 5"
    )

    WITH GET_ALL_RECORD AS
    (
        SELECT 
            DISTINCT
            LEVEL1NAME AS "Product Category Level 1",
            LEVEL2NAME AS "Product Category Level 2",
            LEVEL3NAME AS "Product Category Level 3",
            LEVEL4NAME AS "Product Category Level 4",
            LEVEL5NAME AS "Product Category Level 5"
        FROM 
            CDP.PUBLIC.PRODUCTCATEGORYSUMMARY PS
            
        UNION
        
        SELECT 
            DISTINCT
            LEVEL1NAME AS "Product Category Level 1",
            LEVEL2NAME AS "Product Category Level 2",
            LEVEL3NAME AS "Product Category Level 3",
            CASE 
                WHEN LEVEL3NAME = ''W PANTS'' AND LEVEL4NAME <> ''W PANTS DENIM'' THEN ''W PANTS NON DENIM''
                WHEN LEVEL3NAME = ''M PANTS'' AND (LEVEL4NAME = ''MENS PANTS DENIM'' OR LEVEL4NAME = ''M PANTS DENIM'') THEN ''M PANTS DENIM''
                WHEN LEVEL3NAME = ''M PANTS'' AND LEVEL4NAME <> ''MENS PANTS DENIM'' AND LEVEL4NAME <> ''M PANTS DENIM'' THEN ''M PANTS NON DENIM''
                ELSE LEVEL4NAME 
            END AS "Product Category Level 4",
            LEVEL5NAME AS "Product Category Level 5"
        FROM 
            CDP_JC.PUBLIC.PRODUCTCATEGORYSUMMARY
    ),

    GET_NEW_RECORD AS 
    (
        SELECT
            "Product Category Level 1",
            "Product Category Level 2",
            "Product Category Level 3",
            "Product Category Level 4",
            "Product Category Level 5"
        FROM GET_ALL_RECORD
        
        EXCEPT
        
        SELECT
            "Product Category Level 1",
            "Product Category Level 2",
            "Product Category Level 3",
            "Product Category Level 4",
            "Product Category Level 5"
        FROM RPT."PRODUCT CATEGORY SUMMARY"
        
    ),

    GET_MAX_ID AS
    (
        SELECT COALESCE(MAX("Id"), 0) AS MAX_ID
        FROM
        RPT."PRODUCT CATEGORY SUMMARY"
    )
    
    SELECT
        DENSE_RANK() OVER (
            ORDER BY 
                "Product Category Level 1", 
                "Product Category Level 2", 
                "Product Category Level 3", 
                "Product Category Level 4", 
                "Product Category Level 5"
        ) + (SELECT MAX_ID FROM GET_MAX_ID) AS "Id",
        "Product Category Level 1",
        "Product Category Level 2",
        "Product Category Level 3",
        "Product Category Level 4",
        "Product Category Level 5"
    FROM (
        SELECT 
            "Product Category Level 1",
            "Product Category Level 2",
            "Product Category Level 3",
            "Product Category Level 4",
            "Product Category Level 5"
        FROM 
            GET_NEW_RECORD
    );

    -- Return success message
    RETURN ''Table RPT."PRODUCT CATEGORY SUMMARY" created or replaced successfully.'';
END;
';