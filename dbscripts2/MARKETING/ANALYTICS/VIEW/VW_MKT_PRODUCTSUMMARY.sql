create or replace view VW_MKT_PRODUCTSUMMARY(
	"Id",
	"Parent Product Id",
	"Product Style",
	"Image Url"
) as 

WITH ALLPRODUCT AS (
    SELECT 
    PARENTPRODUCTID as "Parent Product Id"
    ,Description AS "Product Style"
    ,IMAGEURL As "Image Url"
    FROM CDP_JC.PUBLIC.PRODUCTSUMMARY
    UNION
    SELECT 
    PARENTPRODUCTID as "Parent Product Id"
    ,Description AS "Product Style"
    ,IMAGEURL As "Image Url"
    FROM CDP.PUBLIC.PRODUCTSUMMARY
),
GETFIRSTIMAGEURLONLY AS(
    SELECT
      "Parent Product Id",
      "Product Style",
      "Image Url",
      RANK() OVER (PARTITION BY "Parent Product Id", "Product Style" ORDER BY "Image Url") AS "Rank"
    FROM ALLPRODUCT
),

GETFIRSTPRODUCTIMAGE AS(
    SELECT
      "Parent Product Id",
      "Product Style",
      "Image Url"
    FROM GETFIRSTIMAGEURLONLY
    WHERE "Rank" = 1
)
SELECT
DENSE_RANK() OVER(ORDER BY "Parent Product Id","Product Style","Image Url") AS "Id"
,"Parent Product Id"
,"Product Style"
,"Image Url"
FROM GETFIRSTPRODUCTIMAGE;