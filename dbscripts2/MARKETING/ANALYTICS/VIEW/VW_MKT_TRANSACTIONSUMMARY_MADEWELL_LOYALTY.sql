create or replace view VW_MKT_TRANSACTIONSUMMARY_MADEWELL_LOYALTY(
	"MasterCustomerId",
	"FM Status Type",
	"FQ Status Type",
	"FY Status Type",
	"Status Type",
	"FM Status Sort",
	"Organization Id",
	"Transaction Date Id",
	"Buyer Identification",
	"Sub Type",
	"Product Id",
	"Price Type",
	"Price Type Sort",
	"Device Type",
	"Last Touch Channel",
	"Source",
	CHANNEL,
	"Channel Type",
	"Channel Sub Type",
	"Initiative",
	"Source Initiative",
	"Product Summary Id",
	"Loyalty Tier Week Status",
	"Loyalty Tier Month Status",
	"Loyalty Tier Quarter Status",
	"Loyalty Tier Year Status",
	"Transaction Id",
	"Units",
	"Sale Revenue",
	MARGIN
) as

SELECT
T.MASTERCUSTOMERID AS "MasterCustomerId"
,C_STATUSTYPEFM AS "FM Status Type"
,C_STATUSTYPEFQ AS "FQ Status Type"
,C_STATUSTYPEFY AS "FY Status Type"
,C_STATUSTYPE AS "Status Type"
,CASE WHEN "FM Status Type" = 'New'
     THEN 1
     WHEN "FM Status Type" = 'Continuing'
     THEN 2
     WHEN "FM Status Type" = 'Reactivated'
     THEN 3
     END AS "FM Status Sort"
,ORGANIZATIONID AS "Organization Id"
,ts.id  AS "Transaction Date Id"
,BUYERIDENTIFICATION AS "Buyer Identification"
,T.SUBTYPE AS "Sub Type"
,PO."Id" AS "Product Id"
,CASE WHEN C_FPMDITEM = 'F' THEN 'Full Price'
    WHEN C_FPMDITEM = 'P' THEN 'Promotion'
    WHEN C_FPMDITEM = 'M' THEN 'Markdown'
 END AS "Price Type"
,CASE WHEN "Price Type" = 'Full Price' THEN 1
     WHEN "Price Type" = 'Promotion' THEN 2
     WHEN "Price Type" = 'Markdown' THEN 3
     END AS "Price Type Sort"
,c_devicetype AS "Device Type"
,c_lasttouchchannel AS "Last Touch Channel"
,C_SOURCE AS "Source"
,CASE WHEN C_DEVICETYPE='APP' THEN 'APP'
     WHEN C_LASTTOUCHCHANNEL IN ('AFFILIATE','AFFILIATES') THEN 'AFFILIATES'
     WHEN C_LASTTOUCHCHANNEL IN ('NATURAL SEARCH','ORGANIC SEARCH','ORGANIC') THEN 'SEO'
     WHEN C_LASTTOUCHCHANNEL IN ('VENDOR','REFERRAL') THEN 'REFERRAL'
     WHEN C_LASTTOUCHCHANNEL IN ('MBL DTS','WEB DTS','DIRECT') THEN 'DIRECT'
     WHEN C_LASTTOUCHCHANNEL IN ('APP DTS') THEN 'APP'
     WHEN C_LASTTOUCHCHANNEL IN ('INTERNAL','OTHER','(OTHER)','MISC', 'OC NOT CAPTURED') THEN 'OC NOT CAPTURED'
     ELSE C_LASTTOUCHCHANNEL END AS Channel
,CASE WHEN channel IN ('EMAIL','SMS','DIRECT','REFERRAL','SEO','APP','CUSTOMER SERVICE CHANNEL','OC NOT CAPTURED') THEN 'UNPAID'
     WHEN channel IN ('AFFILIATES','PAID SEARCH','SOCIAL','DISPLAY') THEN 'PAID'
     ELSE channel END AS "Channel Type"
,CASE WHEN channel IN ('EMAIL','SMS') THEN 'EMAIL + SMS'
     WHEN channel IN ('DIRECT','REFERRAL','SEO') THEN 'ORGANIC CHANNELS'
     WHEN channel IN ('CUSTOMER SERVICE CHANNEL','OC NOT CAPTURED') THEN 'OTHER' 
     ELSE channel END AS "Channel Sub Type"
,CASE
    WHEN C_INITIATIVE ILIKE 'branding' THEN 'Branding'
    WHEN C_CAMPAIGN LIKE '%MWEMBR07191%' THEN 'Trigger'
    WHEN C_CAMPAIGN LIKE '%MWEMBR07192%' THEN 'Trigger'
    WHEN C_INITIATIVE ILIKE 'sale' THEN 'Promo'
    WHEN C_INITIATIVE ILIKE 'promo' THEN 'Promo'
    WHEN C_INITIATIVE ILIKE 'mix' THEN 'Mix'
    WHEN C_INITIATIVE ILIKE 'trigger' THEN 'Trigger'
    WHEN C_INITIATIVE ILIKE 'vendor' THEN 'Vendor'
    WHEN C_INITIATIVE ILIKE 'transactional' THEN 'Transactional'
    WHEN C_INITIATIVE ILIKE 'bau' THEN 'BAU'
    WHEN C_CAMPAIGN ILIKE 'order-%' THEN 'Transactional'
    WHEN C_INITIATIVE ILIKE 'marketing' THEN 'Marketing'
    WHEN C_INITIATIVE ILIKE 'brand_search' THEN 'Brand Search'
    WHEN C_CAMPAIGN ILIKE 'MW_BR_US_EN_X_%' THEN 'Brand Search'
    WHEN C_INITIATIVE ILIKE 'nonbrand_search' THEN 'Non-Brand Search'
    WHEN C_INITIATIVE ILIKE 'shopping_ads' THEN 'Shopping Ads'
    WHEN C_INITIATIVE ILIKE 'dynamic' THEN 'Dynamic'
    WHEN C_INITIATIVE ILIKE 'product' THEN 'Product'
    WHEN C_INITIATIVE ILIKE 'organic' THEN 'Organic'
    WHEN C_INITIATIVE ILIKE 'socialorganic' THEN 'Organic'
    WHEN C_CAMPAIGN ILIKE '%-Org' THEN 'Organic'
    WHEN C_INITIATIVE ILIKE 'partnership' THEN 'Partnership'
    WHEN C_INITIATIVE ILIKE 'creator' THEN 'Creator'
    WHEN C_SOURCE ILIKE 'dash hudson' THEN 'Organic'
    WHEN C_INITIATIVE ILIKE 'loyalty' THEN 'Loyalty'
    WHEN C_INITIATIVE ILIKE 'coupon' THEN 'Coupon'
    WHEN C_INITIATIVE ILIKE 'bloggers' THEN 'Bloggers'
    WHEN C_INITIATIVE ILIKE 'blogger' THEN 'Bloggers'
    WHEN C_INITIATIVE ILIKE 'ambassador' THEN 'Bloggers'
    WHEN C_SOURCE ILIKE 'rstyle.me' THEN 'Bloggers'
    WHEN C_INITIATIVE ILIKE 'media' THEN 'Media'
    WHEN C_INITIATIVE ILIKE 'shopping' THEN 'Media'
    WHEN C_INITIATIVE ILIKE 'content' THEN 'Content'
    WHEN C_INITIATIVE ILIKE 'forgot_password' THEN 'Forgot Password'
    WHEN C_CAMPAIGN   ILIKE 'password-reset-email' THEN 'Forgot Password'
    ELSE '(Other)'
END AS "Initiative"
,C_INITIATIVE AS "Source Initiative"
,MP."Id" As "Product Summary Id"
,'Non-Loyalty' AS "Loyalty Tier Week Status"
,'Non-Loyalty' AS "Loyalty Tier Month Status"
,'Non-Loyalty' AS "Loyalty Tier Quarter Status"
,'Non-Loyalty' AS "Loyalty Tier Year Status"
,T.TRANSACTIONID AS "Transaction Id"
,SUM(Quantity) AS "Units"
,SUM(SALEREVENUE) AS "Sale Revenue"
,SUM(SALEREVENUE-COSTBASIS) as Margin
FROM CDP.PUBLIC.TRANSACTIONSUMMARY T
LEFT JOIN CDP.PUBLIC.ORGANIZATIONSUMMARY O ON O.ID = T.ORGANIZATIONID
LEFT JOIN CDP.PUBLIC.CUSTOMERSUMMARY C ON T.MASTERCUSTOMERID = C.MASTERCUSTOMERID
LEFT JOIN CDP.PUBLIC.TIMESUMMARY TS ON TO_DATE(TS.DATEINUNIX) = TO_DATE(t.TRANSACTIONTIMESTAMP)
LEFT JOIN CDP.PUBLIC.PRODUCTCATEGORYXREF PX ON PX.PRODUCTID = T.PRODUCTID
LEFT JOIN CDP.PUBLIC.PRODUCTCATEGORYSUMMARY PS ON PS.Id = PX.PRODUCTCATEGORYID
LEFT JOIN EDW_MARKETING_PRD.ANALYTICS.VW_MKT_PRODUCTCATEGORYSUMMARY PO ON PO."Product Category Level 1" = PS.LEVEL1NAME AND PO."Product Category Level 3" = PS.LEVEL3NAME AND PO."Product Category Level 2" = PS.LEVEL2NAME AND PO."Product Category Level 4" = PS.LEVEL4NAME AND PO."Product Category Level 5" = PS.LEVEL5NAME
LEFT JOIN CDP.PUBLIC.PRODUCTSUMMARY P ON P.ID = T.PRODUCTID
LEFT JOIN EDW_MARKETING_PRD.ANALYTICS.VW_MKT_PRODUCTSUMMARY MP ON MP."Product Style" = P.DESCRIPTION AND MP."Parent Product Id" = P.PARENTPRODUCTID
WHERE TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM CDP.PUBLIC.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)- 2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
        SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
        WHERE TS.DATEINUNIX::DATE <= current_date::DATE
        AND TS.FISCALDAYOFWEEK = 7
      )
AND T.SUBTYPE in ('Demand', 'Shipped') AND BUYERIDENTIFICATION = 'Identified'
GROUP BY ALL;