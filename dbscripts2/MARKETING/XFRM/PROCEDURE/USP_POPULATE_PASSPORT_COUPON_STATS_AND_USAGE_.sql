CREATE OR REPLACE PROCEDURE "USP_POPULATE_PASSPORT_COUPON_STATS_AND_USAGE"()
RETURNS VARCHAR(100)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
	BEGIN

	create or replace TABLE Temp."PASSPORT COUPON STATS AND USAGE" (
	"MCID" VARCHAR(100),
		"Issue Date Id" NUMBER(38,0),
		"Brand" VARCHAR(10),
		"Redemption Brand" VARCHAR(10),
		"Loyalty Tier" VARCHAR(100),
		"Brand Sort" NUMBER(38,0),
		"Code" VARCHAR(200),
		"Reward Name" VARCHAR(200),
		"Status" VARCHAR(10),
		"Loyalty Coupon Id" NUMBER(38,0),
		"Expiration Date Id" NUMBER(38,0),
		"Used Date Id" NUMBER(38,0),
		"Transaction Date Id" NUMBER(38,0),
		"Organization ID" VARCHAR(30),
		"Employee Flag" NUMBER(3,0),
		"Days to Usage" NUMBER(38,0),
		"Distribution Channel" VARCHAR(16777216),
		"Status Type" NUMBER(3, 0),
		"DMD Revenue" FLOAT,
		"DMD Units" NUMBER(38,0),
		"DMD Cost Basis" FLOAT,
		"Rewards Amount" FLOAT,
		"Transactions" VARCHAR(200)
		,"JCCC Tender" VARCHAR(5)
		,"JCCC Cardholder" VARCHAR(5)
		,"JCCC Customers / Cardholders filter" VARCHAR(5)
	);

	INSERT INTO  Temp."PASSPORT COUPON STATS AND USAGE"(
	"MCID",
		"Issue Date Id",
		"Brand",
		"Redemption Brand",
		"Loyalty Tier",
		"Brand Sort",
		"Code",
		"Reward Name",
		"Status",
		"Loyalty Coupon Id",
		"Expiration Date Id",
		"Used Date Id",
		"Transaction Date Id",
		"Organization ID",
		"Employee Flag",
		"Days to Usage",
		"Distribution Channel",
		"Status Type",
		"DMD Revenue",
		"DMD Units",
		"DMD Cost Basis",
		"Rewards Amount",
		"Transactions",
		"JCCC Tender",
		"JCCC Cardholder",
		"JCCC Customers / Cardholders filter"
	) 

	WITH GET_TIER_DETAIL AS
	(
		SELECT EXTERNAL_CUSTOMER_ID,
		SNAPSHOT_DATE,
		LAST_ACTIVITY,
		TOP_TIER_NAME,
		FISCALYEAR,
		FISCALWEEK,
		BALANCE,
		ROW_NUMBER() OVER (
				PARTITION BY
				  external_customer_id, snapshot_date
				ORDER BY
				  last_activity desc nulls last, balance desc 
			  ) AS rn
		FROM loyalty.jc_prd.prd_jc_customers_snapshot ss
		INNER JOIN cdp_jc.PUBLIC.timesummary t ON DATE(ss.snapshot_date) = DATE(t.dateinunix)+1 AND t.fiscaldayofweek = 7
	),

	jc_final_unique_loyalty AS (
		SELECT DISTINCT EXTERNAL_CUSTOMER_ID, FISCALYEAR, FISCALWEEK, TOP_TIER_NAME
		FROM GET_TIER_DETAIL
		WHERE rn = 1
	),

	DMD_TRANS AS (
		SELECT DISTINCT transactionid
			,ts.mastercustomerid
			,ts.c_loyaltyflag
			,ts.c_loyaltytier
			,t.id AS "Transaction Date Id"
			,o.type as channel
			,o.c_brand
			,OO."Id" AS "Organization ID"
			,CASE WHEN (c.c_employeeflag = ''Y'' and c.c_employeeenddate is not NULL) THEN 1
				ELSE 0 END AS "Employee Flag"
			,C_STATUSTYPEBRAND
			,sum(salerevenue) AS salerevenue
			,sum(quantity) AS quantity
			,sum(costbasis) AS costbasis
			,max(c_rewardsredeemed) AS rewards
			,ts.c_PLCCFLAG AS JCCC_Tender
			,ts.C_Cobrandflag AS CoBrandFlag
		FROM cdp_jc.PUBLIC.transactionsummary ts
		LEFT JOIN cdp_jc.public.customersummary c on ts.mastercustomerid = c.mastercustomerid
		LEFT JOIN cdp_jc.PUBLIC.organizationsummary o on ts.organizationid = o.id
		LEFT JOIN RPT."ORGANIZATION SUMMARY" OO on ts.organizationid = OO.organizationid
		LEFT JOIN cdp_jc.public.timesummary t on date(ts.transactiondate) = date(t.dateinunix)
		WHERE ts.subtype IN (
				''Shipped''
				,''Demand''
				)
		AND fiscalyear >= 2022
		GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,15,16
	),

	ALL_REWARD AS
	(
		SELECT
		  DISTINCT ORDER_ID,
		  EXTERNAL_CUSTOMER_ID,
		  LOYALTY_CUSTOMER_ID, 
		  TRANSACTION_DATE,
		  REWARD_CODE AS REWARD_CODE,
		  ''transactional_reward1'' AS POS_CODE,
		  TIER_AT_EVENT
		FROM
		  (
			SELECT 
			  ORDER_ID,
			  EXTERNAL_CUSTOMER_ID,
			  LOYALTY_CUSTOMER_ID, 
			  TRANSACTION_DATE,
			  type,
			  TIER_AT_EVENT,
			  TRIM(t.VALUE) AS REWARD_CODE
			FROM
			  "LOYALTY"."JC_PRD"."PRD_JC_EVENTS",
			LATERAL SPLIT_TO_TABLE(REWARD_CODE, '','') t
		  ) a
		WHERE ORDER_ID IS NOT NULL
		AND REWARD_CODE IS NOT NULL
		AND type = ''purchase''
		GROUP BY ALL

		UNION ALL

		SELECT 
		  DISTINCT ORDER_ID,
		  EXTERNAL_CUSTOMER_ID,
		  LOYALTY_CUSTOMER_ID, 
		  TRANSACTION_DATE,
		  REWARD_CODE,
		  POS_CODE,
		  TIER_AT_EVENT
		FROM
		  (
			SELECT 
			  DISTINCT ORDER_ID,
			  EXTERNAL_CUSTOMER_ID,
			  LOYALTY_CUSTOMER_ID, 
			  TRANSACTION_DATE,
			  TYPE,
			  TIER_AT_EVENT,
			  TRIM(T.VALUE) AS REWARD_CODE,
			  TRIM(L.VALUE) AS POS_CODE
			FROM
			  "LOYALTY"."JC_PRD"."PRD_JC_EVENTS",
			  LATERAL SPLIT_TO_TABLE(OFFERS_REDEEMED, '','') T,
			  LATERAL SPLIT_TO_TABLE(OFFER_POS_CODE, '','') L
			  WHERE T.INDEX = L.INDEX
		  )
		WHERE ORDER_ID IS NOT NULL
		AND REWARD_CODE IS NOT NULL
		AND TYPE = ''purchase''
	),
		 MAP_CUST AS (
		select DISTINCT c.mastercustomerid, i.external_Customer_Id,c.c_Plccflag,c.c_cobrandflag
		from loyalty.jccc_prd.prd_jccc_identities as i
		join 
			(select distinct 
				cs.mastercustomerid, 
				cs.c_plccpartnerid as plccpartnerid, 
				c.c_loyaltyid as loyaltyid ,
				cs.c_plccflag,
				cs.c_cobrandflag
			from cdp_jc.public.customersummary as cs 
			left join cdp_jc.public.mastercustomer as mc on mc.mastercustomerid = cs.mastercustomerid 
			left join cdp_jc.public.customer as c on c.id = mc.customerid 
			where cs.c_plccpartnerid is not NULL /* has a PLCC partnerid */
				and c.c_loyaltyid is not NULL /* has a loyaltyid */) as c on c.plccpartnerid = i.identity_value and c.loyaltyid = i.external_customer_id
		where i.identity_type = ''plcc''
		UNION
		select DISTINCT c.mastercustomerid, i.external_Customer_Id,c.c_Plccflag,c.c_cobrandflag
		from loyalty.jccc_prd.prd_jccc_identities as i
		join 
			(select distinct 
				cs.mastercustomerid, 
				cs.c_cobrandpartnerid as cobrandpartnerid, 
				c.c_loyaltyid as loyaltyid ,
				cs.c_plccflag,
				cs.c_cobrandflag
			from cdp_jc.public.customersummary as cs 
			left join cdp_jc.public.mastercustomer as mc on mc.mastercustomerid = cs.mastercustomerid 
			left join cdp_jc.public.customer as c on c.id = mc.customerid 
			where cs.c_cobrandpartnerid is not NULL /* has a Cobrand partnerid */
				and c.c_loyaltyid is not NULL /* has a loyaltyid */) as c on c.cobrandpartnerid = i.identity_value and c.loyaltyid = i.external_customer_id
		where i.identity_type = ''cob'')

	,USED_COUPONS_AFTER_NOV2023 AS
	(
		SELECT 
		CU.mastercustomerid AS "MCID"
    		,rd.ID AS "Issue Date Id"
    		,''JCFA'' AS "Brand"
			,TS.c_brand AS "Redemption Brand"
			,COALESCE(CSU.TOP_TIER_NAME, CASE LOWER(R.TIER_AT_EVENT) WHEN ''green'' THEN ''Green'' WHEN ''navy'' THEN ''Navy'' WHEN ''gold'' THEN ''Gold'' END, ''Non-Loyalty'') AS "Loyalty Tier"
			,COALESCE(C."CODE", R.ORDER_ID) AS "Code"
			,COALESCE(C.POS_CODE, R.POS_CODE, CASE WHEN C."CODE" LIKE ''6858%'' THEN ''fa-rtl-acquisition'' WHEN C."CODE" LIKE ''6848%'' THEN ''20-FP-FACTORY'' END) AS "Reward Name"
			,C."STATUS" AS "Status"
			,C.LOYALTY_COUPON_ID AS "Loyalty Coupan Id"
			,ed.ID AS "Expiration Date Id"
			,ud.ID AS "Used Date Id"
			,td.ID AS "Transaction Date Id"
			,"Organization ID"
			,"Employee Flag"
			,DATEDIFF(day, DATE (C.REDEEMED_AT), DATE (C.USED_AT)) AS "Days to Usage"
			,CASE WHEN TS.channel = ''Digital'' THEN ''Direct'' WHEN TS.channel = ''Physical'' THEN ''Retail'' END AS "Distribution Channel"
			,CASE C_STATUSTYPEBRAND WHEN ''New'' THEN 1 WHEN ''Continuing'' THEN 2 WHEN ''Reactivated'' THEN 3 END AS "Status Type"
			,SUM(CASE WHEN R.REWARD_CODE IS NOT NULL THEN TS.SALEREVENUE END) AS "DMD Revenue"
    		,SUM(CASE WHEN R.REWARD_CODE IS NOT NULL THEN TS.QUANTITY END) AS "DMD Units"
    		,SUM(CASE WHEN R.REWARD_CODE IS NOT NULL THEN TS.COSTBASIS END) AS "DMD Cost Basis"
    		,SUM(CASE WHEN R.REWARD_CODE IS NOT NULL THEN TS.Rewards END) AS "Rewards Amount"
    		,R.ORDER_ID AS "Transaction ID"
			,CASE WHEN TS.JCCC_Tender IN (1,3) OR TS.CoBrandFlag = 1 THEN ''Yes'' ELSE ''No'' END AS "JCCC Tender"
			,CASE WHEN CU.c_plccflag = ''Y'' OR CU.c_cobrandflag = ''Y'' THEN ''Yes'' ELSE ''No'' END AS "JCCC Cardholder"
		FROM (
				SELECT * FROM ALL_REWARD
    		) R
		LEFT JOIN (
    		   SELECT DISTINCT *
				FROM LOYALTY.JC_PRD.PRD_JC_COUPONS
		) C ON C.LOYALTY_CUSTOMER_ID = R.LOYALTY_CUSTOMER_ID AND C.EXTERNAL_CUSTOMER_ID = R.EXTERNAL_CUSTOMER_ID AND C.code = R.REWARD_CODE
		LEFT JOIN MAP_CUST CU ON CU.external_customer_id=C.external_customer_id
		LEFT JOIN DMD_TRANS TS ON R.ORDER_ID = TS.TRANSACTIONID
		LEFT JOIN cdp_jc.public.customersummary cs ON cs.mastercustomerid = TS.mastercustomerid
		LEFT JOIN cdp_jc.public.timesummary rd ON DATE(C.REDEEMED_AT) = DATE(rd.dateinunix)
		LEFT JOIN jc_final_unique_loyalty CSU ON C.EXTERNAL_CUSTOMER_ID = csu.EXTERNAL_CUSTOMER_ID AND CSU.FISCALYEAR = rd.FISCALYEAR AND CSU.FISCALWEEK = rd.FISCALWEEK
		LEFT JOIN cdp_jc.public.timesummary ed ON DATE(C.EXPIRES_AT) = DATE(ed.dateinunix)
		LEFT JOIN cdp_jc.public.timesummary ud ON DATE(C.USED_AT) = DATE(ud.dateinunix)
		LEFT JOIN cdp_jc.public.timesummary td ON DATE(R.TRANSACTION_DATE) = DATE(td.dateinunix)      
		GROUP BY ALL
	),

	USED_COUPONS_BEFORE_NOV2023 AS
	(
		SELECT
		CU.mastercustomerid AS "MCID"
		,rd.ID AS "Issue Date Id"
		,''JCFA'' AS "Brand"
		,TS.c_brand AS "Redemption Brand"
		,COALESCE(R.TIER
		,CASE 
			WHEN TS.c_loyaltytier LIKE ''%GREEN%''
				THEN ''Green''
			WHEN TS.c_loyaltytier LIKE ''%NAVY%''
				THEN ''Navy''
			WHEN TS.c_loyaltytier LIKE ''%GOLD%''
				THEN ''Gold''
			WHEN TS.c_loyaltytier IS NULL AND cs.c_loyaltysignupdate IS NOT NULL
			THEN ''Green'' END
		,CSU.TOP_TIER_NAME, ''Non-Loyalty'') AS "Loyalty Tier"
		,C."CODE" AS "Code"
		,COALESCE(C.POS_CODE, CASE WHEN C."CODE" LIKE ''6858%'' THEN ''fa-rtl-acquisition'' WHEN C."CODE" LIKE ''6848%'' THEN ''20-FP-FACTORY'' END) AS "Reward Name"
		,C."STATUS" AS "Status"
		,C.LOYALTY_COUPON_ID AS "Loyalty Coupan Id"
		,ed.ID AS "Expiration Date Id"
		,ud.ID AS "Used Date Id"
		,ud.ID AS "Transaction Date Id"
		,"Organization ID"
		,"Employee Flag"
		,DATEDIFF(day, DATE (C.REDEEMED_AT), DATE (C.USED_AT)) AS "Days to Usage"
		,CASE WHEN TS.channel = ''Digital'' THEN ''Direct'' WHEN TS.channel = ''Physical'' THEN ''Retail'' END AS "Distribution Channel"
		,CASE C_STATUSTYPEBRAND WHEN ''New'' THEN 1 WHEN ''Continuing'' THEN 2 WHEN ''Reactivated'' THEN 3 END AS "Status Type"
		,SUM(CASE WHEN R.REWARDS IS NOT NULL THEN TS.SALEREVENUE END) AS "DMD Revenue"
		,SUM(CASE WHEN R.REWARDS IS NOT NULL THEN TS.QUANTITY END) AS "DMD Units"
		,SUM(CASE WHEN R.REWARDS IS NOT NULL THEN TS.COSTBASIS END) AS "DMD Cost Basis"
		,SUM(CASE WHEN R.REWARDS IS NOT NULL THEN TS.Rewards END) AS "Rewards Amount"
		,TS.TRANSACTIONID AS "Transaction ID"
				,CASE WHEN TS.JCCC_Tender IN (1,3) OR TS.CoBrandFlag = 1 THEN ''Yes'' ELSE ''No'' END AS "JCCC Tender"
			,CASE WHEN CU.c_plccflag = ''Y'' OR CU.c_cobrandflag = ''Y'' THEN ''Yes'' ELSE ''No'' END AS "JCCC Cardholder"
		FROM 
		(
			SELECT DISTINCT *
    		FROM LOYALTY.JC_PRD.PRD_JC_COUPONS
			WHERE CODE NOT IN (SELECT DISTINCT "Code" FROM USED_COUPONS_AFTER_NOV2023)
		) C
		INNER JOIN (
			SELECT 
			DISTINCT 
			rewards
			,distr_chan
			,transactionid
			,BRAND
			,tier
			FROM LOYALTY.JC_PRD.T_LOY_TRANS_REWARDS
			WHERE BRAND <> ''MW''
			AND ORDERDATE <= ''20230803''
		) R ON lower(C."CODE") = lower(R.REWARDS)
			LEFT JOIN MAP_CUST CU ON CU.external_customer_id=C.external_customer_id
		LEFT JOIN DMD_TRANS TS
			ON R.TRANSACTIONID = TS.TRANSACTIONID
		LEFT JOIN cdp_jc.public.customersummary cs ON cs.mastercustomerid = TS.mastercustomerid
		LEFT JOIN cdp_jc.public.timesummary rd ON DATE(C.REDEEMED_AT) = DATE(rd.dateinunix)
		LEFT JOIN jc_final_unique_loyalty CSU ON C.EXTERNAL_CUSTOMER_ID = csu.EXTERNAL_CUSTOMER_ID AND CSU.FISCALYEAR = rd.FISCALYEAR AND CSU.FISCALWEEK = rd.FISCALWEEK
		LEFT JOIN cdp_jc.public.timesummary ed ON DATE(C.EXPIRES_AT) = DATE(ed.dateinunix)
		LEFT JOIN cdp_jc.public.timesummary ud ON DATE(C.USED_AT) = DATE(ud.dateinunix)
		GROUP BY ALL
	),

	COUPON_USED_BUT_NO_TRANSACTION AS
	(
		SELECT 
			DISTINCT
			CU.mastercustomerid AS "MCID"
    		,rd.ID AS "Issue Date Id"
    		,''JCFA'' AS "Brand"
			,NULL AS "Redemption Brand"
			,COALESCE(CSU.TOP_TIER_NAME, ''Non-Loyalty'') AS "Loyalty Tier"
			,C."CODE" AS "Code"
			,COALESCE(C.POS_CODE, CASE WHEN C."CODE" LIKE ''6858%'' THEN ''fa-rtl-acquisition'' WHEN C."CODE" LIKE ''6848%'' THEN ''20-FP-FACTORY'' END) AS "Reward Name"
			,C."STATUS" AS "Status"
			,C.LOYALTY_COUPON_ID AS "Loyalty Coupan Id"
			,ed.ID AS "Expiration Date Id"
			,ud.ID AS "Used Date Id"
			,CASE WHEN ud.ID <= ''20230803'' THEN ud.ID ELSE NULL END AS "Transaction Date Id"
			,NULL AS "Organization ID"
			,0 AS "Employee Flag"
			,NULL AS "Status Type"
			,DATEDIFF(day, DATE (C.REDEEMED_AT), DATE (C.USED_AT)) AS "Days to Usage"
			,NULL AS "Distribution Channel"
			,NULL AS "DMD Revenue"
			,NULL AS "DMD Units"
			,NULL AS "DMD Cost Basis"
			,NULL AS "Rewards Amount"
			,NULL AS "Transaction ID"
			,''No'' AS "JCCC Tender"
			,CASE WHEN CU.c_plccflag = ''Y'' OR CU.c_cobrandflag = ''Y'' THEN ''Yes'' ELSE ''No'' END AS "JCCC Cardholder"
		FROM (
				SELECT DISTINCT *
				FROM LOYALTY.JC_PRD.PRD_JC_COUPONS
				WHERE CODE NOT IN (SELECT DISTINCT "Code" FROM USED_COUPONS_AFTER_NOV2023)
				AND CODE NOT IN (SELECT DISTINCT "Code" FROM USED_COUPONS_BEFORE_NOV2023)
    		) C
			LEFT JOIN MAP_CUST CU ON CU.external_customer_id=C.external_customer_id
		LEFT JOIN cdp_jc.public.timesummary rd ON DATE(C.REDEEMED_AT) = DATE(rd.dateinunix)
		LEFT JOIN jc_final_unique_loyalty CSU ON C.EXTERNAL_CUSTOMER_ID = csu.EXTERNAL_CUSTOMER_ID AND CSU.FISCALYEAR = rd.FISCALYEAR AND CSU.FISCALWEEK = rd.FISCALWEEK
		LEFT JOIN cdp_jc.public.timesummary ed ON DATE(C.EXPIRES_AT) = DATE(ed.dateinunix)
		LEFT JOIN cdp_jc.public.timesummary ud ON DATE(C.USED_AT) = DATE(ud.dateinunix)
	)


	SELECT
	"MCID"
	,"Issue Date Id"
	,"Brand"
	,"Redemption Brand"
	,"Loyalty Tier"
	,CASE 
		WHEN "Brand" = ''JCFA''
			THEN 3
		END AS "Brand Sort"
	,"Code"
	,"Reward Name"
	,"Status"
	,"Loyalty Coupan Id"
	,"Expiration Date Id"
	,"Used Date Id"
	,"Transaction Date Id"
	,"Organization ID"
	,coalesce("Employee Flag", 0) AS "Employee Flag"
	,"Days to Usage"
	,"Distribution Channel"
	,"Status Type"
	,"DMD Revenue"
	,"DMD Units"
	,"DMD Cost Basis"
	,"Rewards Amount"
	,"Transaction ID"
	,"JCCC Tender"
	,"JCCC Cardholder"
	, CASE WHEN "JCCC Tender"=''Yes'' AND "JCCC Cardholder"=''Yes'' THEN ''Yes'' ELSE ''No'' END AS "JCCC Customers / Cardholders filter"
	FROM 
	USED_COUPONS_BEFORE_NOV2023

	UNION

	SELECT
	"MCID"
	,"Issue Date Id"
	,"Brand"
	,"Redemption Brand"
	,"Loyalty Tier"
	,CASE 
		WHEN "Brand" = ''JCFA''
			THEN 3
		END AS "Brand Sort"
	,"Code"
	,"Reward Name"
	,"Status"
	,"Loyalty Coupan Id"
	,"Expiration Date Id"
	,"Used Date Id"
	,"Transaction Date Id"
	,"Organization ID"
	,coalesce("Employee Flag", 0) AS "Employee Flag"
	,"Days to Usage"
	,"Distribution Channel"
	,"Status Type"
	,"DMD Revenue"
	,"DMD Units"
	,"DMD Cost Basis"
	,"Rewards Amount"
	,"Transaction ID"
	,"JCCC Tender"
	,"JCCC Cardholder"
	, CASE WHEN "JCCC Tender"=''Yes'' AND "JCCC Cardholder"=''Yes'' THEN ''Yes'' ELSE ''No'' END AS "JCCC Customers / Cardholders filter"
	FROM
	USED_COUPONS_AFTER_NOV2023

	UNION

	SELECT 
	"MCID"
	,"Issue Date Id"
	,"Brand"
	,"Redemption Brand"
	,"Loyalty Tier"
	,CASE 
		WHEN "Brand" = ''JCFA''
			THEN 3
		END AS "Brand Sort"
	,"Code"
	,"Reward Name"
	,"Status"
	,"Loyalty Coupan Id"
	,"Expiration Date Id"
	,"Used Date Id"
	,"Transaction Date Id"
	,"Organization ID"
	,coalesce("Employee Flag", 0) AS "Employee Flag"
	,"Days to Usage"
	,"Distribution Channel"
	,"Status Type"
	,"DMD Revenue"
	,"DMD Units"
	,"DMD Cost Basis"
	,"Rewards Amount"
	,"Transaction ID"
	,"JCCC Tender"
	,"JCCC Cardholder"
	, CASE WHEN "JCCC Tender"=''Yes'' AND "JCCC Cardholder"=''Yes'' THEN ''Yes'' ELSE ''No'' END AS "JCCC Customers / Cardholders filter"
	FROM
	COUPON_USED_BUT_NO_TRANSACTION;

	DROP TABLE IF EXISTS RPT."PASSPORT COUPON STATS AND USAGE";

	ALTER TABLE Temp."PASSPORT COUPON STATS AND USAGE" RENAME TO RPT."PASSPORT COUPON STATS AND USAGE";

	RETURN ''TABLE RPT."PASSPORT COUPON STATS AND USAGE" created or replaced successfully.'';
	END
	';