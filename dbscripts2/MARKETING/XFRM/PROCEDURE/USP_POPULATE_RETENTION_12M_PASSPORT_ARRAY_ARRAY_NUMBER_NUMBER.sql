CREATE OR REPLACE PROCEDURE "USP_POPULATE_RETENTION_12M_PASSPORT"("START_DATE" ARRAY, "END_DATE" ARRAY, "FISCAL_YEAR" NUMBER(38,0), "FISCAL_MONTH" NUMBER(38,0))
RETURNS VARCHAR(20)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN

INSERT INTO RPT."PASSPORT RETENTION L12"
(
    "MasterCustomerId",
    "Brand",
    "Transaction Id",
    "Units",
    "Sale Revenue",
    Margin,
    "Gross Margin",
    period,
    "Loyalty Tier",
    Channel,
    "FP",
    "Lapsed Status",
    "Status Type",
    "Last Status Type",
    "Year-Month"
)

WITH JCDATA AS (
        SELECT
            MAX(T.id) AS "Date Key",
            O.LEVEL1NAME,
            TS.mastercustomerid AS "MasterCustomerId",
            CASE
                WHEN C_FPMDITEM = ''F'' THEN 1
                WHEN C_FPMDITEM = ''P'' THEN 2
                WHEN C_FPMDITEM = ''M'' THEN 3
            END AS "Price Type",
            CASE WHEN O.TYPE = ''Physical'' THEN 1
            WHEN O.TYPE = ''Digital'' THEN 2
            ELSE 3 END AS Channel,
            TS.TRANSACTIONID AS "Transaction Id",
            SUM(Quantity) AS "Units",
            SUM(SALEREVENUE) AS "Sale Revenue",
            SUM(SALEREVENUE - COSTBASIS) as Margin,
            SUM(
                SALEREVENUE - COALESCE(COSTBASIS, 0) + COALESCE(SHIPPINGCOST, 0) + COALESCE(OTHERCOST, 0)
            ) AS "Gross Margin",
            CASE
                WHEN DATE(ts.transactiondate) >= DATE(:START_DATE[0])
                AND DATE(ts.transactiondate) <= DATE(:END_DATE[0]) THEN ''TY''
                WHEN DATE(ts.transactiondate) >= DATE(:START_DATE[1])
                AND DATE(ts.transactiondate) <= DATE(:END_DATE[1]) THEN ''LY''
                WHEN DATE(ts.transactiondate) >= DATE(:START_DATE[2])
                AND DATE(ts.transactiondate) <= DATE(:END_DATE[2]) THEN ''LLY''
            END AS period
        FROM
            cdp_jc.PUBLIC.transactionsummary ts
            LEFT JOIN CDP_JC.PUBLIC.CUSTOMERSUMMARY C ON TS.MASTERCUSTOMERID = C.MASTERCUSTOMERID
            LEFT JOIN cdp_jc.PUBLIC.timesummary t ON DATE(ts.transactiondate) = DATE (t.dateinunix)
            LEFT JOIN cdp_jc.public.organizationsummary o on o.id = ts.organizationid
        WHERE
            TS.SUBTYPE IN (''Shipped'', ''Demand'')
            AND BUYERIDENTIFICATION = ''Identified''
            AND TO_DATE(TRANSACTIONTIMESTAMP) >= DATE(:START_DATE[2])
            AND DATE(ts.transactiondate) <= DATE(:END_DATE[0])
        GROUP BY
            ALL
    ),

    -- Tier LookUp

    Get_Max_Transaction_Date AS(
        SELECT
        "MasterCustomerId",
        period,
        MAX("Date Key") AS "Date Key"
        FROM
        JCDATA
        GROUP BY ALL
    ),


    snapshot_date_lly AS
    (
        SELECT
            CASE WHEN
            (SELECT
            COUNT(1)
            FROM
            loyalty.jc_prd.prd_jc_customers_snapshot
            WHERE date(snapshot_date) = :END_DATE[2]) <> 0 THEN :END_DATE[2] ELSE (SELECT MIN(SNAPSHOT_DATE) FROM loyalty.jc_prd.prd_jc_customers_snapshot) END AS snapshot_date
    ),

    snapshot_date_ly AS
    (
        SELECT
            CASE WHEN
            (SELECT
            COUNT(1)
            FROM
            loyalty.jc_prd.prd_jc_customers_snapshot
            WHERE date(snapshot_date) = :END_DATE[1]) <> 0 THEN DATEADD(DAY, 1, :END_DATE[1]) ELSE (SELECT MIN(SNAPSHOT_DATE) FROM loyalty.jc_prd.prd_jc_customers_snapshot) END AS snapshot_date
    ),

	
	Get_Tier AS (
            SELECT
                ''TY'' AS Period,
                mastercustomerid AS "MasterCustomer ID",
                Loyalty_Tier AS "Loyalty Tier"
            FROM (
                SELECT
                    DISTINCT cs.mastercustomerid,
                    c.external_customer_id AS loyalty_id,
                    c.top_tier_name AS loyalty_tier, 
                    DATE(c.snapshot_date) AS date, 
                    CASE 
                        WHEN c.top_tier_name = ''Green'' THEN 1
                        WHEN c.top_tier_name = ''Navy'' THEN 2
                        WHEN c.top_tier_name = ''Gold'' THEN 3
                    END AS tier_sort
                FROM loyalty.jc_prd.prd_jc_customers_snapshot AS c
                JOIN cdp_jc.public.customersummary AS cs ON cs.c_loyaltyid = c.external_customer_id
                WHERE DATE(c.snapshot_date) = DATEADD(DAY, 1, :END_DATE[0])
                QUALIFY ROW_NUMBER() OVER (PARTITION BY loyalty_id ORDER BY tier_sort DESC) = 1
            )
            
            UNION ALL
            
            SELECT
                ''LY'' AS Period,
                mastercustomerid AS "MasterCustomer ID",
                Loyalty_Tier AS "Loyalty Tier"
            FROM (
                SELECT
                    DISTINCT cs.mastercustomerid,
                    c.external_customer_id AS loyalty_id,
                    c.top_tier_name AS loyalty_tier, 
                    DATE(c.snapshot_date) AS date, 
                    CASE 
                        WHEN c.top_tier_name = ''Green'' THEN 1
                        WHEN c.top_tier_name = ''Navy'' THEN 2
                        WHEN c.top_tier_name = ''Gold'' THEN 3
                    END AS tier_sort
                FROM loyalty.jc_prd.prd_jc_customers_snapshot AS c
                JOIN cdp_jc.public.customersummary AS cs ON cs.c_loyaltyid = c.external_customer_id
               WHERE DATE(c.snapshot_date) = (select max(snapshot_date) from snapshot_date_ly)
                QUALIFY ROW_NUMBER() OVER (PARTITION BY loyalty_id ORDER BY tier_sort DESC) = 1
            )
            
            UNION ALL
            
            SELECT
                ''LLY'' AS Period,
                mastercustomerid AS "MasterCustomer ID",
                Loyalty_Tier AS "Loyalty Tier"
            FROM (
                SELECT
                    DISTINCT cs.mastercustomerid,
                    c.external_customer_id AS loyalty_id,
                    c.top_tier_name AS loyalty_tier, 
                    DATE(c.snapshot_date) AS date, 
                    CASE 
                        WHEN c.top_tier_name = ''Green'' THEN 1
                        WHEN c.top_tier_name = ''Navy'' THEN 2
                        WHEN c.top_tier_name = ''Gold'' THEN 3
                    END AS tier_sort
                FROM loyalty.jc_prd.prd_jc_customers_snapshot AS c
                JOIN cdp_jc.public.customersummary AS cs ON cs.c_loyaltyid = c.external_customer_id
                WHERE DATE(c.snapshot_date) = (select max(snapshot_date) from snapshot_date_lly)
                QUALIFY ROW_NUMBER() OVER (PARTITION BY loyalty_id ORDER BY tier_sort DESC) = 1
            )
        ),
        

		Get_Customer_Tier AS(
			SELECT
			DISTINCT
			"MasterCustomerId",
			GMT.period,
			CASE WHEN "Loyalty Tier" = ''Green'' THEN 6
			WHEN "Loyalty Tier" = ''Navy'' THEN 7
			WHEN "Loyalty Tier" = ''Gold'' THEN 8
			ELSE 9 END AS "Loyalty Tier"
			FROM
			Get_Max_Transaction_Date GMT
			LEFT JOIN Get_Tier GT ON "MasterCustomerId" = "MasterCustomer ID" AND GT.Period = GMT.Period
		),

    --------------------------------------- CHANNEL LOOKUP ------------------------------------------

    
    customer_channel as (
        SELECT
            "MasterCustomerId",
            LEVEL1NAME,
            ''TY'' AS period,
            MAX(
                CASE
                    WHEN Channel = 1 THEN 1
                    ELSE 0
                END
            ) AS Is_Physical,
            MAX(
                CASE
                    WHEN Channel = 2 THEN 1
                    ELSE 0
                END
            ) AS Is_Digital
        FROM
            JCDATA
        WHERE
            period = ''TY''
        GROUP BY
            ALL
            
        UNION ALL
        
        SELECT
            "MasterCustomerId",
            LEVEL1NAME,
            ''LY'' AS period,
            MAX(
                CASE
                    WHEN Channel = 1 THEN 1
                    ELSE 0
                END
            ) AS Is_Physical,
            MAX(
                CASE
                    WHEN Channel = 2 THEN 1
                    ELSE 0
                END
            ) AS Is_Digital
        FROM
            JCDATA
        WHERE
            period = ''LY''
        GROUP BY
            ALL
            
        UNION ALL
        
        SElECT
            "MasterCustomerId",
            LEVEL1NAME,
            ''LLY'' AS period,
            MAX(
                CASE
                    WHEN Channel = 1 THEN 1
                    ELSE 0
                END
            ) AS Is_Physical,
            MAX(
                CASE
                    WHEN Channel = 2 THEN 1
                    ELSE 0
                END
            ) AS Is_Digital
        FROM
            JCDATA
        WHERE
            period = ''LLY''
        GROUP BY
            ALL
    ),
    Final_Channel AS (
        SELECT
            "MasterCustomerId",
            LEVEL1NAME,
            period,
            CASE
                WHEN Is_Physical = 1
                AND Is_Digital = 0 THEN 1
                WHEN Is_Physical = 0
                AND Is_Digital = 1 THEN 2
                WHEN Is_Physical = 1
                AND Is_Digital = 1 THEN 3
            END AS channel
        FROM
            customer_channel
        GROUP BY
            ALL
    ),

    
    --------------------------------------- FULL PRICE LOOKUP ------------------------------------------

    
    customer_fp as (
        SELECT
            "MasterCustomerId",
            LEVEL1NAME,
            ''TY'' AS period,
            MAX(
                CASE
                    WHEN "Price Type" = 1 THEN 1
                    ELSE 0
                END
            ) AS Is_FP,
            MAX(
                CASE
                    WHEN "Price Type" = 2 THEN 1
                    ELSE 0
                END
            ) AS Is_PR,
            MAX(
                CASE
                    WHEN "Price Type" = 3 THEN 1
                    ELSE 0
                END
            ) AS Is_MD
        FROM
            JCDATA
        WHERE
            period = ''TY''
        GROUP BY
            ALL
        UNION ALL
        SELECT
            "MasterCustomerId",
            LEVEL1NAME,
            ''LY'' AS period,
            MAX(
                CASE
                    WHEN "Price Type" = 1 THEN 1
                    ELSE 0
                END
            ) AS Is_FP,
            MAX(
                CASE
                    WHEN "Price Type" = 2 THEN 1
                    ELSE 0
                END
            ) AS Is_PR,
            MAX(
                CASE
                    WHEN "Price Type" = 3 THEN 1
                    ELSE 0
                END
            ) AS Is_MD
        FROM
            JCDATA
        WHERE
            period = ''LY''
        GROUP BY
            ALL
            
        UNION ALL
        
        SElECT
            "MasterCustomerId",
            LEVEL1NAME,
            ''LLY'' AS period,
            MAX(
                CASE
                    WHEN "Price Type" = 1 THEN 1
                    ELSE 0
                END
            ) AS Is_FP,
            MAX(
                CASE
                    WHEN "Price Type" = 2 THEN 1
                    ELSE 0
                END
            ) AS Is_PR,
            MAX(
                CASE
                    WHEN "Price Type" = 3 THEN 1
                    ELSE 0
                END
            ) AS Is_MD
        FROM
            JCDATA
        WHERE
            period = ''LLY''
        GROUP BY
            ALL
    ),
    Final_fp AS (
        SELECT
            "MasterCustomerId",
            LEVEL1NAME,
            period,
            CASE
                WHEN Is_fp = 1
                AND Is_PR = 0
                AND Is_MD = 0 THEN 1
                WHEN Is_fp = 0
                AND Is_PR = 0
                AND Is_MD = 1 THEN 2
                WHEN Is_fp = 0
                AND Is_PR = 1
                AND Is_MD = 0 THEN 3
                WHEN Is_fp = 1
                AND Is_PR = 1
                AND Is_MD = 0 THEN 4
                WHEN Is_fp = 1
                AND Is_PR = 0
                AND Is_MD = 1 THEN 5
                WHEN Is_fp = 0
                AND Is_PR = 1
                AND Is_MD = 1 THEN 6
                WHEN Is_fp = 1
                AND Is_PR = 1
                AND Is_MD = 1 THEN 7
            END AS FP
        FROM
            customer_fp
        GROUP BY
            ALL
    ),

    --------------------------------------- STATUS LOOKUP ------------------------------------------
    
    Check_Period AS
    (
       SELECT 
            DISTINCT
            CASE
                WHEN DATE(TRANSACTIONDATE) >= DATE(:START_DATE[0]) AND DATE(TRANSACTIONDATE) <= DATE(:END_DATE[0]) THEN ''TY''
                WHEN DATE(TRANSACTIONDATE) >= DATE(:START_DATE[1]) AND DATE(TRANSACTIONDATE) <= DATE(:END_DATE[1]) THEN ''LY''
                WHEN DATE(TRANSACTIONDATE) >= DATE(:START_DATE[2]) AND DATE(TRANSACTIONDATE) <= DATE(:END_DATE[2]) THEN ''2LY''
                WHEN DATE(TRANSACTIONDATE) >= DATE(:START_DATE[3]) AND DATE(TRANSACTIONDATE) <= DATE(:END_DATE[3]) THEN ''3LY''
                WHEN DATE(TRANSACTIONDATE) >= DATE(:START_DATE[4]) AND DATE(TRANSACTIONDATE) <= DATE(:END_DATE[4]) THEN ''4LY''
                WHEN DATE(TRANSACTIONDATE) >= DATE(:START_DATE[5]) AND DATE(TRANSACTIONDATE) <= DATE(:END_DATE[5]) THEN ''5LY''
                WHEN DATE(TRANSACTIONDATE) >= DATE(:START_DATE[6]) AND DATE(TRANSACTIONDATE) <= DATE(:END_DATE[6]) THEN ''6LY''
                WHEN DATE(TRANSACTIONDATE) >= DATE(:START_DATE[7]) AND DATE(TRANSACTIONDATE) <= DATE(:END_DATE[7]) THEN ''7LY''
            END AS period,
            MASTERCUSTOMERID,
            TRANSACTIONDATE,
            LEVEL1NAME
        FROM
            CDP_JC.PUBLIC.TRANSACTIONSUMMARY T
        LEFT JOIN
            CDP_JC.PUBLIC.ORGANIZATIONSUMMARY O ON O.ID = T.ORGANIZATIONID
        WHERE
            DATE(TRANSACTIONDATE) >= :START_DATE[7] 
            AND DATE(TRANSACTIONDATE) <= :END_DATE[0]
            AND T.SUBTYPE IN (''Shipped'', ''Demand'')
    ),
    
    Has_period AS
    (
        SELECT
        MASTERCUSTOMERID,
        LEVEL1NAME,
        MAX(
            CASE
                WHEN period = ''TY'' THEN 1
                ELSE 0
            END
        ) AS has_TY,
        MAX(
            CASE
                WHEN period = ''LY'' THEN 1
                ELSE 0
            END
        ) AS has_LY,
        MAX(
            CASE
                WHEN period = ''2LY'' THEN 1
                ELSE 0
            END
        ) AS has_2LY,
        MAX(
            CASE
                WHEN period = ''3LY'' THEN 1
                ELSE 0
            END
        ) AS has_3LY,
        MAX(
            CASE
                WHEN period = ''4LY'' THEN 1
                ELSE 0
            END
        ) AS has_4LY,
        MAX(
            CASE
                WHEN period = ''5LY'' THEN 1
                ELSE 0
            END
        ) AS has_5LY,
        MAX(
            CASE
                WHEN period = ''6LY'' THEN 1
                ELSE 0
            END
        ) AS has_6LY,
        MAX(
            CASE
                WHEN period = ''7LY'' THEN 1
                ELSE 0
            END
        ) AS has_7LY,
        FROM
        Check_Period
        GROUP BY 1, 2
    ),
    
    First_Status_Type AS 
    (
        SELECT
        MASTERCUSTOMERID AS "MasterCustomerId",
        LEVEL1NAME,
        ''TY'' AS period,
        CASE WHEN has_LY = 1 THEN 2 -- Continuing
        WHEN (has_LY + has_2LY + has_3LY + has_4LY + has_5LY) = 0 THEN 1 -- New
        WHEN has_LY = 0 and (has_2LY + has_3LY + has_4LY + has_5LY) > 0 THEN 3 END AS "Status Type", -- Reactivated
        NULL AS "Lapsed Status"
        FROM
        Has_period
        WHERE has_TY = 1
    
        UNION ALL
    
        SELECT
        MASTERCUSTOMERID AS "MasterCustomerId",
        LEVEL1NAME,
        ''LY'' AS period,
        CASE WHEN has_2LY = 1 THEN 2
        WHEN (has_2LY + has_3LY + has_4LY + has_5LY + has_6LY) = 0 THEN 1
        WHEN has_2LY = 0 and (has_3LY + has_4LY + has_5LY + has_6LY) > 0 THEN 3 END AS "Status Type",
        CASE WHEN has_TY = 1 THEN 1
        WHEN has_TY = 0 THEN -1 END AS "Lapsed Status"
        FROM
        Has_period
        WHERE has_LY = 1
    
        UNION ALL
    
        SELECT
        MASTERCUSTOMERID AS "MasterCustomerId",
        LEVEL1NAME,
        ''LLY'' AS period,
        CASE WHEN has_3LY = 1 THEN 2
        WHEN (has_3LY + has_4LY + has_5LY + has_6LY + has_7LY) = 0 THEN 1
        WHEN has_3LY = 0 and (has_4LY + has_5LY + has_6LY + has_7LY) > 0 THEN 3 END AS "Status Type",
        CASE WHEN has_LY = 1 THEN 1
        WHEN has_LY = 0 THEN -1 END AS "Lapsed Status"
        FROM
        Has_period
        WHERE has_2LY = 1
    ),

    FINAL_FACT AS
    (
        SELECT
            M."MasterCustomerId",
            CASE WHEN M.LEVEL1NAME = ''JCrew'' THEN 2
            WHEN M.LEVEL1NAME = ''Factory'' THEN 3 END AS "Brand",
            COUNT(DISTINCT "Transaction Id") AS "Transaction Id",
            SUM("Units") AS "Units",
            SUM("Sale Revenue") AS "Sale Revenue",
            SUM(MARGIN) AS MARGIN,
            SUM("Gross Margin") AS "Gross Margin",
            M.period,
            TT."Loyalty Tier",
            C.Channel,
            P.FP AS "FP",
            -- CASE WHEN M.Period = ''LY'' and FST."Lapsed Status" = 1 THEN LT."Loyalty Tier" ELSE TT."Loyalty Tier" END AS "Loyalty Tier",
            -- CASE WHEN M.Period = ''LY'' and FST."Lapsed Status" = 1 THEN LC.Channel ELSE C.Channel END AS Channel,
            -- CASE WHEN M.Period = ''LY'' and FST."Lapsed Status" = 1 THEN LP.FP ELSE P.FP END AS "FP",
            FST."Lapsed Status" AS "Lapsed Status",
            FST."Status Type" AS "Status Type",
            CASE WHEN FST."Status Type" = 2 THEN LYS."Status Type" ELSE NULL END AS "Last Status Type",
            CONCAT(:FISCAL_YEAR, :FISCAL_MONTH)::INT AS "Year-Month"
        FROM
            JCDATA M
            LEFT JOIN Get_Customer_Tier TT on TT."MasterCustomerId" = M."MasterCustomerId" and TT.period = M.period
            LEFT JOIN Get_Customer_Tier LT on LT."MasterCustomerId" = M."MasterCustomerId" and LT.period = ''TY'' and M.period = ''LY''
            LEFT JOIN Final_Channel C ON C."MasterCustomerId" = M."MasterCustomerId" and C.period = M.period AND C.LEVEL1NAME = M.LEVEL1NAME
            LEFT JOIN Final_Channel LC ON LC."MasterCustomerId" = M."MasterCustomerId" and LC.period = ''TY'' and M.period = ''LY'' and LC.LEVEL1NAME = M.LEVEL1NAME
            LEFT JOIN Final_fp P ON P."MasterCustomerId" = M."MasterCustomerId" AND P.period = M.period AND P.LEVEL1NAME = M.LEVEL1NAME
            LEFT JOIN Final_fp LP ON LP."MasterCustomerId" = M."MasterCustomerId" and LP.period = ''TY'' and M.period = ''LY'' AND LP.LEVEL1NAME = M.LEVEL1NAME
            LEFT JOIN First_Status_Type FST ON FST."MasterCustomerId" = M."MasterCustomerId" AND FST.period = M.period AND FST.LEVEL1NAME = M.LEVEL1NAME
            LEFT JOIN First_Status_Type LYS ON LYS."MasterCustomerId" = M."MasterCustomerId" AND ((LYS.period = ''LY'' AND M.period = ''TY'') OR (LYS.period = ''LLY'' AND M.period = ''LY'')) AND LYS.LEVEL1NAME = M.LEVEL1NAME
            GROUP BY ALL
    )


    SELECT
    RANK() OVER(ORDER BY "MasterCustomerId") AS "MasterCustomerId",
    "Brand",
    "Transaction Id",
    "Units",
    "Sale Revenue",
    Margin,
    "Gross Margin",
    period,
    "Loyalty Tier",
    Channel,
    "FP",
    "Lapsed Status",
    "Status Type",
    "Last Status Type",
    "Year-Month"
    FROM
    FINAL_FACT;
    

RETURN ''Created'';

END;
';