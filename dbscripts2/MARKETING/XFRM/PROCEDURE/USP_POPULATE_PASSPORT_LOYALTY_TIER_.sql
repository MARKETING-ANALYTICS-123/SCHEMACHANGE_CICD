CREATE OR REPLACE PROCEDURE "USP_POPULATE_PASSPORT_LOYALTY_TIER"()
RETURNS VARCHAR(100)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    --Truncting the table to perform full Load
    create or replace TABLE Temp."PASSPORT LOYALTY TIER" (
      "Loyalty Tier" VARCHAR(16777216),
      "Loyalty Flag" VARCHAR(16777216),
      "Loyalty Tier Month" VARCHAR(16777216),
      "Loyalty Tier Quarter" VARCHAR(16777216),
      "Loyalty Tier Year" VARCHAR(16777216),
      "Loyalty Tier Year LY" VARCHAR(16777216),
      "Date Key" NUMBER(38,0),
      "MasterCustomer ID" VARCHAR(16777216),
      "MasterCustomer Transaction Sequence" NUMBER(38,0),
      "Last Modified Date" TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
    );

    -- Insert new data into the target table
    INSERT INTO Temp."PASSPORT LOYALTY TIER"(
        "Loyalty Tier",
        "Loyalty Flag",
        "Loyalty Tier Month",
        "Loyalty Tier Quarter",
        "Loyalty Tier Year",
        "Loyalty Tier Year LY",
        "Date Key",
        "MasterCustomer ID",
        "MasterCustomer Transaction Sequence"  
    )

    WITH TIER_WEEK AS
    (
        select  
        *  
        from  
            (select distinct 
                cs.mastercustomerid,  
                c.external_customer_id as loyalty_id,  
                c.top_tier_name as loyalty_tier, 
                t.fiscalyear,
                t.fiscalmonth,
                t.fiscalquarter,
                t.fiscalweek,
                date(t.dateinunix) as date,
                case when c.top_tier_name = ''Green'' then 1 when c.top_tier_name = ''Navy'' then 2 when c.top_tier_name = ''Gold'' then 3 end as tier_sort  
              from loyalty.jc_prd.prd_jc_customers_snapshot as c
              join cdp_jc.public.timesummary t ON DATE (c.snapshot_date) = DATE(t.dateinunix) + 1 and fiscalyear >= 2023 and fiscaldayofweek = 7
              join cdp_jc.public.customersummary as cs on cs.c_loyaltyid = c.external_customer_id
              where c.brand in (''JC'', ''jc'', ''FA'', ''fa''))
            qualify row_number() over (partition by loyalty_id, fiscalyear, fiscalweek order by tier_sort desc) = 1
    ),

    SNAPSHOT_DATE_MONTH AS
    (
        select 
        fiscalyear, 
        fiscalmonth, 
        max(dateinunix::date) as date
        from cdp_jc.public.timesummary
        where dateinunix::date <= date(''2025-07-26'')
        and fiscalyear >= 2023
        group by all
        order by date
    ),
    
    BY_MONTH AS
    (
        select
        *
        from
        TIER_WEEK
        WHERE
        date IN (select date from SNAPSHOT_DATE_MONTH)
    ),

    SNAPSHOT_DATE_QUARTER AS
    (
        select 
        fiscalyear, 
        fiscalquarter, 
        max(dateinunix::date) as date
        from cdp_jc.public.timesummary
        where dateinunix::date <= date(''2025-07-26'')
        and fiscalyear >= 2023
        group by all
    ),
    
    BY_QUARTER AS
    (
        select
        *
        from
        TIER_WEEK
        WHERE
        date IN (select date from SNAPSHOT_DATE_QUARTER)
    ), 

    SNAPSHOT_DATE_YEAR AS
    (
        select 
        fiscalyear,
        max(dateinunix::date) as date
        from cdp_jc.public.timesummary
        where dateinunix::date <= date(''2025-07-26'')
        and fiscalyear >= 2023
        group by all
    ),
    
    BY_YEAR AS
    (
        select
        *
        from
        TIER_WEEK
        WHERE
        date IN (select date from SNAPSHOT_DATE_YEAR)
    ),


    SNAPSHOT_DATE_YEAR_LY AS
    (
        select 
        fiscalyear,
        max(dateinunix::date) as date
        from cdp_jc.public.timesummary
        where fiscalyear = 2024
        and fiscalweek <= 25
        group by all
    ),
    
    BY_YEAR_LY AS
    (
        select
        *
        from
        TIER_WEEK
        WHERE
        date IN (select date from SNAPSHOT_DATE_YEAR_LY)
    )
    
     SELECT DISTINCT
             COALESCE(csu.loyalty_tier, ''Non-Loyalty'') AS "Loyalty Tier"
            ,CASE WHEN "Loyalty Tier" <> ''Non-Loyalty'' THEN ''Loyalty'' ELSE ''Non-Loyalty'' END AS "Loyalty Flag"
            ,COALESCE(csu_m.loyalty_tier, ''Non-Loyalty'') AS "Loyalty Tier Month"
            ,COALESCE(csu_q.loyalty_tier, ''Non-Loyalty'') AS "Loyalty Tier Quarter"
            ,COALESCE(csu_y.loyalty_tier, ''Non-Loyalty'') AS "Loyalty Tier Year"
            ,COALESCE(csu_y.loyalty_tier, ''Non-Loyalty'') AS "Loyalty Tier Year LY"
            ,T.id AS "Date Key"
            ,TS.mastercustomerid AS "MasterCustomer ID"
            ,TS.mastercustomertransactionsequence AS "MasterCustomer Transaction Sequence"
    FROM cdp_jc.PUBLIC.transactionsummary ts
        LEFT JOIN cdp_jc.public.timesummary t ON DATE(ts.transactiondate) = DATE(t.dateinunix)
        LEFT JOIN TIER_WEEK csu ON ts.mastercustomerid = csu.mastercustomerid and csu.fiscalyear = t.fiscalyear and csu.fiscalweek = t.fiscalweek
        LEFT JOIN BY_MONTH csu_m ON ts.mastercustomerid = csu_m.mastercustomerid and csu_m.fiscalyear = t.fiscalyear and csu_m.fiscalmonth = t.fiscalmonth
        LEFT JOIN BY_QUARTER csu_q ON ts.mastercustomerid = csu_q.mastercustomerid and csu_q.fiscalyear = t.fiscalyear and csu_q.fiscalquarter = t.fiscalquarter
        LEFT JOIN BY_YEAR csu_y ON ts.mastercustomerid = csu_y.mastercustomerid and csu_y.fiscalyear = t.fiscalyear
        LEFT JOIN BY_YEAR_LY csu_ly ON ts.mastercustomerid = csu_ly.mastercustomerid and csu_ly.fiscalyear = t.fiscalyear
        WHERE 
        ts.mastercustomerid <> ''-1''
        AND TO_DATE(TRANSACTIONTIMESTAMP) >= (SELECT TO_DATE(DATEINUNIX) FROM EDW_MARKETING_PRD.ANALYTICS.TIMESUMMARY WHERE FISCALYEAR = (SELECT FISCALYEAR FROM CDP_JC.PUBLIC.TIMESUMMARY WHERE TO_DATE(DATEINUNIX) > current_date-7 AND TO_DATE(DATEINUNIX) <= current_date AND FISCALDAYOFWEEK = 7)-2 AND FISCALDAYOFMONTH = 1 AND FISCALMONTH = 1)
        AND TO_DATE(TRANSACTIONTIMESTAMP) <= (
                        SELECT MAX(TS.DATEINUNIX::DATE) FROM CDP.PUBLIC.TIMESUMMARY TS
                        WHERE TS.DATEINUNIX::DATE <= current_date::DATE
                        AND TS.FISCALDAYOFWEEK = 7
                    );
    
        DROP TABLE IF EXISTS RPT."PASSPORT LOYALTY TIER";
        ALTER TABLE Temp."PASSPORT LOYALTY TIER" RENAME TO RPT."PASSPORT LOYALTY TIER";
          
        RETURN ''Table RPT."PASSPORT LOYALTY TIER" created or replaced successfully.'';
END;
';