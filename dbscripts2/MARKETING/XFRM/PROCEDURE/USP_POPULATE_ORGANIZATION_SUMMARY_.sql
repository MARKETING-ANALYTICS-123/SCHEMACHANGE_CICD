CREATE OR REPLACE PROCEDURE "USP_POPULATE_ORGANIZATION_SUMMARY"()
RETURNS VARCHAR(100)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    maxId INT;
BEGIN
    -- Get the maximum current value in the Id column
    SELECT MAX("Id"::INT) INTO maxId
    FROM RPT."ORGANIZATION SUMMARY";

    -- Create the temporary table
    CREATE OR REPLACE TABLE Temp."ORGANIZATION SUMMARY" (
        "Id" NUMBER(4, 0),
        ORGANIZATIONID VARCHAR(10),
        "Brand Name" VARCHAR(16777216),
        "Store Name" VARCHAR(16777216),
        "Store Id Name" VARCHAR(16777216),
        "Country" VARCHAR(16777216),
        "State" VARCHAR(16777216),
        "City" VARCHAR(16777216),
        "Store Open Date" DATE,
        "Store Open Year" NUMBER(38,0),
        STATUS VARCHAR(16777216),
        "Store Type" VARCHAR(16777216),
        "Type" VARCHAR(16777216),
        "Brand Sort" NUMBER(38,0),
        "Brand M&P Id" NUMBER(38,0),
        "Region" VARCHAR(16777216),
        "Sub Region" VARCHAR(16777216),
        "District" VARCHAR(16777216),
        "Store Opened Date" DATE,
        "Store Comp Indicator" VARCHAR(16777216),
        "Store Closed Date" DATE,
        "Store Concept" VARCHAR(16777216),
        "Last Modified Date" TIMESTAMP_NTZ(9)
    );


    
    INSERT INTO Temp."ORGANIZATION SUMMARY" (
        "Id",
        ORGANIZATIONID,
        "Brand Name",
        "Store Name",
        "Store Id Name",
        "Country",
        "State",
        "City",
        "Store Open Date",
        "Store Open Year",
        STATUS,
        "Store Type",
        "Type",
        "Brand Sort",
        "Brand M&P Id",
        "Region",
        "Sub Region",
        "District",
        "Store Opened Date",
        "Store Comp Indicator",
        "Store Closed Date",
        "Store Concept"
    )

    -- Insert new data into the temporary table
    WITH CTE AS (
        SELECT DISTINCT 
            "Store Number",
            "Brand",
            "Region",
            "Sub Region",
            "District",
            "Store Opened Date",
            "Store Comp Indicator",
            "Store Closed Date",
            CASE 
                WHEN "Store Concept" = ''VALUE CENTER'' THEN ''Value Center''
                WHEN "Store Concept" = ''OUTLET'' THEN ''Outlet''
                ELSE ''''
            END AS "Store Concept"
        FROM EDWPRD.RPT."Location"
        WHERE "Store Number" NOT IN (''LBDI'', ''LBDF'', ''LBDM'')
          AND "Store Number" IS NOT NULL
    ),
    ORG_SUMMARY AS (
        SELECT
            O.ID AS ORG_ID,
            OS."Id" AS "Id",
            CASE 
                WHEN O.LEVEL1NAME = ''JCrew'' THEN ''J.Crew''
                ELSE O.LEVEL1NAME
            END AS "Brand Name", 
            O.LEVEL3NAME AS "Store Name",
            CONCAT(O.ID, '' - '', O.LEVEL3NAME) AS "Store Id Name",
            O.COUNTRY AS "Country",
            O.STATE AS "State",
            O.CITY AS "City",
            O.C_STOREOPENDATE::DATE AS "Store Open Date",
            YEAR(O.C_STOREOPENDATE) AS "Store Open Year",
            O.STATUS,
            CASE 
                WHEN O.TYPE = ''Physical'' THEN ''Retail''
                WHEN O.TYPE = ''Digital'' THEN ''Direct''
                ELSE O.TYPE
            END AS "Store Type", 
            O.TYPE AS "Type"
        FROM CDP_JC.PUBLIC.ORGANIZATIONSUMMARY O
        LEFT JOIN RPT."ORGANIZATION SUMMARY" OS ON OS.ORGANIZATIONID = O.ID

        UNION ALL

        SELECT 
            O.ID AS ORG_ID,
            OS."Id" AS "Id", 
            O.LEVEL1NAME AS "Brand Name", 
            O.LEVEL3NAME AS "Store Name",
            CONCAT(O.ID, '' - '', O.LEVEL3NAME) AS "Store Id Name",
            O.COUNTRY AS "Country",
            O.STATE AS "State",
            O.CITY AS "City",
            O.C_STOREOPENDATE::DATE AS "Store Open Date",
            YEAR(O.C_STOREOPENDATE) AS "Store Open Year",
            O.STATUS,
            CASE 
                WHEN O.TYPE = ''Physical'' THEN ''Retail''
                WHEN O.TYPE = ''Digital'' THEN ''Direct''
                ELSE O.TYPE
            END AS "Store Type", 
            O.TYPE AS "Type"
        FROM CDP.PUBLIC.ORGANIZATIONSUMMARY O
        LEFT JOIN RPT."ORGANIZATION SUMMARY" OS ON OS.ORGANIZATIONID = O.ID
    )
    
    SELECT
        T."Id",
        T.ORG_ID AS ORGANIZATIONID,
        T."Brand Name",
        T."Store Name",
        T."Store Id Name",
        T."Country",
        T."State",
        T."City",
        T."Store Open Date",
        T."Store Open Year",
        T.STATUS,
        T."Store Type",
        T."Type",
        CASE 
            WHEN T."Brand Name" = ''J.Crew'' THEN 1
            WHEN T."Brand Name" = ''Factory'' THEN 2
            WHEN T."Brand Name" = ''Madewell'' THEN 3
        END AS "Brand Sort",
        CASE 
            WHEN T."Brand Name" = ''J.Crew'' THEN 2
            WHEN T."Brand Name" = ''Factory'' THEN 2
            WHEN T."Brand Name" = ''Madewell'' THEN 1
        END AS "Brand M&P Id",
        L."Region",
        L."Sub Region",
        L."District",
        L."Store Opened Date",
        CASE 
            WHEN L."Store Comp Indicator" IS NULL AND T.STATUS = ''CLOSED'' THEN ''X''
            ELSE L."Store Comp Indicator" 
        END AS "Store Comp Indicator",
        L."Store Closed Date",
        L."Store Concept"
    FROM ORG_SUMMARY T
    LEFT JOIN CTE L 
        ON T.ORG_ID = L."Store Number" 
        AND T."Brand Name" = L."Brand";

    -- Clear the target table if it exists
    DROP TABLE IF EXISTS RPT."ORGANIZATION SUMMARY";

    -- Rename the temporary table
    ALTER TABLE Temp."ORGANIZATION SUMMARY" RENAME TO RPT."ORGANIZATION SUMMARY";

    -- Update the Ids
    CREATE OR REPLACE TEMPORARY TABLE RankedData AS
    SELECT 
        ORGANIZATIONID,
        ROW_NUMBER() OVER (ORDER BY ORGANIZATIONID) + :maxId AS NewId
    FROM RPT."ORGANIZATION SUMMARY"
    WHERE "Id" IS NULL;

    
    UPDATE RPT."ORGANIZATION SUMMARY" AS T
    SET "Id" = RD.NewId
    FROM RankedData RD
    WHERE T."Id" IS NULL AND T.ORGANIZATIONID = RD.ORGANIZATIONID;

    -- Return success message
    RETURN ''Table RPT."ORGANIZATION SUMMARY" created or replaced successfully.'';
END;
';