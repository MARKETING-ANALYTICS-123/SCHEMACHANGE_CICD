CREATE OR REPLACE PROCEDURE "USP_POPULATE_RETENTION_MADEWELL_DYNAMIC_DATE"()
RETURNS VARCHAR(100)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    currentYear INT;
    currentMonth INT;
    start_dates ARRAY;
    end_dates ARRAY;
    result STRING;
    month_cursor CURSOR FOR
        SELECT DISTINCT
            FISCALYEAR,
            FISCALMONTH
        FROM
            CDP.PUBLIC.TIMESUMMARY
        WHERE
            ID >= (
                SELECT MIN(ID)
                FROM
                CDP.PUBLIC.TIMESUMMARY 
                WHERE FISCALMONTH = (SELECT FISCALMONTH FROM CDP.PUBLIC.TIMESUMMARY WHERE DATE(DATEINUNIX) = DATE(CURRENT_DATE))
                AND FISCALYEAR = (SELECT FISCALYEAR - 1 FROM CDP.PUBLIC.TIMESUMMARY WHERE DATE(DATEINUNIX) = DATE(CURRENT_DATE))
            )
            AND ID <= (
                SELECT MAX(ID)
                FROM
                CDP.PUBLIC.TIMESUMMARY 
                WHERE FISCALMONTH = (SELECT CASE WHEN FISCALMONTH - 1 = 0 THEN 12 ELSE  FISCALMONTH - 1 END FROM CDP.PUBLIC.TIMESUMMARY WHERE DATE(DATEINUNIX) = DATE(CURRENT_DATE))
                AND FISCALYEAR = (SELECT CASE WHEN FISCALMONTH - 1 = 0 THEN FISCALYEAR - 1 ELSE FISCALYEAR END FROM CDP.PUBLIC.TIMESUMMARY WHERE DATE(DATEINUNIX) = DATE(CURRENT_DATE))
            )
        ORDER BY FISCALYEAR, FISCALMONTH;

    -- Define a cursor for static query
    -- dates_cursor CURSOR;

    
BEGIN
    -- Initialize arrays to store the results

    -- Loop through the month_cursor
    
    TRUNCATE TABLE RPT."MADEWELL RETENTION L12";
    
    FOR month_cur IN month_cursor
    DO
        currentYear := month_cur.FISCALYEAR;
        currentMonth := month_cur.FISCALMONTH;

        -- Construct the dynamic query for dates_cursor
        LET dynamic_query STRING;

        IF(currentMonth <> 12)
            THEN
                dynamic_query := ''
                    CREATE OR REPLACE TABLE Temp.temp_dates AS
                    SELECT
                    *
                    FROM
                    (
                        SELECT
                            FISCALYEAR,
                            LAG(MIN(CASE WHEN FISCALMONTH = '' || (currentMonth + 1) || '' THEN DATEINUNIX END)::DATE, 1)
                                OVER (ORDER BY FISCALYEAR) AS START_DATE,
                            MAX(CASE WHEN FISCALMONTH = '' || currentMonth || '' THEN DATEINUNIX END)::DATE AS END_DATE
                        FROM
                            CDP.PUBLIC.TIMESUMMARY
                        WHERE
                            FISCALMONTH IN ('' || currentMonth || '', '' || (currentMonth + 1) || '')
                            AND FISCALYEAR BETWEEN '' || (currentYear - 8) || '' AND '' || currentYear || ''
                        GROUP BY FISCALYEAR
                    ) T
                    WHERE START_DATE IS NOT NULL
                    ORDER BY FISCALYEAR DESC
                    '';
                ELSE
                    dynamic_query := ''
                    CREATE OR REPLACE TABLE Temp.temp_dates AS
                    SELECT
                    *
                    FROM
                    (
                        SELECT
                            FISCALYEAR,
                            MIN(DATEINUNIX)::DATE AS START_DATE,
                            MAX(DATEINUNIX)::DATE AS END_DATE
                        FROM
                            CDP.PUBLIC.TIMESUMMARY
                        WHERE
                            FISCALMONTH IN ('' || (currentMonth - 11) || '', '' || currentMonth || '')
                            AND FISCALYEAR BETWEEN '' || (currentYear - 8) || '' AND '' || currentYear || ''
                        GROUP BY FISCALYEAR
                    ) T
                    WHERE START_DATE IS NOT NULL
                    ORDER BY FISCALYEAR DESC
                    '';
        END IF;

        -- Execute the dynamic query (this will create the temporary table)
        EXECUTE IMMEDIATE :dynamic_query;

        -- Define a cursor to loop over the temporary table data
        LET dates_cursor CURSOR FOR
            SELECT START_DATE, END_DATE
            FROM Temp.temp_dates
            ORDER BY FISCALYEAR DESC;

        start_dates := ARRAY_CONSTRUCT();
        end_dates := ARRAY_CONSTRUCT();

        -- Loop through the dates_cursor to fetch results
        FOR rec IN dates_cursor
        DO
            -- Append to the arrays
            start_dates := ARRAY_APPEND(start_dates, rec.START_DATE);
            end_dates := ARRAY_APPEND(end_dates, rec.END_DATE);
        END FOR;


        CALL XFRM.USP_POPULATE_RETENTION_12M_MADEWELL(:start_dates, :end_dates, :currentYear, :currentMonth);

        -- Clean up the temporary table after using it
        DROP TABLE IF EXISTS Temp.temp_dates;
        
    END FOR;

    RETURN result;
END;
';