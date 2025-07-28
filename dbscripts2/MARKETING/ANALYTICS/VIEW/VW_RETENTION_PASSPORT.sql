create or replace view VW_RETENTION_PASSPORT(
	"MasterCustomerId",
	"Brand",
	"Transaction Id",
	"Units",
	"Sale Revenue",
	MARGIN,
	"Gross Margin",
	PERIOD,
	"Loyalty Tier",
	CHANNEL,
	FP,
	"Lapsed Status",
	"Status Type",
	"Last Status Type",
	"Year-Month"
) as
SELECT
COUNT(DISTINCT "MasterCustomerId"),
"Brand",
SUM("Transaction Id"),
SUM("Units"),
SUM("Sale Revenue"),
SUM(MARGIN),
SUM("Gross Margin"),
PERIOD,
"Loyalty Tier",
CHANNEL,
FP,
"Lapsed Status",
"Status Type",
"Last Status Type",
"Year-Month"
FROM RPT."PASSPORT RETENTION L12"
GROUP BY ALL;