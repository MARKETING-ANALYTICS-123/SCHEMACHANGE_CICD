create or replace TABLE "MADEWELL RETENTION L12" (
	"MasterCustomerId" NUMBER(20,0),
	"Brand" NUMBER(1,0),
	"Transaction Id" NUMBER(18,0),
	"Units" NUMBER(38,0),
	"Sale Revenue" FLOAT,
	MARGIN FLOAT,
	"Gross Margin" FLOAT,
	PERIOD VARCHAR(16777216),
	"Loyalty Tier" NUMBER(1,0),
	CHANNEL NUMBER(1,0),
	FP NUMBER(1,0),
	"Lapsed Status" NUMBER(1,0),
	"Status Type" NUMBER(1,0),
	"Last Status Type" NUMBER(1,0),
	"Year-Month" NUMBER(38,0)
);