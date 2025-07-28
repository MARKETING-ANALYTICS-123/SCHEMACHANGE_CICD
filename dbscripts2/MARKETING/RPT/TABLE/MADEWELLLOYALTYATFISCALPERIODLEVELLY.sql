create or replace TABLE "MADEWELL LOYALTY AT FISCAL PERIOD LEVEL LY" (
	"Date Key" NUMBER(38,0),
	"Brand Name" VARCHAR(8),
	"Master Customer Id" VARCHAR(500),
	"Master Customer Transaction Sequence" NUMBER(38,0),
	"Channel Flag" VARCHAR(20),
	"Loyalty Tier" VARCHAR(100),
	"Loyalty Flag" VARCHAR(100),
	"Net Revenue" FLOAT,
	"Net Units" NUMBER(38,0),
	"Net Cost Basis" FLOAT,
	"DMD Revenue" FLOAT,
	"DMD Units" NUMBER(38,0),
	"DMD Cost Basis" FLOAT,
	"DMD Transactions" NUMBER(38,0),
	"Fiscal Status" NUMBER(38,0),
	"Last Modified Date" TIMESTAMP_NTZ(9)
);