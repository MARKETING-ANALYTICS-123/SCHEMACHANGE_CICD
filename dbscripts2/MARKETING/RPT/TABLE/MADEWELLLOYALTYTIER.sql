create or replace TABLE "MADEWELL LOYALTY TIER" (
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