create or replace TABLE "ACQUIA REFRESH COUNT" (
	"Brand" VARCHAR(16777216),
	"Transaction Date" DATE,
	"Transaction Count" NUMBER(38,0),
	"Customer Count" NUMBER(38,0),
	"Last Modified Date" TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);