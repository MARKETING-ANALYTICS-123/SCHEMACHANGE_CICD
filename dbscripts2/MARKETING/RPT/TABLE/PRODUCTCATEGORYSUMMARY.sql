create or replace TABLE "PRODUCT CATEGORY SUMMARY" (
	"Id" NUMBER(38,0),
	"Product Category Level 1" VARCHAR(16777216),
	"Product Category Level 2" VARCHAR(16777216),
	"Product Category Level 3" VARCHAR(16777216),
	"Product Category Level 4" VARCHAR(16777216),
	"Product Category Level 5" VARCHAR(16777216),
	"Last Modified Date" TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);