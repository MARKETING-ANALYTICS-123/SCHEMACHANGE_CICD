create or replace TABLE "PRODUCT SUMMARY" (
	"Id" NUMBER(38,0),
	"Parent Product Id" VARCHAR(16777216),
	"Product Style" VARCHAR(16777216),
	"Image Url" VARCHAR(16777216),
	"Last Modified Date" TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);