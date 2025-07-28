create or replace TABLE "ACQUISITION LOYALTY" (
	"Date Key" NUMBER(38,0),
	"Brand ID" VARCHAR(16777216),
	"MasterCustomer ID" VARCHAR(16777216),
	"Customer Loyalty Flag" VARCHAR(16777216),
	"MasterCustomer Transaction Sequence" NUMBER(38,0),
	"Status Type" VARCHAR(16777216),
	"FM Status Type" VARCHAR(16777216),
	"FQ Status Type" VARCHAR(16777216),
	"FY Status Type" VARCHAR(16777216),
	"Loyalty Signup Date ID" NUMBER(38,0),
	"Loyalty Signup Date" DATE,
	"Loyalty SignUp Channel" VARCHAR(16777216),
	"Loyalty First Transaction Date" TIMESTAMP_NTZ(9),
	"Transaction Timestamp" DATE,
	"Last Modified Date" TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);