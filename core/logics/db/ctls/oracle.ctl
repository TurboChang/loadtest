LOAD DATA
CHARACTERSET UTF8
INFILE '/Users/changliuxin/Programs/datapipeline/loadTest/assets/testData/TestData.csv'
truncate into table CUSTOMER
FIELDS TERMINATED BY ","
TRAILING NULLCOLS
(  CUSTOMER_ID
 , FIRST_NAME
 , LAST_NAME
 , PHONE
 , EMAIL
 , STATUS
 , BIRDSDAY
 , ADDR
)