## Turn on glue partition indexes for existing tables 

1. Add partition indexes to all 4 tables via AWS console for Glue AWS console commands:

``` sh
aws glue create-partition-index --database-name loglakedemo --table-name from_cloudtrail_raw --partition-index Keys=logaccountid,logyear,logmon,logday,IndexName=cloudtrailRawIndex
aws glue create-partition-index --database-name loglakedemo --table-name from_cloudwatch_raw --partition-index Keys=logaccountid,logyear,logmon,logday,loghour,IndexName=cloudwatchRawIndex
aws glue create-partition-index --database-name loglakedemo --table-name from_cloudtrail_readready --partition-index Keys=logcalendarday,loghour,IndexName=cloudtrailReadReadyIndex
aws glue create-partition-index --database-name loglakedemo --table-name from_cloudwatch_readready --partition-index Keys=logcalendarday,loghour,IndexName=cloudwatchReadReadyIndex
```

How to check index creation successful:
`aws glue get-partition-indexes --database-name loglakedemo --table-name from_cloudtrail_raw`

2. Enable partition filtering for each of the 4 tables (not required if metadata stack sets this first)

``` sql
ALTER TABLE loglakedemo.from_cloudtrail_raw
SET TBLPROPERTIES ('partition_filtering.enabled' = 'true')

ALTER TABLE loglakedemo.from_cloudwatch_raw
SET TBLPROPERTIES ('partition_filtering.enabled' = 'true')

ALTER TABLE loglakedemo.from_cloudtrail_readready
SET TBLPROPERTIES ('partition_filtering.enabled' = 'true')

ALTER TABLE loglakedemo.from_cloudwatch_readready
SET TBLPROPERTIES ('partition_filtering.enabled' = 'true')
```