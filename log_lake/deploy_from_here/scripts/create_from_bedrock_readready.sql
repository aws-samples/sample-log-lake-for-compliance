-- This file is superseded by the CloudFormation template: stack_for_loglake_bedrock_stack. The SQL file is here for reference

CREATE EXTERNAL TABLE loglakeseanaccount.from_bedrock_readready(
schematype string,
schemaversion string,
logaccountid bigint,
request_time timestamp,
requestid string,
operation string,
modelid string, 
input_messages string,
input_tools string, 
input_inputtokencount int, 
input_orig string, 
output_outputbodyjson_id string,
output_outputbodyjson_type string,
output_outputbodyjson_role string,
output_outputbodyjson_model string,
output_outputbodyjson_content string,
output_outputbodyjson_stopreason string,
output_outputbodyjson_stopsequence string,
output_outputtokencount int,
output_orig string, 
awsregion string
)
PARTITIONED BY ( 
  `logcalendarday` bigint, 
  `loghour` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  's3://as-loglakeseanbedrock-store-bedrock-readready/from_bedrock_readready/orc/'
TBLPROPERTIES (
  'classification'='orc', 
  'notes'='demo', 
  'orc.compress'='ZLIB', 
  'partition_filtering.enabled'='true')