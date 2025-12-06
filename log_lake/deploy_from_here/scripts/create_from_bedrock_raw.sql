-- This file is superseded by the CloudFormation template: stack_for_loglake_bedrock_stack. The SQL file is here for reference

CREATE external  TABLE replace_me_with_your_database.from_bedrock_raw (
  schemaType string,
  schemaVersion string,
  timestamp string,
  accountId string,
  identity struct<arn:string>,
  region string,
  requestId string,
  operation string,
  modelId string,
  input struct<inputContentType:string,inputBodyJson:struct<anthropic_version:string,max_tokens:int,messages:array<struct<role:string,content:string>>,tools:array<struct<name:string,description:string,input_schema:struct<type:string,properties:map<string,struct<type:string,description:string>>,required:array<string>>>>>,inputTokenCount:int>,
  output struct<outputContentType:string,outputBodyJson:struct<id:string,type:string,role:string,model:string,content:array<struct<type:string,text:string,id:string,name:string,input:map<string,string>>>,stop_reason:string,stop_sequence:string,usage:struct<input_tokens:int,cache_creation_input_tokens:int,cache_read_input_tokens:int,output_tokens:int>>,outputTokenCount:int>,
  inferenceRegion string
)
PARTITIONED BY (
  logregion string,
  logyear int,
  logmon int,
  logday int,
  loghour int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://as-loglakeseanbedrock-store-bedrock-raw/AWSLogs/111111111111/BedrockModelInvocationLogs/'
TBLPROPERTIES (
  'classification' = 'json',
  'partition_filtering.enabled' = 'true',
  'has_encrypted_data' = 'false'
);
