SELECT 
t.useridentity_arn 
,t.eventtime 
,t.eventsource 
,t.eventname 
,b.request_time 
,b.modelid 
,regexp_extract(b.input_messages, '^(.*)({"input":{.*"type":"tool_use"})(.*)$', 2) as input_message_with_tool_use
,b.input_messages
,b.input_inputtokencount
,b.output_outputbodyjson_content
,b.output_outputtokencount
FROM loglakeblog.from_cloudtrail_readready t
left outer join loglakeblog.from_bedrock_readready b 
on t.requestid = b.requestid 
where t.logcalendarday>20240601